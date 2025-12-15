"""
Resilience Infrastructure for Duck Sun Modesto

Provides retry logic with exponential backoff for weather API providers.
Conservative strategy: 2 retries max, 1-5 second delays.

Features:
- @with_retry decorator for async functions
- Error categorization (timeout, rate_limit, api_error, parse_error)
- Jitter to prevent thundering herd
- Integration with lessons_learned tracking
"""

import asyncio
import functools
import logging
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Optional, Tuple, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar('T')


class ErrorType(Enum):
    """Categories of errors for tracking."""
    TIMEOUT = "timeout"
    RATE_LIMIT = "rate_limit"
    API_ERROR = "api_error"
    PARSE_ERROR = "parse_error"
    UNKNOWN = "unknown"


@dataclass
class RetryConfig:
    """Configuration for retry behavior - Conservative defaults."""
    max_retries: int = 2  # 2 retries = 3 total attempts
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 5.0
    exponential_base: float = 2.0
    jitter: bool = True  # Add randomness to prevent thundering herd

    # HTTP status codes that should NOT trigger retry
    non_retryable_status_codes: tuple = (400, 401, 403, 404, 422)

    # HTTP status codes that SHOULD trigger retry
    retryable_status_codes: tuple = (408, 429, 500, 502, 503, 504)


# Default conservative config
DEFAULT_RETRY_CONFIG = RetryConfig(
    max_retries=2,
    base_delay_seconds=1.0,
    max_delay_seconds=5.0,
    jitter=True
)


def categorize_error(exception: Exception) -> Tuple[ErrorType, str]:
    """
    Categorize an exception for tracking purposes.

    Returns:
        Tuple of (ErrorType, error_message)
    """
    import httpx
    import json

    error_msg = str(exception)[:200]  # Truncate long messages

    if isinstance(exception, httpx.TimeoutException):
        return (ErrorType.TIMEOUT, f"Timeout: {error_msg}")

    elif isinstance(exception, httpx.HTTPStatusError):
        status = exception.response.status_code
        if status == 429:
            return (ErrorType.RATE_LIMIT, f"HTTP 429 Too Many Requests")
        elif status == 503:
            return (ErrorType.RATE_LIMIT, f"HTTP 503 Service Unavailable (quota?)")
        else:
            return (ErrorType.API_ERROR, f"HTTP {status}: {error_msg}")

    elif isinstance(exception, httpx.RequestError):
        return (ErrorType.API_ERROR, f"Request error: {error_msg}")

    elif isinstance(exception, (json.JSONDecodeError, KeyError, ValueError, TypeError)):
        return (ErrorType.PARSE_ERROR, f"Parse error: {error_msg}")

    else:
        return (ErrorType.UNKNOWN, error_msg)


def calculate_backoff_delay(attempt: int, config: RetryConfig) -> float:
    """
    Calculate delay with exponential backoff and optional jitter.

    Args:
        attempt: The retry attempt number (0-indexed)
        config: Retry configuration

    Returns:
        Delay in seconds
    """
    delay = min(
        config.base_delay_seconds * (config.exponential_base ** attempt),
        config.max_delay_seconds
    )

    if config.jitter:
        # Add up to 25% jitter
        jitter_amount = delay * 0.25 * random.random()
        delay += jitter_amount

    return delay


def is_retryable_error(exception: Exception, config: RetryConfig) -> bool:
    """
    Determine if an exception should trigger a retry.

    Args:
        exception: The caught exception
        config: Retry configuration

    Returns:
        True if should retry, False otherwise
    """
    import httpx

    # Check HTTP status codes
    if isinstance(exception, httpx.HTTPStatusError):
        status = exception.response.status_code
        if status in config.non_retryable_status_codes:
            return False
        # Retry on known retryable codes or 5xx errors
        return status in config.retryable_status_codes or status >= 500

    # Timeouts are always retryable
    if isinstance(exception, httpx.TimeoutException):
        return True

    # Connection errors are retryable
    if isinstance(exception, httpx.RequestError):
        return True

    # Parse errors are NOT retryable (same bad data will come back)
    if isinstance(exception, (KeyError, ValueError, TypeError)):
        return False

    # Unknown errors - retry once to be safe
    return True


def with_retry(
    config: Optional[RetryConfig] = None,
    provider_name: str = "unknown"
) -> Callable:
    """
    Decorator that adds retry logic with exponential backoff.

    Works with async functions only (all providers use async).

    Usage:
        @with_retry(provider_name="NWS")
        async def fetch_async(self) -> Optional[List[dict]]:
            ...

    Args:
        config: Retry configuration (uses DEFAULT_RETRY_CONFIG if None)
        provider_name: Name for logging purposes

    Returns:
        Decorated function
    """
    if config is None:
        config = DEFAULT_RETRY_CONFIG

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs) -> Optional[Any]:
            last_exception = None
            start_time = time.time()

            for attempt in range(config.max_retries + 1):
                try:
                    # Wait before retry (not on first attempt)
                    if attempt > 0:
                        delay = calculate_backoff_delay(attempt - 1, config)
                        logger.info(
                            f"[{provider_name}] Retry {attempt}/{config.max_retries} "
                            f"after {delay:.1f}s delay"
                        )
                        await asyncio.sleep(delay)

                    # Attempt the call
                    result = await func(*args, **kwargs)

                    # Success!
                    elapsed = time.time() - start_time
                    if attempt > 0:
                        logger.info(
                            f"[{provider_name}] Succeeded on attempt {attempt + 1} "
                            f"({elapsed:.2f}s total)"
                        )

                    return result

                except Exception as e:
                    last_exception = e
                    error_type, error_msg = categorize_error(e)

                    logger.warning(
                        f"[{provider_name}] Attempt {attempt + 1} failed: "
                        f"{error_type.value} - {error_msg}"
                    )

                    # Check if we should retry
                    if not is_retryable_error(e, config):
                        logger.error(
                            f"[{provider_name}] Error not retryable, giving up"
                        )
                        break

                    # Check if we have retries left
                    if attempt >= config.max_retries:
                        break

            # All retries exhausted
            elapsed = time.time() - start_time
            error_type, error_msg = categorize_error(last_exception) if last_exception else (ErrorType.UNKNOWN, "Unknown")

            logger.error(
                f"[{provider_name}] All {config.max_retries + 1} attempts failed "
                f"({elapsed:.2f}s total). Last error: {error_type.value}"
            )

            return None

        return async_wrapper

    return decorator


# Convenience function for one-off retries without decorator
async def retry_async(
    func: Callable,
    provider_name: str = "unknown",
    config: Optional[RetryConfig] = None,
    *args,
    **kwargs
) -> Optional[Any]:
    """
    Execute an async function with retry logic.

    Args:
        func: Async function to call
        provider_name: Name for logging
        config: Retry configuration
        *args, **kwargs: Arguments to pass to func

    Returns:
        Result of func or None on failure
    """
    if config is None:
        config = DEFAULT_RETRY_CONFIG

    @with_retry(config=config, provider_name=provider_name)
    async def wrapper():
        return await func(*args, **kwargs)

    return await wrapper()
