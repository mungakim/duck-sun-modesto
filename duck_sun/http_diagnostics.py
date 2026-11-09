"""
Say what actually came back when an API answer can't be used.

MID's firewall decrypts and re-encrypts HTTPS (its certificate is issued by
`MID-FW-Trusted`). On Nov 6 and Nov 9 2026 it broke api.weather.gov and
api.open-meteo.com for hours at a time, in two ways:

  * HTTP 200 whose body was not JSON, returned in ~40ms (a real NOAA
    answer is ~13KB of forecast). Logged only as
    "Expecting value: line 1 column 1 (char 0)"
  * `[SSL: DECRYPTION_FAILED_OR_BAD_RECORD_MAC]` - TLS records altered in
    transit

Neither message said what had happened, and PowerShell / a dev venv
fetched the same URLs fine an hour later. These helpers make the run log
carry the evidence instead: status, content type, size, response time and
the start of the body, with the full body saved under logs/.
"""

import logging
import re
import ssl
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# Relative to the working directory, like logs/duck_sun.log and
# logs/wunderground_last_failed_page.html.
LOG_DIR = Path("logs")
SNIPPET_CHARS = 300
MAX_SAVED_BYTES = 512 * 1024

INTERCEPTION_HINT = (
    "possible MID firewall SSL-inspection interference - "
    "see CLAUDE.md 'Network Interception'"
)

# OpenSSL errors that mean the bytes were altered between server and client,
# rather than a certificate or hostname problem.
_TLS_CORRUPTION_MARKERS = (
    "DECRYPTION_FAILED_OR_BAD_RECORD_MAC",
    "BAD_RECORD_MAC",
    "WRONG_VERSION_NUMBER",
    "UNEXPECTED_EOF_WHILE_READING",
)


def bad_response_path(source: str) -> Path:
    """logs/<source>_last_bad_response.txt (source sanitised for a filename)."""
    safe = re.sub(r"[^A-Za-z0-9_-]+", "_", source).strip("_").lower() or "unknown"
    return LOG_DIR / f"{safe}_last_bad_response.txt"


def _elapsed_ms(resp: httpx.Response) -> str:
    try:
        return f"{resp.elapsed.total_seconds() * 1000:.0f}ms"
    except Exception:  # elapsed is only set once the response is closed
        return "?ms"


def describe_bad_response(source: str, resp: httpx.Response, reason: str) -> Path | None:
    """Log what the server (or something in between) sent, and save the body."""
    body = resp.content or b""
    text = body.decode(resp.encoding or "utf-8", errors="replace")
    snippet = repr(text[:SNIPPET_CHARS]) if text else "(empty body)"

    logger.error(
        f"[{source}] Unusable response ({reason}): HTTP {resp.status_code}, "
        f"{_elapsed_ms(resp)}, {len(body)}B, "
        f"type={resp.headers.get('content-type', '-')}, "
        f"encoding={resp.headers.get('content-encoding', '-')} - {INTERCEPTION_HINT}"
    )
    logger.error(f"[{source}] Body starts: {snippet}")

    path = bad_response_path(source)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        header = "\n".join(
            [
                f"# {source} unusable response, saved {datetime.now().isoformat(timespec='seconds')}",
                f"# reason: {reason}",
                f"# url: {resp.request.url if resp.request else '?'}",
                f"# HTTP {resp.status_code}, {_elapsed_ms(resp)}, {len(body)} bytes",
                *(f"# {k}: {v}" for k, v in resp.headers.items()),
                "",
                "",
            ]
        )
        path.write_text(header + text[:MAX_SAVED_BYTES], encoding="utf-8")
        logger.error(f"[{source}] Saved the response to {path}")
        return path
    except Exception as exc:
        logger.warning(f"[{source}] Could not save the response to {path}: {exc}")
        return None


def json_or_explain(resp: httpx.Response, source: str) -> Any:
    """resp.json(), but a body that isn't JSON is logged and saved first.

    Re-raises the original ValueError so callers and the retry layer
    (categorize_error -> PARSE_ERROR) behave exactly as before.
    """
    try:
        return resp.json()
    except ValueError as exc:
        describe_bad_response(source, resp, f"not JSON: {exc}")
        raise


def is_tls_corruption(exc: BaseException) -> bool:
    """True for TLS errors that mean the connection was altered in transit.

    httpx wraps ssl.SSLError in httpx.ConnectError / ReadError, so the
    OpenSSL reason is matched in the message chain rather than by type.
    """
    seen = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, ssl.SSLError) and getattr(current, "reason", None) in _TLS_CORRUPTION_MARKERS:
            return True
        if any(marker in str(current) for marker in _TLS_CORRUPTION_MARKERS):
            return True
        current = current.__cause__ or current.__context__
    return False


def log_if_tls_corruption(source: str, exc: BaseException) -> bool:
    """Add an explicit interception line to the log for a corrupted TLS connection."""
    if not is_tls_corruption(exc):
        return False
    logger.error(f"[{source}] TLS connection corrupted in transit ({exc}) - {INTERCEPTION_HINT}")
    return True
