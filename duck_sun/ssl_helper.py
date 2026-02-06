"""
SSL Helper for Certificate Store Integration

Provides CA bundle resolution for curl_cffi, which uses libcurl's SSL
stack and doesn't automatically use the OS certificate store.

With pip-system-certs installed, certifi.where() returns the OS cert
store path, which both httpx and curl_cffi can use for proper SSL
verification.
"""

import logging
import os
import sys

try:
    import certifi
    HAS_CERTIFI = True
except ImportError:
    HAS_CERTIFI = False
    certifi = None

logger = logging.getLogger(__name__)


def get_ca_bundle_for_curl() -> str | bool:
    """
    Get the appropriate CA bundle for curl_cffi.

    With pip-system-certs installed, certifi.where() returns the OS
    certificate store, so curl_cffi will trust the same certs as the OS.

    Priority:
    1. DUCK_SUN_CA_BUNDLE environment variable (explicit override)
    2. certifi CA bundle (pip-system-certs patches this to use OS certs)
    3. True (use curl's default CA store)

    Returns:
        Path to CA bundle file, or True for curl's default
    """
    # Check for explicit override
    env_bundle = os.getenv("DUCK_SUN_CA_BUNDLE")
    if env_bundle and env_bundle.lower() not in ('true', '1', 'yes'):
        if os.path.exists(env_bundle):
            logger.info(f"[ssl_helper] Using CA bundle from env: {env_bundle}")
            return env_bundle
        else:
            logger.warning(f"[ssl_helper] DUCK_SUN_CA_BUNDLE path not found: {env_bundle}")

    # Use certifi CA bundle (pip-system-certs patches this to use OS certs)
    if HAS_CERTIFI:
        try:
            certifi_bundle = certifi.where()
            if certifi_bundle and os.path.exists(certifi_bundle):
                logger.info(f"[ssl_helper] Using certifi CA bundle: {certifi_bundle}")
                return certifi_bundle
        except Exception as e:
            logger.warning(f"[ssl_helper] certifi.where() failed: {e}")

    # Use curl's default CA store (SSL verification stays ON)
    logger.warning("[ssl_helper] certifi not available - using curl default CA store")
    return True
