"""
SSL Helper for Windows Certificate Store Integration

Extracts certificates from Windows certificate store to a PEM file
that can be used by curl_cffi and other libraries that don't natively
support the Windows certificate store.

This allows the application to work through corporate proxies that use
SSL inspection (like MID's proxy).
"""

import logging
import os
import ssl
import sys
import tempfile
from pathlib import Path

try:
    import certifi
    HAS_CERTIFI = True
except ImportError:
    HAS_CERTIFI = False
    certifi = None

logger = logging.getLogger(__name__)

# Cache the extracted cert file path
_cached_cert_file: str | None = None


def get_windows_ca_bundle() -> str | None:
    """
    Extract Windows certificate store to a temporary PEM file.

    Returns the path to the PEM file, or None if not on Windows
    or if extraction fails.

    The file is cached for the lifetime of the process.
    """
    global _cached_cert_file

    # Return cached path if already extracted
    if _cached_cert_file and os.path.exists(_cached_cert_file):
        return _cached_cert_file

    # Only works on Windows
    if sys.platform != 'win32':
        logger.debug("[ssl_helper] Not on Windows, skipping cert extraction")
        return None

    try:
        import ssl

        # Get the default SSL context which uses Windows cert store
        context = ssl.create_default_context()

        # Extract certificates from the context
        certs = context.get_ca_certs(binary_form=True)

        if not certs:
            logger.warning("[ssl_helper] No certificates found in Windows store")
            return None

        logger.info(f"[ssl_helper] Found {len(certs)} certificates in Windows store")

        # Write certificates to a temporary PEM file
        # Use a persistent temp file that survives the session
        cert_dir = Path(tempfile.gettempdir()) / "duck_sun_certs"
        cert_dir.mkdir(exist_ok=True)
        cert_file = cert_dir / "windows_ca_bundle.pem"

        with open(cert_file, 'wb') as f:
            for cert in certs:
                # Convert DER to PEM format
                import base64
                pem = b"-----BEGIN CERTIFICATE-----\n"
                pem += base64.encodebytes(cert)
                pem += b"-----END CERTIFICATE-----\n"
                f.write(pem)

        logger.info(f"[ssl_helper] Wrote CA bundle to: {cert_file}")
        _cached_cert_file = str(cert_file)
        return _cached_cert_file

    except Exception as e:
        logger.error(f"[ssl_helper] Failed to extract Windows certs: {e}")
        return None


def get_ca_bundle_for_curl() -> str | bool:
    """
    Get the appropriate CA bundle for curl_cffi.

    Priority:
    1. DUCK_SUN_CA_BUNDLE environment variable (explicit override)
    2. Windows certificate store (if on Windows)
    3. certifi CA bundle (standard Python SSL certificates)
    4. False (skip verification - last resort for corporate proxies)

    Returns:
        Path to CA bundle file, or False to skip verification
    """
    # Check for explicit override
    env_bundle = os.getenv("DUCK_SUN_CA_BUNDLE")
    if env_bundle and env_bundle.lower() not in ('true', '1', 'yes'):
        if os.path.exists(env_bundle):
            logger.info(f"[ssl_helper] Using CA bundle from env: {env_bundle}")
            return env_bundle
        else:
            logger.warning(f"[ssl_helper] DUCK_SUN_CA_BUNDLE path not found: {env_bundle}")

    # Try Windows certificate store
    windows_bundle = get_windows_ca_bundle()
    if windows_bundle:
        return windows_bundle

    # Try certifi CA bundle (works in PyInstaller exe when bundled with --collect-data certifi)
    if HAS_CERTIFI:
        try:
            certifi_bundle = certifi.where()
            if certifi_bundle and os.path.exists(certifi_bundle):
                logger.info(f"[ssl_helper] Using certifi CA bundle: {certifi_bundle}")
                return certifi_bundle
        except Exception as e:
            logger.warning(f"[ssl_helper] certifi fallback failed: {e}")

    # Final fallback: skip verification
    # This matches what httpx-based providers (NOAA, Met.no, MID.org, Open-Meteo,
    # METAR) already do with verify=False. Behind corporate proxies with SSL
    # inspection, neither certifi nor curl_cffi's bundled Mozilla CA will have
    # the proxy's CA cert. Better to skip than fail and serve stale cached data.
    logger.warning("[ssl_helper] No CA bundle available - falling back to verify=False")
    return False


# Pre-extract on module load for Windows
if sys.platform == 'win32':
    try:
        _bundle = get_windows_ca_bundle()
        if _bundle:
            logger.info(f"[ssl_helper] Pre-extracted Windows CA bundle: {_bundle}")
    except Exception as e:
        logger.debug(f"[ssl_helper] Pre-extraction failed: {e}")
