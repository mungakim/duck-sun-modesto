"""
SSL Helper for Certificate Store Integration

Two functions for two SSL stacks:

1. get_ca_bundle_for_curl() → PEM file path for curl_cffi (libcurl)
2. get_httpx_ssl_context() → ssl.SSLContext for httpx (Python ssl module)

curl_cffi uses libcurl which needs a PEM file on disk.
httpx can accept an ssl.SSLContext directly — this avoids the "Missing
Authority Key Identifier" error that occurs when Python's ssl module
loads certain Windows certs from a PEM file. Loading certs natively
into an SSLContext handles chain-building without needing the AKI
extension on every certificate.
"""

import base64
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

# Cache the exported PEM path for process lifetime
_cached_windows_pem: str | None = None
# Cache the SSLContext for process lifetime
_cached_ssl_context: ssl.SSLContext | None = None


def _export_windows_cert_store() -> str | None:
    """
    Export Windows certificate store to a PEM file.

    Uses ssl.enum_certificates() to read directly from the Windows
    ROOT and CA stores, avoiding any monkey-patching by pip-system-certs.
    This captures all certs Windows trusts, including firewall inspection CAs.

    The file is cached in a temp directory for the process lifetime.

    Returns:
        Path to PEM file, or None if not on Windows or export fails.
    """
    global _cached_windows_pem

    if _cached_windows_pem and os.path.exists(_cached_windows_pem):
        return _cached_windows_pem

    if sys.platform != 'win32':
        return None

    try:
        der_certs = []

        # Read directly from Windows cert stores (ROOT = trusted root CAs, CA = intermediate CAs)
        for store_name in ('ROOT', 'CA'):
            try:
                for cert_data, encoding, trust in ssl.enum_certificates(store_name):
                    if encoding == 'x509_asn':
                        der_certs.append(cert_data)
            except AttributeError:
                # ssl.enum_certificates not available (non-Windows or old Python)
                break
            except Exception as e:
                logger.debug(f"[ssl_helper] Error reading {store_name} store: {e}")

        if not der_certs:
            logger.warning("[ssl_helper] No certificates found in Windows stores")
            return None

        # Write to a persistent temp file
        cert_dir = Path(tempfile.gettempdir()) / "duck_sun_certs"
        cert_dir.mkdir(exist_ok=True)
        cert_file = cert_dir / "windows_ca_bundle.pem"

        with open(cert_file, 'wb') as f:
            for der_cert in der_certs:
                pem = b"-----BEGIN CERTIFICATE-----\n"
                pem += base64.encodebytes(der_cert)
                pem += b"-----END CERTIFICATE-----\n"
                f.write(pem)

        logger.info(f"[ssl_helper] Exported {len(der_certs)} Windows certs to: {cert_file}")
        _cached_windows_pem = str(cert_file)
        return _cached_windows_pem

    except Exception as e:
        logger.warning(f"[ssl_helper] Windows cert export failed: {type(e).__name__}: {e}")
        return None


def get_ca_bundle_for_curl() -> str | bool:
    """
    Get the appropriate CA bundle for curl_cffi.

    Priority:
    1. DUCK_SUN_CA_BUNDLE environment variable (explicit override)
    2. Windows cert store export (includes firewall/inspection CAs)
    3. certifi CA bundle (standard Mozilla CA bundle)
    4. True (use curl's default CA store)

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

    # Export Windows cert store (includes any firewall/inspection CAs)
    windows_pem = _export_windows_cert_store()
    if windows_pem:
        return windows_pem

    # Fallback to certifi (standard Mozilla CA bundle)
    if HAS_CERTIFI:
        try:
            certifi_bundle = certifi.where()
            if certifi_bundle and os.path.exists(certifi_bundle):
                logger.info(f"[ssl_helper] Using certifi CA bundle: {certifi_bundle}")
                return certifi_bundle
        except Exception as e:
            logger.warning(f"[ssl_helper] certifi.where() failed: {e}")

    # Use curl's default CA store (SSL verification stays ON)
    logger.warning("[ssl_helper] No CA bundle available - using curl default CA store")
    return True


def get_httpx_ssl_context() -> ssl.SSLContext:
    """
    Get an ssl.SSLContext for httpx with Windows cert store loaded.

    Unlike get_ca_bundle_for_curl() which returns a PEM file path,
    this returns an SSLContext that httpx can use directly. Loading
    certs natively into an SSLContext avoids the "Missing Authority
    Key Identifier" error that Python's ssl module raises when it
    reads certain Windows certs from a PEM file — chain-building
    works correctly when certs are loaded via the SSLContext API.

    On non-Windows platforms, returns ssl.create_default_context()
    which uses the OS default cert store.

    Returns:
        ssl.SSLContext configured for HTTPS with proper CA certs.
    """
    global _cached_ssl_context

    if _cached_ssl_context is not None:
        return _cached_ssl_context

    ctx = ssl.create_default_context()

    if sys.platform == 'win32':
        loaded = 0
        skipped = 0
        for store_name in ('ROOT', 'CA'):
            try:
                for cert_data, encoding, trust in ssl.enum_certificates(store_name):
                    if encoding == 'x509_asn':
                        try:
                            pem = ssl.DER_cert_to_PEM_cert(cert_data)
                            ctx.load_verify_locations(cadata=pem)
                            loaded += 1
                        except Exception:
                            skipped += 1
            except AttributeError:
                break
            except Exception as e:
                logger.debug(f"[ssl_helper] Error reading {store_name} store: {e}")

        if loaded:
            logger.info(
                f"[ssl_helper] httpx SSLContext: loaded {loaded} Windows certs"
                + (f" (skipped {skipped} problematic)" if skipped else "")
            )

    _cached_ssl_context = ctx
    return ctx
