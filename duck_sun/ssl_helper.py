"""
SSL Helper for Certificate Store Integration

Two functions for two SSL stacks:

1. get_ca_bundle_for_curl() → PEM file path for curl_cffi (libcurl)
2. get_httpx_ssl_context() → ssl.SSLContext for httpx (Python ssl module)

curl_cffi uses libcurl which needs a PEM file on disk.

httpx needs an ssl.SSLContext. On Windows behind a corporate firewall,
the firewall's intermediate cert may lack the Authority Key Identifier
(AKI) X.509 extension. OpenSSL 3.x (bundled with Python 3.14) rejects
these chains, but Windows SChannel handles them fine. The `truststore`
package delegates SSL verification to the OS native stack (SChannel on
Windows), bypassing OpenSSL's strict AKI requirement.
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

try:
    import truststore
    HAS_TRUSTSTORE = True
except ImportError:
    HAS_TRUSTSTORE = False

logger = logging.getLogger(__name__)


def _patch_truststore_for_py314_recursion() -> bool:
    """Fix truststore<=0.10.4's verify_mode recursion against Python 3.14.

    Python 3.14 redefined `ssl.SSLContext.verify_mode` as a Python property
    whose setter calls `super(SSLContext, SSLContext).verify_mode.__set__`.
    truststore stores `super(SSLContext, SSLContext)` at import time and
    later calls `.verify_mode.__set__` on it — but `super()` resolves
    attributes lazily at access time, so on 3.14 the lookup returns the
    same patched property, producing infinite recursion (RecursionError:
    Stack overflow on every HTTPS connection).

    Bypass by going straight to the C-level `_ssl._SSLContext.verify_mode`
    getset descriptor, which has no Python wrapper.
    """
    if sys.version_info < (3, 14):
        return False  # 3.13 and earlier are unaffected
    if not HAS_TRUSTSTORE:
        return False
    try:
        import _ssl
        import importlib

        c_verify_mode = _ssl._SSLContext.verify_mode

        def _safe_set_verify_mode(ssl_context, verify_mode):
            c_verify_mode.__set__(ssl_context, verify_mode)

        # Patch the canonical location first.
        import truststore._ssl_constants as _tsc
        _tsc._set_ssl_context_verify_mode = _safe_set_verify_mode

        # Platform shims bind the helper via `from ._ssl_constants import ...`
        # at import time, so the from-import alias must also be patched
        # wherever it's actually called.
        for mod_name in ('_windows', '_macos', '_openssl'):
            try:
                mod = importlib.import_module(f'truststore.{mod_name}')
            except Exception:
                continue
            if hasattr(mod, '_set_ssl_context_verify_mode'):
                mod._set_ssl_context_verify_mode = _safe_set_verify_mode

        logger.info(
            "[ssl_helper] Patched truststore 0.10.4 to bypass Python "
            f"{sys.version_info.major}.{sys.version_info.minor} "
            "SSLContext.verify_mode recursion"
        )
        return True
    except Exception as exc:
        logger.warning(f"[ssl_helper] truststore Python 3.14 patch failed: {exc}")
        return False


# Apply the patch eagerly so any subsequent truststore.SSLContext() works.
_TRUSTSTORE_PATCHED_FOR_PY314 = _patch_truststore_for_py314_recursion()

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
    Get an ssl.SSLContext for httpx with OS-native cert verification.

    Priority:
    1. truststore (delegates to Windows SChannel / macOS SecureTransport)
       - Bypasses OpenSSL 3.x strict AKI checks that reject firewall certs
       - Used by pip itself for corporate proxy/firewall environments
    2. Fallback: ssl.create_default_context() + manual Windows cert loading

    Returns:
        ssl.SSLContext configured for HTTPS with proper CA certs.
    """
    global _cached_ssl_context

    if _cached_ssl_context is not None:
        return _cached_ssl_context

    # Option 1: truststore — uses OS native SSL (SChannel on Windows).
    # This is the only reliable way to handle firewall certs that lack
    # the Authority Key Identifier extension (OpenSSL 3.x rejects them,
    # but Windows SChannel handles them via subject/issuer name matching).
    #
    # Python 3.14+: truststore 0.10.4 has a verify_mode recursion bug.
    # _patch_truststore_for_py314_recursion() patches it; the resulting
    # context works correctly. If the patch couldn't apply, fall back to
    # stdlib SSL + Windows cert loading (Option 2 below).
    truststore_usable = (
        HAS_TRUSTSTORE
        and (sys.version_info < (3, 14) or _TRUSTSTORE_PATCHED_FOR_PY314)
    )
    if truststore_usable:
        try:
            ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            logger.info("[ssl_helper] httpx SSLContext: using truststore (OS-native SSL)")
            _cached_ssl_context = ctx
            return ctx
        except Exception as e:
            logger.warning(f"[ssl_helper] truststore init failed: {e}, falling back")
    elif HAS_TRUSTSTORE:
        logger.warning(
            "[ssl_helper] truststore present but unusable on Python "
            f"{sys.version_info.major}.{sys.version_info.minor}; "
            "falling back to stdlib SSL. AKI-less firewall certs may fail."
        )

    # Option 2: Manual Windows cert loading (works if no AKI issues)
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
                f"[ssl_helper] httpx SSLContext: loaded {loaded} Windows certs (no truststore)"
                + (f" (skipped {skipped} problematic)" if skipped else "")
            )

    _cached_ssl_context = ctx
    return ctx
