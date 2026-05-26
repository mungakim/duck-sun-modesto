"""Regression tests for the Python 3.14 truststore monkey-patch in ssl_helper."""

import ssl
import sys


def test_c_level_verify_mode_setter_bypasses_python_wrapper():
    """The truststore Python 3.14 patch works by going through
    `_ssl._SSLContext.verify_mode.__set__` instead of the Python
    `SSLContext.verify_mode` property. Verify the bypass mechanism itself."""
    import _ssl

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False  # required before setting CERT_NONE

    # C-level descriptor must accept assignment without recursing
    _ssl._SSLContext.verify_mode.__set__(ctx, ssl.CERT_NONE)
    assert ctx.verify_mode == ssl.CERT_NONE

    _ssl._SSLContext.verify_mode.__set__(ctx, ssl.CERT_REQUIRED)
    assert ctx.verify_mode == ssl.CERT_REQUIRED


def test_patch_is_noop_on_python_pre_314():
    """The patch should only fire on 3.14+ (where the recursion bug exists).
    On earlier Pythons, _TRUSTSTORE_PATCHED_FOR_PY314 is False and truststore
    runs unmodified."""
    from duck_sun import ssl_helper

    if sys.version_info < (3, 14):
        assert ssl_helper._TRUSTSTORE_PATCHED_FOR_PY314 is False, (
            "Patch fired on a Python where it shouldn't have"
        )
    # On 3.14+, the patch should fire if truststore is installed (we can't
    # easily test this without being on 3.14, but the bypass mechanism is
    # exercised by test_c_level_verify_mode_setter_bypasses_python_wrapper).


def test_truststore_usable_decision_uses_patch_flag():
    """get_httpx_ssl_context's truststore eligibility check must consult
    the patch flag, not just the Python version. Otherwise we'd disable
    truststore on 3.14 even after a successful patch."""
    import inspect

    from duck_sun import ssl_helper

    src = inspect.getsource(ssl_helper.get_httpx_ssl_context)
    # The condition must reference both: version check AND patch flag
    assert "_TRUSTSTORE_PATCHED_FOR_PY314" in src, (
        "get_httpx_ssl_context must consult _TRUSTSTORE_PATCHED_FOR_PY314 "
        "to re-enable truststore after the 3.14 patch"
    )
