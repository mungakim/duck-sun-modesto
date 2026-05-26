"""Regression tests for the Python 3.14 truststore monkey-patch in ssl_helper."""

import ssl
import sys


def test_c_level_verify_mode_setter_bypasses_python_wrapper():
    """The truststore Python 3.14 patch works by going through
    `_ssl._SSLContext.verify_mode.__set__` instead of the Python
    `SSLContext.verify_mode` property. Verify the bypass mechanism itself.

    Note: On Python 3.14, `ctx.check_hostname = False` has the same broken
    wrapper as verify_mode (silently no-ops via recursive super()), so we
    also use the C-level descriptor to set check_hostname here. This keeps
    the test version-agnostic: it exercises the SAME bypass path that
    `_patch_ssl_context_setters_for_py314` installs at runtime."""
    import _ssl

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    # Disable hostname check via C-level (bypassing 3.14's broken Python setter)
    _ssl._SSLContext.check_hostname.__set__(ctx, False)
    assert ctx.check_hostname is False, "C-level check_hostname bypass failed"

    # Now verify_mode=CERT_NONE is allowed
    _ssl._SSLContext.verify_mode.__set__(ctx, ssl.CERT_NONE)
    assert ctx.verify_mode == ssl.CERT_NONE

    _ssl._SSLContext.verify_mode.__set__(ctx, ssl.CERT_REQUIRED)
    assert ctx.verify_mode == ssl.CERT_REQUIRED


def test_patches_are_noops_on_python_pre_314():
    """Both patches should only fire on 3.14+ (where the property setters
    are broken). On earlier Pythons, the flags are False and truststore +
    ssl.SSLContext run unmodified."""
    from duck_sun import ssl_helper

    if sys.version_info < (3, 14):
        assert ssl_helper._TRUSTSTORE_PATCHED_FOR_PY314 is False
        assert ssl_helper._SSL_SETTERS_PATCHED_FOR_PY314 is False


def test_truststore_usable_decision_requires_both_patches_on_314():
    """get_httpx_ssl_context's truststore eligibility check must consult
    BOTH the global SSLContext setter patch AND the truststore-specific
    helper patch. Either one alone is insufficient on 3.14:
    - Without the setter patch, ctx.check_hostname = False silently fails,
      so verify_mode=CERT_NONE raises.
    - Without the truststore helper patch, _set_ssl_context_verify_mode
      recurses on the broken super() proxy."""
    import inspect

    from duck_sun import ssl_helper

    src = inspect.getsource(ssl_helper.get_httpx_ssl_context)
    assert "_TRUSTSTORE_PATCHED_FOR_PY314" in src, (
        "Eligibility check must consult truststore-specific patch flag"
    )
    assert "_SSL_SETTERS_PATCHED_FOR_PY314" in src, (
        "Eligibility check must consult global SSLContext setter patch flag"
    )


def test_global_ssl_setter_patch_uses_c_level_descriptor():
    """The Py3.14 patch replaces ssl.SSLContext.{check_hostname,verify_mode}
    properties with wrappers that go straight to _ssl._SSLContext's C-level
    descriptors. Verify the patch function references the right C-level API."""
    import inspect

    from duck_sun import ssl_helper

    src = inspect.getsource(ssl_helper._patch_ssl_context_setters_for_py314)
    assert "_ssl._SSLContext.check_hostname" in src
    assert "_ssl._SSLContext.verify_mode" in src
    assert "ssl_mod.SSLContext.check_hostname = property" in src
    assert "ssl_mod.SSLContext.verify_mode = property" in src
