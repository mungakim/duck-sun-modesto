"""An unusable API answer is described in the log, not just "Expecting value".

Nov 6 and Nov 9 2026: MID's SSL-inspecting firewall answered api.weather.gov
and api.open-meteo.com with HTTP 200 bodies that weren't JSON, or corrupted
the TLS stream (BAD_RECORD_MAC). The NOAA, OPEN-METEO and HRRR rows went
blank, and the log said only "Expecting value: line 1 column 1 (char 0)".
These tests replay both failures and assert the log now carries the status,
content type, size and body, with the full body saved under logs/.
"""

import asyncio
import logging
import ssl

import httpx
import pytest

from duck_sun import http_diagnostics
from duck_sun.http_diagnostics import (
    bad_response_path,
    is_tls_corruption,
    json_or_explain,
)
from duck_sun.providers import noaa, open_meteo
from duck_sun.resilience import RetryConfig, with_retry

BLOCK_PAGE = b"<html><head><title>Request intercepted</title></head><body>MID-FW</body></html>"
BAD_MAC = "[SSL: DECRYPTION_FAILED_OR_BAD_RECORD_MAC] decryption failed or bad record mac (_ssl.c:2700)"


@pytest.fixture(autouse=True)
def _logs_in_tmp(tmp_path, monkeypatch):
    monkeypatch.setattr(http_diagnostics, "LOG_DIR", tmp_path)
    return tmp_path


def _response(body: bytes, content_type: str = "text/html") -> httpx.Response:
    return httpx.Response(
        200,
        content=body,
        headers={"content-type": content_type},
        request=httpx.Request("GET", "https://api.weather.gov/gridpoints/STO/45,63/forecast"),
    )


def _serve(monkeypatch, body: bytes, content_type: str = "text/html"):
    """Route every httpx.AsyncClient to a fixed 200 response."""
    def handler(request):
        return httpx.Response(200, content=body, headers={"content-type": content_type})

    class _Client(httpx.AsyncClient):
        def __init__(self, *args, **kwargs):
            kwargs.pop("verify", None)
            super().__init__(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", _Client)


# --- the helper ---------------------------------------------------------------

def test_non_json_body_is_logged_saved_and_still_raises(caplog):
    caplog.set_level(logging.ERROR)
    with pytest.raises(ValueError):
        json_or_explain(_response(BLOCK_PAGE), "noaa_forecast")

    log = caplog.text
    assert "HTTP 200" in log
    assert "type=text/html" in log
    assert f"{len(BLOCK_PAGE)}B" in log
    assert "Request intercepted" in log
    assert "MID firewall" in log

    saved = bad_response_path("noaa_forecast").read_text(encoding="utf-8")
    assert "url: https://api.weather.gov/gridpoints/STO/45,63/forecast" in saved
    assert "Request intercepted" in saved


def test_empty_body_says_so(caplog):
    caplog.set_level(logging.ERROR)
    with pytest.raises(ValueError):
        json_or_explain(_response(b"", "application/geo+json"), "noaa_gridpoint")
    assert "(empty body)" in caplog.text
    assert "0B" in caplog.text


def test_valid_json_passes_through_untouched(tmp_path):
    data = json_or_explain(_response(b'{"properties": {"periods": []}}', "application/geo+json"), "noaa_forecast")
    assert data == {"properties": {"periods": []}}
    assert list(tmp_path.iterdir()) == []


def test_bad_record_mac_is_recognised_however_httpx_wraps_it():
    assert is_tls_corruption(httpx.ConnectError(BAD_MAC))

    wrapped = httpx.ReadError("read failed")
    wrapped.__cause__ = ssl.SSLError(1, BAD_MAC)
    assert is_tls_corruption(wrapped)


def test_certificate_errors_are_not_called_corruption():
    assert not is_tls_corruption(
        httpx.ConnectError("[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed")
    )
    assert not is_tls_corruption(httpx.ReadTimeout("timed out"))


# --- the providers that went blank -------------------------------------------

def test_noaa_periods_and_hourly_explain_a_non_json_200(monkeypatch, caplog):
    caplog.set_level(logging.ERROR)
    _serve(monkeypatch, BLOCK_PAGE)
    provider = noaa.NOAAProvider()

    assert asyncio.run(provider.fetch_forecast_periods()) is None
    assert asyncio.run(provider.fetch_async()) is None

    assert "Request intercepted" in bad_response_path("noaa_forecast").read_text(encoding="utf-8")
    assert "Request intercepted" in bad_response_path("noaa_gridpoint").read_text(encoding="utf-8")
    assert caplog.text.count("Unusable response") == 2


def test_open_meteo_and_hrrr_explain_a_non_json_200(monkeypatch):
    _serve(monkeypatch, b"", "application/json")

    with pytest.raises(ValueError):
        asyncio.run(open_meteo.fetch_open_meteo(days=8))
    assert asyncio.run(open_meteo.fetch_hrrr_forecast(force_refresh=True)) is None

    assert bad_response_path("open_meteo").exists()
    assert bad_response_path("hrrr").exists()


def test_retry_layer_labels_tls_corruption(caplog):
    caplog.set_level(logging.ERROR)

    @with_retry(config=RetryConfig(max_retries=0), provider_name="open_meteo")
    async def _fetch():
        raise httpx.ConnectError(BAD_MAC)

    asyncio.run(_fetch())
    assert "[open_meteo] TLS connection corrupted in transit" in caplog.text
