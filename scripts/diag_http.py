r"""
Is MID's firewall interfering with the httpx-based weather APIs?

Built for the Nov 2026 incident: api.weather.gov and api.open-meteo.com
returned HTTP 200 bodies that weren't JSON (or broke with
BAD_RECORD_MAC) to the report for hours, while PowerShell fetched the same
URLs fine. See CLAUDE.md "Network Interception".

For each host it prints who signed the certificate this PC receives
(`MID-FW-Trusted` = the firewall is decrypting the traffic) and fetches it
ROUNDS times each way:
  - httpx exactly as the report does (ssl_helper's context)
  - httpx without compression
  - httpx limited to TLS 1.2
  - curl_cffi, as Weather.com / WUnderground do
A variant that scores 5/5 while "as the report does" fails is a workaround.
Read-only: nothing is cached, written or changed.

Run from the repo root:
    .\venv\Scripts\python.exe scripts\diag_http.py
"""
import json
import socket
import ssl
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # repo root

import _ssl
import httpx

from duck_sun.ssl_helper import get_ca_bundle_for_curl, get_httpx_ssl_context

try:
    import truststore
except ImportError:
    truststore = None

ROUNDS = 5
URLS = {
    "NOAA forecast": "https://api.weather.gov/gridpoints/STO/45,63/forecast",
    "NOAA hourly": "https://api.weather.gov/gridpoints/STO/45,63",
    "Open-Meteo": "https://api.open-meteo.com/v1/forecast?latitude=37.6391&longitude=-120.9969&hourly=temperature_2m",
}
UA = {"User-Agent": "(duck-sun-modesto diagnostic)"}


def fresh_context(tls12=False):
    """A NEW context each call (never mutate ssl_helper's cached one).
    TLS version is set via the C-level descriptor: on Python 3.14 the
    Python-level setter recurses forever when truststore is injected."""
    if truststore is not None:
        ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        inner = ctx._ctx
    else:
        ctx = ssl.create_default_context()
        inner = ctx
    if tls12:
        _ssl._SSLContext.maximum_version.__set__(inner, ssl.TLSVersion.TLSv1_2)
    return ctx


def cert_issuer(host):
    try:
        ctx = fresh_context()
        with socket.create_connection((host, 443), timeout=10) as s:
            with ctx.wrap_socket(s, server_hostname=host) as t:
                issuer = dict(x[0] for x in t.getpeercert()["issuer"])
                return f"{issuer.get('organizationName')} / {issuer.get('commonName')}  [{t.version()}]"
    except Exception as e:
        return f"ERROR {type(e).__name__}: {e}"


def is_json(raw):
    try:
        json.loads(raw)
        return True
    except Exception:
        return False


def httpx_variant(url, ctx_factory, headers):
    with httpx.Client(timeout=20, verify=ctx_factory()) as c:
        r = c.get(url, headers={**UA, **headers})
        return r.status_code, dict(r.headers), r.content


def curl_variant(url):
    from curl_cffi import requests as creq
    # Same CA bundle the Weather.com provider passes (Windows cert store
    # export), so the firewall's MID-FW-Trusted certificate is trusted.
    r = creq.get(url, headers=UA, impersonate="chrome120", timeout=20,
                 verify=get_ca_bundle_for_curl())
    return r.status_code, dict(r.headers), r.content


VARIANTS = [
    ("httpx as the exe does", lambda u: httpx_variant(u, get_httpx_ssl_context, {})),
    ("httpx, no compression", lambda u: httpx_variant(u, fresh_context, {"Accept-Encoding": "identity"})),
    ("httpx, TLS 1.2 max", lambda u: httpx_variant(u, lambda: fresh_context(tls12=True), {})),
    ("curl_cffi (like Weather.com)", curl_variant),
]

print(f"Python {sys.version.split()[0]}  httpx {httpx.__version__}  {ssl.OPENSSL_VERSION}  "
      f"truststore {getattr(truststore, '__version__', 'absent')}")

for name, url in URLS.items():
    host = url.split("/")[2]
    print(f"\n== {name}  ({host})   cert issuer: {cert_issuer(host)}")
    for label, fn in VARIANTS:
        ok, bad_samples = 0, []
        for _ in range(ROUNDS):
            t0 = time.time()
            try:
                status, headers, raw = fn(url)
                ms = int((time.time() - t0) * 1000)
                if status == 200 and is_json(raw):
                    ok += 1
                else:
                    bad_samples.append(
                        f"HTTP {status} {ms}ms body={len(raw)}B "
                        f"type={headers.get('content-type', '-')} enc={headers.get('content-encoding', '-')} "
                        f"{raw[:120]!r}"
                    )
            except Exception as e:
                bad_samples.append(f"{type(e).__name__}: {str(e)[:140]}")
            time.sleep(0.5)
        print(f"  {label:<30} {ok}/{ROUNDS} OK")
        for s in bad_samples[:2]:
            print(f"      bad: {s}")
