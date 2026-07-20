"""Deterministic repro for the BoringSSL abort.

Theory under test: connections pooled inside a curl handle die silently
(VPN idle timeout / relay change). The next request prunes them —
libcurl writes a TLS close_notify (or h2 GOAWAY) on the dead connection
and BoringSSL's ssl_write_buffer_flush aborts the process.

Method: download → `mullvad reconnect` (kills all tunnel TCP flows,
same relay) → download again on the same handle. If theory is right,
mode=plain aborts (exit 134). Candidate fixes are tested the same way.

Modes:
  plain         current app config (shared h1 session)      → expect ABORT
  close-handle  close this thread's curl handle after each
                download, while connections are healthy      → expect survive
  forbid-reuse  session created with FORBID_REUSE so no
                connection is ever pooled                    → expect survive

Usage: uv run python vpn_kill_repro.py <mode> [cycles]
"""

import faulthandler
import socket
import subprocess
import sys
import time

faulthandler.enable(all_threads=True)
socket.setdefaulttimeout(120)

import yfinance as yf
from curl_cffi import CurlHttpVersion, CurlOpt
from curl_cffi import requests as curl_requests
from curl_cffi.curl import Curl as _Curl

_Curl.__del__ = lambda self: None   # match app config

MODE = sys.argv[1] if len(sys.argv) > 1 else "plain"
CYCLES = int(sys.argv[2]) if len(sys.argv) > 2 else 2
assert MODE in ("plain", "close-handle", "forbid-reuse", "app"), MODE

if MODE == "app":
    # The app's actual session config, exactly as Streamlit uses it
    from yf_session import YF_SESSION as SESSION
else:
    session_kwargs: dict = {
        "impersonate": "chrome",
        "http_version": CurlHttpVersion.V1_1,
    }
    if MODE == "forbid-reuse":
        session_kwargs["curl_options"] = {CurlOpt.FORBID_REUSE: 1}
    SESSION = curl_requests.Session(**session_kwargs)

TICKERS = [t.strip() for t in open("tickers.txt").read().split(",") if t.strip()][:30]


def close_thread_curl():
    local = getattr(SESSION, "_local", None)
    curl = getattr(local, "curl", None) if local is not None else None
    if curl is not None:
        try:
            curl.close()
        except Exception:
            pass
        local.curl = None


def wait_online(timeout=90):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            r = subprocess.run(["curl", "-s", "-m", "5", "-o", "/dev/null",
                                "-w", "%{http_code}", "https://am.i.mullvad.net/ip"],
                               capture_output=True, text=True)
            if r.stdout.strip() == "200":
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


def download(n):
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] download {n} START", flush=True)
    df = yf.download(tickers=TICKERS, period="6mo", group_by="ticker",
                     auto_adjust=False, threads=False, progress=False,
                     session=SESSION)
    print(f"[{time.strftime('%H:%M:%S')}] download {n} DONE  "
          f"{time.time()-t0:.0f}s  shape={df.shape}", flush=True)


print(f"mode={MODE}  cycles={CYCLES}", flush=True)
for cycle in range(1, CYCLES + 1):
    download(f"{cycle}a")

    if MODE == "close-handle":
        print(f"[cycle {cycle}] closing thread curl handle (healthy)", flush=True)
        close_thread_curl()

    print(f"[cycle {cycle}] mullvad reconnect (kills tunnel TCP flows)…", flush=True)
    subprocess.run(["mullvad", "reconnect"], capture_output=True, timeout=60)
    ok = wait_online()
    print(f"[cycle {cycle}] back online: {ok}", flush=True)
    time.sleep(3)

    # THE critical request: first use of the handle after its pooled
    # connections were killed → libcurl prunes them → TLS shutdown write
    download(f"{cycle}b")

    if MODE == "close-handle":
        close_thread_curl()

print(f"SURVIVED mode={MODE} — no abort", flush=True)
