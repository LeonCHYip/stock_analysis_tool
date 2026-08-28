"""Shared curl_cffi session for all yfinance calls in this process.

yfinance 1.x creates a NEW curl_cffi session on every yf.download() call
(multi.py: `session = session or requests.Session(impersonate="chrome")`)
and its YfData singleton then replaces the previous session (data.py:
`self._session = session`), dropping the last reference to it.  When the old
session's native curl handles are garbage-collected, curl_cffi 0.13 aborts
the entire process (SIGABRT in Curl.close via __del__) — no Python exception,
the Streamlit server just dies.  Observed crashing at the start of scan
batch 2, the first time a previous session gets destroyed.

Passing this one persistent session to every yf.download() / yf.Ticker()
call means no session is ever replaced or destroyed, so the crashing path
never runs.  Sharing one session across threads matches yfinance's own
design ("one session one cookie shared by all threads").
"""

import threading
from concurrent.futures import ThreadPoolExecutor

from curl_cffi import requests as _curl_requests
from curl_cffi.curl import Curl as _Curl

# ── Neutralize cross-thread curl handle destruction ──────────────────────────
# Curl.__del__ calls curl.close(). When a thread that used the session dies,
# its thread-local Curl handle becomes garbage and __del__ runs on whatever
# thread performs the collection — a cross-thread native cleanup that corrupts
# libcurl state and aborts the whole process (observed as SIGABRT in
# Curl.close and later in Curl.perform; no Python exception is raised).
# Streamlit makes this unavoidable: every script rerun executes in a fresh
# short-lived ScriptRunner thread, so any synchronous yfinance call — or even
# importing this module (Session.__init__ eagerly creates a handle) — plants
# a handle in a thread that is about to die.
#
# Skipping __del__ trades that crash for a small bounded leak: one native
# handle per dead thread that used yfinance. Deliberate cleanup still works —
# Session.close() and close_thread_curl() below call curl.close() explicitly.
_Curl.__del__ = lambda self: None

# HTTP/1.1 only — every observed crash (6/6 macOS crash reports) aborted in
# libcurl-impersonate's HTTP/2 connection shutdown while pruning stale pooled
# connections:  Curl_cpool_prune_dead → cf_h2_shutdown → h2_progress_egress →
# SSL_write → bssl::ssl_write_buffer_flush → abort().  BoringSSL's assertion
# fires when writing the HTTP/2 GOAWAY frame on a half-closed TLS connection.
# HTTP/1.1 connections are discarded with a plain socket close (no GOAWAY, no
# TLS shutdown write), making the aborting path unreachable.  Yahoo's API
# behaves identically over HTTP/1.1.
from curl_cffi import CurlHttpVersion as _CurlHttpVersion
from curl_cffi import CurlOpt as _CurlOpt

# FORBID_REUSE — the decisive fix, verified against a deterministic repro
# (vpn_kill_repro.py: download → `mullvad reconnect` → download; without this
# the second download aborts with SIGABRT, with it the repro survives).
# Every request closes its connection while it is still healthy, so the
# connection pool is always empty and Curl_cpool_prune_dead never finds a
# dead connection to gracefully shut down — the abort path needs one.
# Cost: a TLS handshake per request (~+20s per 100-ticker batch).
YF_SESSION = _curl_requests.Session(
    impersonate="chrome",
    http_version=_CurlHttpVersion.V1_1,
    curl_options={_CurlOpt.FORBID_REUSE: 1},
)

# ── Bulk-download serialization lock ──────────────────────────────────────────
# yfinance price-history downloads (yf.download / Ticker.history) are NOT safe
# to run concurrently from multiple threads over this shared session:
# responses get crossed between threads. Reproduced deterministically (3/3
# trials): a concurrent scan download + dashboard download lost 3-4 tickers
# per run ("No bulk data"), and in one trial DRAM (a ~98-row recent listing)
# received a 759-row response belonging to another ticker's request.
# Serializing the same workload with this lock: 0 failures (3/3 clean).
#
# Every yf.download()/history() call site for PRICE HISTORY must hold this
# lock (technical_fetcher, swing_analysis, fx_rates, data_fetcher). The
# quoteSummary/timeseries JSON fetches used by the fundamentals workers are
# left unlocked -- they have always run 3-4 threads concurrently in
# production; serializing them would multiply scan time.
YF_DL_LOCK = threading.Lock()

# ── Persistent worker pool ────────────────────────────────────────────────────
# The session keeps one native curl handle per thread (use_thread_local_curl).
# When a thread that used the session dies, its handle becomes garbage and is
# destroyed by whichever thread runs GC — a cross-thread native cleanup that
# corrupts curl state and later aborts the process (observed twice: once in
# Curl.close via __del__, once in Curl.perform on the next request).
#
# Therefore: threads that touch YF_SESSION must be long-lived.  The scan's
# fundamental/peer workers get their threads from this single process-lifetime
# pool instead of a fresh ThreadPoolExecutor per batch.  This module is
# imported (not re-executed) on Streamlit reruns, so the pool persists.

_pool_lock = threading.Lock()
_fund_pool: ThreadPoolExecutor | None = None


def get_fund_pool(max_workers: int) -> ThreadPoolExecutor:
    """Return the shared long-lived worker pool, creating it on first use.
    max_workers only applies on first creation."""
    global _fund_pool
    with _pool_lock:
        if _fund_pool is None:
            _fund_pool = ThreadPoolExecutor(
                max_workers=max_workers, thread_name_prefix="fund-worker"
            )
        return _fund_pool


def close_thread_curl() -> None:
    """Close the CALLING thread's curl handle before the thread exits.

    Safe because the owning thread does its own cleanup — the crash only
    happens when a handle is destroyed from a different thread via GC.
    Call this at the end of any ephemeral thread that used yfinance."""
    local = getattr(YF_SESSION, "_local", None)
    curl = getattr(local, "curl", None) if local is not None else None
    if curl is not None:
        try:
            curl.close()
        except Exception:
            pass
        local.curl = None


# Session.__init__ eagerly created a curl handle in the IMPORTING thread's
# local storage. Under Streamlit that thread is a short-lived ScriptRunner
# thread, so release the handle now, in the owning thread, while it's alive.
close_thread_curl()
