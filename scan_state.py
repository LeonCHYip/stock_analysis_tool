"""scan_state.py — Process-level scan state that survives Streamlit reruns.

Streamlit re-executes app.py in a BRAND-NEW module namespace on every rerun
(any widget click, tab reconnect), so module-level globals defined in app.py
reset constantly -- empirically confirmed: the old _PROC_STARTUP dict-flag
guard "fired once per process" actually fired ~990 times (once per rerun),
and app.py's old module-level _PROC_SCAN could never actually be seen by a
different execution, which meant (a) the progress bar never appeared for
scans a session didn't start itself (auto-scans, other tabs), and (b) the
stop-previous-scan / already-running / overlap-join guards silently never
worked across executions.

IMPORTED modules, by contrast, are cached in sys.modules and NOT re-executed
per rerun -- the same trick yf_session.py already relies on for its
persistent worker pool and YF_DL_LOCK. State that must be shared across
reruns/sessions/threads therefore lives HERE, and app.py imports it.

Access convention: read/write via attribute access (scan_state.PROC_SCAN),
holding PROC_SCAN_LOCK for any check-then-act sequence.
"""
import threading

PROC_SCAN_LOCK = threading.Lock()

# The currently running scan, or None. Shape (set by app.py's
# _start_scan_thread): {"progress": dict, "pause_event": Event,
# "stop_event": Event, "analysis_dt": str, "thread": Thread}
PROC_SCAN: dict | None = None

# Stale-ticker banner state, written by the hourly background check and
# after every scan (app.py's _refresh_stale_count), read into
# st.session_state on every rerun for the sidebar warning banner.
STARTUP: dict = {"stale": {"count": 0, "target": ""}}

# Monotonic data-version counter. app.py's @st.cache_data load wrappers key on
# this (combined with storage.get_latest_run_datetime()) so filter/sort reruns
# hit the cache instead of re-querying the DB. Bumped by app.py at every
# user-facing write path (edits, scan finalize) so a change invalidates the
# cache on the next rerun. Lives here (not app.py) because app.py's module
# globals reset on every Streamlit rerun -- see this module's docstring.
DATA_VERSION: int = 0
