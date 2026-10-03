"""
newsletter_control.py -- start/stop, status and manual runs for the daily
market-close newsletter, for the Streamlit sidebar to drive.

The "newsletter program" is not a resident process. It is a launchd LaunchAgent
(com.leon.dailyclose) that wakes five times per weekday, runs daily_close.py for
a few minutes and exits. So "is it running?" has two quite different answers and
the UI needs both:

  * Is the SCHEDULE armed? -- launchctl bootstrap/bootout + enable/disable.
  * Is it actually DELIVERING? -- the last completed NYSE session either is or is
    not in sent_sessions.txt. This is the signal that matters: the agent can be
    perfectly loaded and still have lost a session because the Mac slept through
    every slot, which is exactly what happened to 2026-09-25.

Deliberately free of Streamlit imports so it can be driven from a shell, and it
never imports daily_close at module scope -- that would drag in yfinance and
news_fetcher for what is mostly a few file reads.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

HERE = Path(__file__).resolve().parent
PROJECT_ROOT = HERE.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

import market_calendar as mc  # noqa: E402

LABEL = "com.leon.dailyclose"
REPO_PLIST = HERE / f"{LABEL}.plist"
INSTALLED_PLIST = Path.home() / "Library" / "LaunchAgents" / f"{LABEL}.plist"

REPORTS_DIR = HERE / "reports"
SENT_LOG = REPORTS_DIR / "sent_sessions.txt"
RUN_STATE = REPORTS_DIR / "run_state.json"
UNIVERSE_CACHE = REPORTS_DIR / "universe_cache.json"
MANUAL_RUN = REPORTS_DIR / "manual_run.json"
MANUAL_LOG = REPORTS_DIR / "manual_run.log"
DAILY_LOG = REPORTS_DIR / "daily_close.log"

CST = ZoneInfo("America/Chicago")
ET = ZoneInfo("America/New_York")

# Mirrors StartCalendarInterval in the plist. Kept as data rather than parsed
# back out of the XML: the plist is 25 near-identical dicts and this is the one
# fact the UI needs from it.
SLOTS = [(8, 0), (16, 45), (17, 30), (18, 30), (20, 0), (21, 0), (22, 0), (23, 0)]

_TIMEOUT_S = 15


# ─────────────────────────────────────────────────────────────────────────────
# launchctl
# ─────────────────────────────────────────────────────────────────────────────
def _domain() -> str:
    return f"gui/{os.getuid()}"


def _launchctl(*args: str) -> tuple[int, str]:
    """Run launchctl; returns (rc, combined output). Never raises."""
    try:
        r = subprocess.run(["launchctl", *args], capture_output=True, text=True,
                           timeout=_TIMEOUT_S)
        return r.returncode, (r.stdout or "") + (r.stderr or "")
    except Exception as exc:
        return 127, f"{type(exc).__name__}: {exc}"


def _is_disabled() -> bool:
    """True when the label carries a persistent disable override.

    Matters because `bootout` alone does not survive a logout: anything in
    ~/Library/LaunchAgents is bootstrapped again at the next login, so a Stop
    that only booted the job out would quietly turn itself back on.
    """
    rc, out = _launchctl("print-disabled", _domain())
    if rc != 0:
        return False
    for line in out.splitlines():
        if f'"{LABEL}"' in line:
            return line.strip().endswith("disabled") or line.strip().endswith("true")
    return False


def schedule_state() -> dict:
    """Everything the badge needs, from two cheap launchctl calls."""
    installed = INSTALLED_PLIST.exists()
    in_sync = False
    if installed and REPO_PLIST.exists():
        try:
            in_sync = INSTALLED_PLIST.read_bytes() == REPO_PLIST.read_bytes()
        except Exception:
            in_sync = False

    rc, out = _launchctl("list", LABEL)
    loaded = rc == 0
    pid, last_exit = None, None
    if loaded:
        for line in out.splitlines():
            line = line.strip()
            if line.startswith('"PID"'):
                try:
                    pid = int(line.split("=")[1].strip().rstrip(";"))
                except Exception:
                    pass
            elif line.startswith('"LastExitStatus"'):
                try:
                    last_exit = int(line.split("=")[1].strip().rstrip(";"))
                except Exception:
                    pass

    disabled = _is_disabled()
    return {
        "installed": installed,
        "in_sync": in_sync,
        "loaded": loaded,
        "disabled": disabled,
        "on": loaded and not disabled,
        "pid": pid,
        "last_exit_code": last_exit,
    }


def start_schedule() -> tuple[bool, str]:
    """Install (or refresh) the plist and arm the schedule."""
    if not REPO_PLIST.exists():
        return False, f"{REPO_PLIST.name} is missing from the repo"
    try:
        INSTALLED_PLIST.parent.mkdir(parents=True, exist_ok=True)
        if (not INSTALLED_PLIST.exists()
                or INSTALLED_PLIST.read_bytes() != REPO_PLIST.read_bytes()):
            shutil.copyfile(REPO_PLIST, INSTALLED_PLIST)
    except Exception as exc:
        return False, f"could not install the plist: {type(exc).__name__}: {exc}"

    _launchctl("enable", f"{_domain()}/{LABEL}")
    _launchctl("bootout", f"{_domain()}/{LABEL}")  # ignore rc: usually not loaded
    rc, out = _launchctl("bootstrap", _domain(), str(INSTALLED_PLIST))
    if rc != 0:
        rc2, out2 = _launchctl("load", "-w", str(INSTALLED_PLIST))
        if rc2 != 0:
            return False, f"launchctl bootstrap failed: {out.strip() or out2.strip()}"
    return True, "Schedule started."


def stop_schedule() -> tuple[bool, str]:
    """Disarm the schedule, persistently."""
    rc, out = _launchctl("bootout", f"{_domain()}/{LABEL}")
    rc2, out2 = _launchctl("disable", f"{_domain()}/{LABEL}")
    if rc != 0 and rc2 != 0:
        return False, f"launchctl could not stop it: {out.strip() or out2.strip()}"
    return True, "Schedule stopped."


def next_fire_dt(now: datetime | None = None) -> datetime | None:
    """Next scheduled slot, in ET. None if the schedule is not armed."""
    now = now or datetime.now(ET)
    for day_offset in range(0, 8):
        d = (now + timedelta(days=day_offset)).date()
        if d.weekday() > 4:  # plist covers Weekday 1-5 only
            continue
        for hh, mm in sorted(SLOTS):
            cand = datetime(d.year, d.month, d.day, hh, mm, tzinfo=ET)
            if cand > now:
                return cand
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Delivery history / health
# ─────────────────────────────────────────────────────────────────────────────
def sent_sessions() -> list[str]:
    """Sessions actually emailed, deduped and sorted. The file is append-only
    and does contain duplicates (a --force resend adds a second line)."""
    try:
        return sorted(set(SENT_LOG.read_text(encoding="utf-8").split()))
    except Exception:
        return []


def run_state() -> dict:
    try:
        return json.loads(RUN_STATE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def last_sent() -> tuple[str | None, datetime | None]:
    """(session date, when it was sent). The timestamp comes from run_state.json
    when it describes that same session, else the archived report's mtime."""
    sessions = sent_sessions()
    if not sessions:
        return None, None
    session = sessions[-1]

    st = run_state()
    if st.get("session") == session and st.get("outcome") == "sent":
        try:
            return session, datetime.fromisoformat(st["finished_at"])
        except Exception:
            pass
    archive = REPORTS_DIR / f"{session}_close.html"
    try:
        return session, datetime.fromtimestamp(archive.stat().st_mtime, CST)
    except Exception:
        return session, None


def health() -> dict:
    """Is the newsletter actually delivering?

    `missing` is the load-bearing field: the last completed NYSE session that
    never made it into sent_sessions.txt. A perfectly loaded agent still shows a
    missing session if the machine was asleep for all five slots.
    """
    target = mc.last_completed_trading_day()
    sent = sent_sessions()
    st = run_state()
    return {
        "target": target,
        "missing": target if (target and target not in sent) else None,
        "last_outcome": st.get("outcome"),
        "last_detail": st.get("detail", ""),
        "last_finished_at": st.get("finished_at"),
    }


def universe_cache_asof() -> str | None:
    try:
        return json.loads(UNIVERSE_CACHE.read_text(encoding="utf-8")).get("as_of")
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Recipients
# ─────────────────────────────────────────────────────────────────────────────
def _mailer():
    if str(HERE) not in sys.path:
        sys.path.append(str(HERE))
    import mailer
    return mailer


def get_recipients() -> tuple[list[str], str]:
    """(addresses, source) where source is 'recipients.json' or '.env'."""
    m = _mailer()
    source = "recipients.json" if m.RECIPIENTS_FILE.exists() else ".env"
    try:
        blob = json.loads(m.RECIPIENTS_FILE.read_text(encoding="utf-8"))
        if not [a for a in (blob.get("to") or []) if str(a).strip()]:
            source = ".env"
    except Exception:
        source = ".env"
    return m.recipients(), source


def validate_email(addr: str) -> bool:
    """Good enough to catch typos; not an RFC 5322 parser."""
    from email.utils import parseaddr
    _, a = parseaddr(addr)
    if a != addr.strip() or a.count("@") != 1:
        return False
    local, _, domain = a.partition("@")
    return bool(local) and "." in domain and not domain.startswith(".") \
        and not domain.endswith(".") and " " not in a


def set_recipients(addresses: list[str]) -> tuple[bool, str]:
    """Validate and persist the recipient list. Returns (ok, error)."""
    cleaned, seen = [], set()
    bad = []
    for raw in addresses:
        a = str(raw).strip()
        if not a:
            continue
        if not validate_email(a):
            bad.append(a)
            continue
        if a.lower() in seen:
            continue
        seen.add(a.lower())
        cleaned.append(a)

    if bad:
        return False, "Not a valid email address: " + ", ".join(bad)
    if not cleaned:
        return False, ("At least one recipient is required. To stop delivery, "
                       "stop the schedule instead.")

    m = _mailer()
    try:
        REPORTS_DIR.mkdir(exist_ok=True)
        tmp = m.RECIPIENTS_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({
            "to": cleaned,
            "updated_at": datetime.now(CST).isoformat(timespec="seconds"),
        }, indent=2), encoding="utf-8")
        # Atomic: a launchd run reading this mid-write would otherwise see a
        # truncated file and fall back to .env without saying so.
        tmp.replace(m.RECIPIENTS_FILE)
    except Exception as exc:
        return False, f"Could not save: {type(exc).__name__}: {exc}"
    return True, ""


# ─────────────────────────────────────────────────────────────────────────────
# Manual runs
# ─────────────────────────────────────────────────────────────────────────────
def _uv() -> str:
    return shutil.which("uv") or "/opt/homebrew/bin/uv"


def _alive(pid: int | None) -> bool:
    """PID liveness, with a cmdline check so a recycled PID cannot masquerade as
    a newsletter run that is still going."""
    if not pid:
        return False
    try:
        import psutil
        if not psutil.pid_exists(pid):
            return False
        return "daily_close.py" in " ".join(psutil.Process(pid).cmdline())
    except Exception:
        return False


def manual_run_state() -> dict:
    """State of a run launched from the UI. Lives on disk, not in session state,
    so a browser reload or an app restart still sees an in-flight run."""
    try:
        blob = json.loads(MANUAL_RUN.read_text(encoding="utf-8"))
    except Exception:
        return {"running": False}
    pid = blob.get("pid")
    running = _alive(pid)
    started = None
    try:
        started = datetime.fromisoformat(blob["started_at"])
    except Exception:
        pass
    elapsed = (datetime.now(CST) - started).total_seconds() if started else None
    return {"running": running, "pid": pid, "started_at": started,
            "elapsed_s": elapsed, "cmd": blob.get("cmd", []),
            "force": blob.get("force", False)}


def start_manual_run(force: bool = False, email: bool = True) -> tuple[bool, str]:
    """Launch daily_close.py detached. Returns (ok, message)."""
    if manual_run_state().get("running"):
        return False, "A newsletter run is already in progress."
    if schedule_state().get("pid"):
        return False, "launchd is running the newsletter right now."

    # caffeinate mirrors _scan_entry in app.py: a full run takes ~4-5 minutes and
    # would otherwise be killed by the lid closing partway through.
    cmd = ["caffeinate", "-i", _uv(), "run", "--directory", str(PROJECT_ROOT),
           "python", "daily_email/daily_close.py"]
    if email:
        cmd.append("--email")
    if force:
        cmd.append("--force")

    try:
        REPORTS_DIR.mkdir(exist_ok=True)
        fh = MANUAL_LOG.open("wb")  # truncate: this log is for the current run
        proc = subprocess.Popen(
            cmd, cwd=str(PROJECT_ROOT), stdin=subprocess.DEVNULL,
            stdout=fh, stderr=subprocess.STDOUT,
            # Detached: survives Streamlit reruns, reconnects and restarts.
            start_new_session=True,
        )
    except Exception as exc:
        return False, f"Could not start the run: {type(exc).__name__}: {exc}"

    try:
        MANUAL_RUN.write_text(json.dumps({
            "pid": proc.pid,
            "started_at": datetime.now(CST).isoformat(timespec="seconds"),
            "cmd": cmd,
            "force": force,
            "log": str(MANUAL_LOG),
        }, indent=2), encoding="utf-8")
    except Exception:
        pass
    return True, f"Started (pid {proc.pid})."


def cancel_manual_run() -> tuple[bool, str]:
    st = manual_run_state()
    if not st.get("running"):
        return False, "No run in progress."
    try:
        import psutil
        proc = psutil.Process(st["pid"])
        # caffeinate is the parent; the uv/python child holds the real work, so
        # kill the whole group rather than orphaning the fetch.
        for child in proc.children(recursive=True):
            child.terminate()
        proc.terminate()
        psutil.wait_procs([proc], timeout=5)
    except Exception as exc:
        return False, f"Could not cancel: {type(exc).__name__}: {exc}"
    return True, "Run cancelled."


def refresh_universe_cache(con) -> int:
    """Rewrite universe_cache.json using the caller's live DuckDB connection."""
    if str(HERE) not in sys.path:
        sys.path.append(str(HERE))
    import daily_close
    return daily_close.refresh_universe_cache(con)


def log_tail(path: Path, n: int = 20) -> list[str]:
    try:
        return path.read_text(encoding="utf-8", errors="replace").splitlines()[-n:]
    except Exception:
        return []
