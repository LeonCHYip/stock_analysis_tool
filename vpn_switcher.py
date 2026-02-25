"""
vpn_switcher.py
---------------
Mullvad VPN server rotation helper for scan batches.
Requires Mullvad app + CLI installed and accessible in PATH.
"""

import shutil
import subprocess
import time
import requests

_IPIFY = "https://api.ipify.org?format=json"


def is_available() -> bool:
    """Returns True if mullvad CLI is found in PATH."""
    return shutil.which("mullvad") is not None


def _run(cmd: list[str], timeout: int = 30) -> str:
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return (res.stdout or "").strip()


def _current_ip(timeout: int = 8) -> str | None:
    try:
        r = requests.get(_IPIFY, timeout=timeout)
        return r.json()["ip"]
    except Exception:
        return None


def _wait_for_internet(timeout: int = 90) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if _current_ip(timeout=5) is not None:
            return True
        time.sleep(1)
    return False


def _wait_for_ip_change(old_ip: str, timeout: int = 120) -> str | None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ip = _current_ip(timeout=5)
        if ip and ip != old_ip:
            return ip
        time.sleep(2)
    return None


def switch_server(country: str, log=print) -> bool:
    """
    Switch Mullvad to the given country code (e.g. 'us', 'nl', 'de').
    Returns True on success, False if anything goes wrong.
    """
    if not is_available():
        log("  [vpn] Mullvad CLI not found — skipping server switch")
        return False

    old_ip = _current_ip()
    log(f"  [vpn] Switching to {country.upper()} (current IP: {old_ip})")

    try:
        _run(["mullvad", "relay", "set", "location", country])
        _run(["mullvad", "disconnect"], timeout=15)
        _run(["mullvad", "connect"],    timeout=15)
    except Exception as e:
        log(f"  [vpn] Command error: {e}")
        return False

    if not _wait_for_internet(timeout=90):
        log("  [vpn] Internet did not recover after switch — continuing anyway")
        return False

    new_ip = _wait_for_ip_change(old_ip or "", timeout=120)
    if new_ip:
        log(f"  [vpn] IP changed: {old_ip} → {new_ip}")
        return True
    else:
        log(f"  [vpn] IP did not change within timeout — continuing anyway")
        return False
