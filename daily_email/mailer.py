"""
mailer.py -- Gmail SMTP sender for the daily close newsletter.

Credentials come from config.py (.env). GMAIL_APP_PASSWORD must be a Google
App Password, which requires 2-factor auth on the account; Google rejects the
plain account password over SMTP.

Recipients come from recipients.json when present (managed from the Streamlit
sidebar), falling back to NEWSLETTER_TO in .env. The file is the override rather
than the only source so a fresh clone behaves exactly as it did before it
existed, and so the list can be changed without an app restart -- daily_close.py
runs as a separate process and re-reads it on every run.

The password is never logged, and SMTP auth failures are reported as a pointer
to the App Password setup rather than a raw traceback that echoes the payload.
"""

from __future__ import annotations

import json
import smtplib
from email.message import EmailMessage
from email.utils import formataddr, formatdate
from pathlib import Path

import config

_SMTP_HOST = "smtp.gmail.com"
_SMTP_PORT = 465  # implicit TLS
_TIMEOUT_S = 60

RECIPIENTS_FILE = Path(__file__).parent / "recipients.json"


class MailError(RuntimeError):
    """Raised when the newsletter could not be sent."""


def recipients() -> list[str]:
    """Current recipient list: recipients.json if it holds any, else .env.

    Never raises -- a corrupt or unreadable file falls back to NEWSLETTER_TO so
    a bad edit cannot silently stop delivery altogether.
    """
    try:
        blob = json.loads(RECIPIENTS_FILE.read_text(encoding="utf-8"))
        addrs = [str(a).strip() for a in (blob.get("to") or []) if str(a).strip()]
        if addrs:
            return addrs
    except Exception:
        pass
    return [a.strip() for a in (config.NEWSLETTER_TO or "").split(",") if a.strip()]


def credentials_available() -> tuple[bool, str]:
    """Return (ok, reason). Lets the CLI fail loudly *before* doing a minute of
    fetching, rather than after."""
    if not config.GMAIL_USER:
        return False, "GMAIL_USER is not set in .env"
    if not config.GMAIL_APP_PASSWORD:
        return False, "GMAIL_APP_PASSWORD is not set in .env"
    if not recipients():
        return False, ("no newsletter recipients configured (add one in the Streamlit "
                       "sidebar, or set NEWSLETTER_TO in .env)")
    return True, ""


def send_html(subject: str, html_body: str, text_body: str,
              to: str | list[str] | None = None,
              sender_name: str = "Market Close") -> str:
    """Send one multipart/alternative email. Returns the recipients, comma-joined.

    All recipients go on a single To header; smtplib derives the envelope from
    it, so a multi-address send is still one message and one SMTP transaction.
    """
    ok, reason = credentials_available()
    if not ok:
        raise MailError(reason)

    if to is None:
        recips = recipients()
    elif isinstance(to, str):
        recips = [a.strip() for a in to.split(",") if a.strip()]
    else:
        recips = [str(a).strip() for a in to if str(a).strip()]
    if not recips:
        raise MailError("no newsletter recipients configured")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr((sender_name, config.GMAIL_USER))
    msg["To"] = ", ".join(recips)
    msg["Date"] = formatdate(localtime=True)
    msg.set_content(text_body)
    msg.add_alternative(html_body, subtype="html")

    try:
        with smtplib.SMTP_SSL(_SMTP_HOST, _SMTP_PORT, timeout=_TIMEOUT_S) as smtp:
            smtp.login(config.GMAIL_USER, config.GMAIL_APP_PASSWORD)
            smtp.send_message(msg)
    except smtplib.SMTPAuthenticationError:
        raise MailError(
            "Gmail rejected the login. GMAIL_APP_PASSWORD must be a 16-character "
            "App Password (https://myaccount.google.com/apppasswords), not the "
            "account password, and 2FA must be enabled on the account."
        ) from None
    except (smtplib.SMTPException, OSError) as exc:
        raise MailError(f"SMTP send failed: {type(exc).__name__}: {exc}") from None

    return ", ".join(recips)
