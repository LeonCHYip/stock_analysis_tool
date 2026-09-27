import os
from dotenv import load_dotenv

load_dotenv()

FMP_API_KEY = os.getenv("FMP_API_KEY", "")

# ── Daily close newsletter (daily_close.py) ──────────────────────────────────
# GMAIL_APP_PASSWORD is a 16-character Google App Password (requires 2FA on the
# account) -- NOT the account password. Generate one at
# https://myaccount.google.com/apppasswords
GEMINI_API_KEY     = os.getenv("GEMINI_API_KEY", "") or os.getenv("GOOGLE_API_KEY", "")
GMAIL_USER         = os.getenv("GMAIL_USER", "").strip()
# Google displays App Passwords as "abcd efgh ijkl mnop"; SMTP wants them
# unspaced, so pasting the displayed form verbatim is accepted here.
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "").replace(" ", "").strip()
NEWSLETTER_TO      = os.getenv("NEWSLETTER_TO", "").strip() or GMAIL_USER
