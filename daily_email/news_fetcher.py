"""
news_fetcher.py -- Google News RSS headlines for the daily close newsletter.

Google News exposes an undocumented RSS search endpoint that supports a
`when:<N>h` recency operator, which is how the newsletter restricts itself to
headlines from the last 48 hours.  Yahoo's search endpoint was evaluated and
rejected: it mostly returns evergreen "analysis" pieces that have nothing to do
with the day's move.

Nothing here raises.  A failed fetch returns an empty list so that one bad
ticker can never abort the newsletter.
"""

from __future__ import annotations

import html
import re
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

_RSS = "https://news.google.com/rss/search"
_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"

# Politeness delay between consecutive Google News requests. ~20 movers per run
# is well inside anything Google throttles, but the newsletter is unattended so
# it should not hammer.
_DELAY_S = 0.3
_TIMEOUT_S = 20

_last_request_at = 0.0


def _throttle() -> None:
    global _last_request_at
    wait = _DELAY_S - (time.monotonic() - _last_request_at)
    if wait > 0:
        time.sleep(wait)
    _last_request_at = time.monotonic()


def _clean_title(title: str) -> tuple[str, str]:
    """Google News appends ' - <Publisher>' to every headline. Split it off so
    the publisher can be rendered separately. Returns (headline, publisher)."""
    title = html.unescape(title or "").strip()
    m = re.match(r"^(.*)\s+-\s+([^-]+)$", title)
    if m:
        return m.group(1).strip(), m.group(2).strip()
    return title, ""


def _parse_pubdate(raw: str | None) -> datetime | None:
    if not raw:
        return None
    try:
        dt = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _age_label(dt: datetime | None) -> str:
    if dt is None:
        return ""
    delta = datetime.now(timezone.utc) - dt
    hours = delta.total_seconds() / 3600.0
    if hours < 1:
        return f"{max(int(delta.total_seconds() // 60), 1)}m ago"
    if hours < 24:
        return f"{int(hours)}h ago"
    return f"{int(hours // 24)}d ago"


def fetch_headlines(query: str, hours: int = 48, limit: int = 3) -> list[dict]:
    """Return up to `limit` Google News items for `query`, newest first.

    Each item: {title, publisher, link, published (datetime|None), age}.
    Returns [] on any failure -- callers render "no recent news" instead.
    """
    if not query:
        return []

    q = urllib.parse.quote_plus(f"{query} when:{hours}h")
    url = f"{_RSS}?q={q}&hl=en-US&gl=US&ceid=US:en"

    try:
        _throttle()
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:
            raw = resp.read()
        root = ET.fromstring(raw)
    except (urllib.error.URLError, ET.ParseError, OSError, ValueError):
        return []

    # Google's `when:` filter is applied server-side, but it has been seen to
    # leak slightly older items, so the cutoff is enforced here too.
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    items: list[dict] = []
    for node in root.findall(".//item"):
        title, publisher = _clean_title(node.findtext("title") or "")
        if not title:
            continue
        published = _parse_pubdate(node.findtext("pubDate"))
        if published is not None and published < cutoff:
            continue
        items.append({
            "title": title,
            "publisher": publisher or (node.findtext("source") or "").strip(),
            "link": (node.findtext("link") or "").strip(),
            "published": published,
            "age": _age_label(published),
        })

    items.sort(key=lambda it: it["published"] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)
    return items[:limit]


def build_query(ticker: str, name: str | None) -> str:
    """Company name is far more targeted than a bare ticker (which collides with
    unrelated acronyms), so prefer it and keep the ticker only as a fallback."""
    name = (name or "").strip()
    if name:
        # Trim corporate suffixes -- they add nothing and dilute the match.
        name = re.sub(r"\b(Inc|Corp|Corporation|Incorporated|Company|Co|Ltd|plc|PLC|Holdings|Group|NV|SA|AG)\b\.?,?\s*$",
                      "", name).strip().rstrip(",")
    if name:
        return f'"{name}" stock'
    return f"{ticker} stock"


# ─────────────────────────────────────────────────────────────────────────────
# Article bodies
# ─────────────────────────────────────────────────────────────────────────────
# Google News RSS is excellent for *finding* relevant, recent stories, but its
# links are JS redirects whose real URL is encoded in the article id -- neither
# base64-decoding the id nor Google's batchexecute resolver works any more, so
# those links cannot be followed to fetch text.
#
# Yahoo's per-ticker feed returns DIRECT publisher URLs, so it is used as the
# body source. The two are merged and de-duplicated by title.

import concurrent.futures
import json as _json

_BODY_TIMEOUT_S = 10
_MAX_BODY_CHARS = 1500
_MAX_PAGE_BYTES = 2_000_000
_FETCH_WORKERS = 24


def _strip(fragment: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", "", fragment))).strip()


def extract_article_text(raw_html: str) -> str:
    """Pull the article body out of a page, preferring structured markup.

    A naive <p> sweep picks up navigation chrome ("Skip to main content",
    market tickers, subscribe prompts), so JSON-LD and <article> are tried
    first and the plain sweep is never used.
    """
    for m in re.finditer(r"(?is)<script[^>]+application/ld\+json[^>]*>(.*?)</script>", raw_html):
        try:
            data = _json.loads(m.group(1).strip())
        except Exception:
            continue
        for node in (data if isinstance(data, list) else [data]):
            if isinstance(node, dict):
                body = node.get("articleBody")
                if isinstance(body, str) and len(body) > 250:
                    return _strip(body)

    art = re.search(r"(?is)<article[^>]*>(.*?)</article>", raw_html)
    if art:
        paras = re.findall(r"(?is)<p[^>]*>(.*?)</p>", art.group(1))
        text = _strip(" ".join(paras))
        if len(text) > 250:
            return text

    m = re.search(r'(?is)<meta[^>]+property=["\']og:description["\'][^>]+content=["\'](.*?)["\']',
                  raw_html)
    return _strip(m.group(1)) if m else ""


# Chrome that survives structured extraction on Yahoo-hosted pages. Left in
# place it would consume most of the per-article character budget before any
# real reporting is reached.
_BOILERPLATE = [
    r"Explore stocks on \w+",
    r"Trading disclosure",
    r"The above button links to [^.]+\.",
    r"Yahoo Finance is not a broker-dealer[^.]*\.",
    r"Oops, something went wrong",
    r"Skip to (navigation|main content|right column)",
    r"U\.S\. markets (open|close) in [^.]{0,30}",
    r"\b\d+ min read\b",
    r"(Mon|Tue|Wed|Thu|Fri|Sat|Sun), \w+ \d{1,2}, \d{4} at [\d:]+ ?[AP]M [A-Z]{2,4}",
    r"Sign in to access your portfolio",
    r"Story Continues",
    r"Coinbase pays us for certain activity[^.]*\.",
    r"Prices displayed are informational[^.]*\.",
    r"Trade \w{1,6} on Coinbase",
]
_BOILERPLATE_RE = re.compile("|".join(_BOILERPLATE), re.I)


def scrub_boilerplate(text: str) -> str:
    return re.sub(r"\s+", " ", _BOILERPLATE_RE.sub(" ", text)).strip()


def _fetch_body(url: str) -> str:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": _UA})
        with urllib.request.urlopen(req, timeout=_BODY_TIMEOUT_S) as resp:
            # Yahoo article pages run ~800 KB; a smaller cap truncates before
            # </article> and silently degrades the body to a meta description.
            raw = resp.read(_MAX_PAGE_BYTES).decode("utf-8", errors="replace")
        return scrub_boilerplate(extract_article_text(raw))[:_MAX_BODY_CHARS]
    except Exception:
        return ""


def fetch_yahoo_news(ticker: str, hours: int = 48, limit: int = 10) -> list[dict]:
    """Recent stories for one ticker from Yahoo, with direct publisher links.

    Uses yfinance, which routes through curl_cffi/libcurl-impersonate. Yahoo's
    search endpoint answers 429 to plain urllib because it wants the
    cookie/crumb handshake curl_cffi performs, so this dependency is not
    avoidable if article bodies are wanted.

    libcurl-impersonate has aborted the process (SIGABRT inside SSL_write)
    during a live run while Yahoo was rate-limiting. A native abort cannot be
    caught in Python, which is why callers run this module as a SUBPROCESS --
    see __main__ below and daily_close.attach_news.
    """
    try:
        import yfinance as yf
        raw = yf.Ticker(ticker).news or []
    except Exception:
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    items: list[dict] = []
    for entry in raw:
        c = entry.get("content") or entry
        title = (c.get("title") or "").strip()
        if not title:
            continue

        link = c.get("clickThroughUrl") or c.get("canonicalUrl") or {}
        link = link.get("url", "") if isinstance(link, dict) else (c.get("link") or "")
        if not link:
            continue

        published = None
        if c.get("pubDate"):
            try:
                published = datetime.fromisoformat(str(c["pubDate"]).replace("Z", "+00:00"))
            except ValueError:
                published = None
        elif c.get("providerPublishTime"):
            try:
                published = datetime.fromtimestamp(float(c["providerPublishTime"]), timezone.utc)
            except (TypeError, ValueError, OSError):
                published = None
        if published is not None and published < cutoff:
            continue

        prov = c.get("provider")
        publisher = (prov.get("displayName", "") if isinstance(prov, dict)
                     else str(c.get("publisher") or ""))
        items.append({"title": title, "publisher": publisher, "link": link,
                      "published": published, "age": _age_label(published), "body": ""})
    return items[:limit]


def _norm_title(t: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (t or "").lower())[:70]


def gather_meta(ticker: str, name: str | None, hours: int = 48,
                max_articles: int = 6) -> list[dict]:
    """Merge Google News + Yahoo results, WITHOUT fetching bodies (fast).

    Google supplies breadth and recency ranking; Yahoo supplies followable
    links. Items whose body cannot be retrieved still carry their headline,
    which is often enough on its own ("Why X Stock Is Jumping as ...").
    """
    google = fetch_headlines(build_query(ticker, name), hours=hours, limit=max_articles)
    yahoo = fetch_yahoo_news(ticker, hours=hours, limit=max_articles)

    merged: list[dict] = []
    seen: set[str] = set()
    for item in yahoo + google:            # Yahoo first: its links are fetchable
        key = _norm_title(item["title"])
        if key and key not in seen:
            seen.add(key)
            merged.append({**item, "body": item.get("body", "")})
    return merged[:max_articles]


def fill_bodies(articles: list[dict], deadline_s: float = 180.0) -> int:
    """Fetch article text for many articles at once, bounded by a wall clock.

    One shared pool across EVERY ticker rather than a pool per ticker: per-ticker
    pools ran serially and cost ~40 s each, which overran the caller's timeout
    at 20 tickers. Whatever has not returned by the deadline keeps its headline
    and an empty body.
    """
    targets = [a for a in articles if a.get("link") and "news.google.com" not in a["link"]]
    if not targets:
        return 0

    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=_FETCH_WORKERS) as pool:
        futures = {pool.submit(_fetch_body, a["link"]): a for a in targets}
        try:
            for fut in concurrent.futures.as_completed(futures, timeout=deadline_s):
                try:
                    body = fut.result()
                except Exception:
                    body = ""
                if body:
                    futures[fut]["body"] = body
                    done += 1
        except concurrent.futures.TimeoutError:
            pass
        for fut in futures:                      # never block on stragglers
            fut.cancel()
    return done


def gather_with_bodies(ticker: str, name: str | None, hours: int = 48,
                       max_articles: int = 6) -> list[dict]:
    """Single-ticker convenience wrapper (metadata + bodies)."""
    arts = gather_meta(ticker, name, hours=hours, max_articles=max_articles)
    fill_bodies(arts)
    return arts



# ─────────────────────────────────────────────────────────────────────────────
# Subprocess entry point
# ─────────────────────────────────────────────────────────────────────────────
# Run as: python news_fetcher.py  with a JSON spec on stdin, JSON on stdout.
# Isolates the curl_cffi-backed Yahoo calls so a native abort takes down only
# this child, leaving the newsletter to continue without reasons.

def _cli_main() -> int:
    import sys
    try:
        spec = _json.loads(sys.stdin.read())
        hours = int(spec.get("hours", 48))
        limit = int(spec.get("limit", 6))
        deadline = float(spec.get("body_deadline", 180))
        out, every = {}, []
        for m in spec.get("movers", []):
            arts = gather_meta(m["ticker"], m.get("name"), hours=hours, max_articles=limit)
            out[m["ticker"]] = arts
            every.extend(arts)
        filled = fill_bodies(every, deadline_s=deadline)
        sys.stderr.write(f"bodies: {filled}/{len(every)}\n")
        out = {t: [{k: v for k, v in a.items() if k != "published"} for a in arts]
               for t, arts in out.items()}
        sys.stdout.write(_json.dumps(out))
        return 0
    except Exception as exc:
        sys.stderr.write(f"{type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(_cli_main())
