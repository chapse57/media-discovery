"""Read the few fields of a podcast RSS feed that identify the show and say whether it is alive."""
from __future__ import annotations

import datetime as dt
import email.utils
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass

NS = {"itunes": "http://www.itunes.com/dtds/podcast-1.0.dtd",
      "podcast": "https://podcastindex.org/namespace/1.0"}


@dataclass
class Feed:
    title: str | None
    author: str | None
    guid: str | None            # <podcast:guid>, stable across hosts and renames when present
    new_feed_url: str | None    # <itunes:new-feed-url>, the publisher's own "I moved" notice
    last_episode_at: dt.datetime | None
    episodes: int


def _date(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    try:
        d = email.utils.parsedate_to_datetime(s.strip())
    except (TypeError, ValueError):
        return None
    return d if d.tzinfo else d.replace(tzinfo=dt.timezone.utc)


def parse(body: bytes) -> Feed:
    root = ET.fromstring(body)
    ch = root.find("channel")
    if ch is None:
        raise ValueError("not an RSS feed: no <channel>")
    text = lambda path: (ch.findtext(path, namespaces=NS) or "").strip() or None  # noqa: E731
    dates = [d for d in (_date(i.findtext("pubDate")) for i in ch.findall("item")) if d]
    return Feed(text("title"), text("itunes:author"), text("podcast:guid"), text("itunes:new-feed-url"),
                max(dates) if dates else None, len(ch.findall("item")))


def url_key(url: str) -> str:
    """Same rule as bench/run.py: no scheme, no www., no query, no trailing slash; host lower-cased."""
    rest = re.sub(r"^[a-zA-Z]+://", "", url.strip())
    rest = re.sub(r"^www\.", "", rest, flags=re.I)
    rest = re.sub(r"[?#].*$", "", rest)
    rest = re.sub(r"/+$", "", rest)
    host, _, path = rest.partition("/")
    return host.lower() + (f"/{path}" if path else "")


def name_key(name: str | None) -> str:
    n = re.sub(r"^\s*the\s+", "", (name or "").lower())
    return re.sub(r"[\W_]+", " ", n).strip()       # Unicode letters count: non-English titles keep a key
