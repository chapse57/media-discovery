"""The two official catalogues: Podcast Index (API key) and Apple's iTunes Search/Lookup (no key).

No web pages are read. Podcast Index needs PODCASTINDEX_KEY and PODCASTINDEX_SECRET in the
environment; iTunes Lookup is public and batches up to 200 ids per request.
Both base URLs can be overridden so tests run against a local fake server.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.parse
from dataclasses import dataclass

from . import http

PI_BASE = os.environ.get("PODCASTINDEX_BASE", "https://api.podcastindex.org/api/1.0")
ITUNES_BASE = os.environ.get("ITUNES_BASE", "https://itunes.apple.com")


@dataclass
class Listing:
    """A show as a catalogue describes it, before we decide which of our shows it is."""
    source: str
    feed_url: str
    title: str | None
    author: str | None = None
    guid: str | None = None
    itunes_id: int | None = None
    pi_id: int | None = None


class SourceError(RuntimeError):
    pass


def _pi_headers() -> dict[str, str]:
    key, secret = os.environ.get("PODCASTINDEX_KEY"), os.environ.get("PODCASTINDEX_SECRET")
    if not key or not secret:
        raise SourceError("set PODCASTINDEX_KEY and PODCASTINDEX_SECRET (see env.example.cmd)")
    now = str(int(time.time()))
    return {"X-Auth-Key": key, "X-Auth-Date": now,
            "Authorization": hashlib.sha1((key + secret + now).encode()).hexdigest()}


def _json(r: http.Response) -> dict:
    if r.status != 200:
        raise SourceError(f"{r.url}: {r.error or r.status}")
    return json.loads(r.body)


def podcastindex(path: str, log) -> dict:
    r = http.get(f"{PI_BASE}/{path}", headers=_pi_headers())
    log.add("podcastindex", r)
    return _json(r)


def _from_pi(f: dict) -> Listing:
    return Listing("podcastindex", f.get("url") or f.get("originalUrl") or "", f.get("title"), f.get("author"),
                   f.get("podcastGuid") or None, f.get("itunesId") or None, f.get("id") or None)


def trending(n: int, log, lang: str = "en") -> list[Listing]:
    """Podcast Index's trending list: the seed and, on later runs, the place new shows show up."""
    d = podcastindex(f"podcasts/trending?max={n}&lang={lang}", log)
    return [_from_pi(f) for f in d.get("feeds", []) if f.get("url")]


def recent_new(n: int, log) -> list[Listing]:
    d = podcastindex(f"recent/newfeeds?max={n}", log)
    return [_from_pi(f) for f in d.get("feeds", []) if f.get("url")]


def by_guid(guid: str, log) -> Listing | None:
    d = podcastindex(f"podcasts/byguid?guid={guid}", log)
    f = d.get("feed") or {}
    return _from_pi(f) if f.get("url") else None


DEFAULT_TERMS = ("news", "technology", "business", "true crime", "comedy", "history", "science",
                 "health", "sports", "education", "politics", "society")


def itunes_search(n: int, log, terms: tuple[str, ...] = DEFAULT_TERMS, country: str = "US") -> list[Listing]:
    """Seed without a Podcast Index key: Apple's Search API, a few broad terms, first n distinct shows.
    Each result is an exact catalogue entry (Apple id + feed URL), not a guess about our data."""
    out: dict[int, Listing] = {}
    for term in terms:
        if len(out) >= n:
            break
        q = urllib.parse.urlencode({"term": term, "media": "podcast", "entity": "podcast", "limit": 200, "country": country})
        r = http.get(f"{ITUNES_BASE}/search?{q}")
        log.add("itunes", r)
        if r.status == 200:
            for res in json.loads(r.body).get("results", []):
                if res.get("feedUrl") and res.get("collectionId") not in out and len(out) < n:
                    out[res["collectionId"]] = Listing("itunes", res["feedUrl"], res.get("collectionName"),
                                                       res.get("artistName"), itunes_id=res["collectionId"])
        time.sleep(float(os.environ.get("ITUNES_PAUSE", "3")))
    return list(out.values())


def have_podcastindex_keys() -> bool:
    return bool(os.environ.get("PODCASTINDEX_KEY") and os.environ.get("PODCASTINDEX_SECRET"))


def itunes_lookup(ids: list[int], log) -> dict[int, Listing]:
    """Apple's own record for each id: the feed URL Apple has, which may differ from ours."""
    out: dict[int, Listing] = {}
    for i in range(0, len(ids), 200):
        batch = ids[i:i + 200]
        r = http.get(f"{ITUNES_BASE}/lookup?id={','.join(map(str, batch))}&entity=podcast")
        log.add("itunes", r)
        if r.status != 200:
            continue                       # logged; the show keeps what it had
        for res in json.loads(r.body).get("results", []):
            if res.get("kind") == "podcast" and res.get("feedUrl"):
                out[res["collectionId"]] = Listing("itunes", res["feedUrl"], res.get("collectionName"),
                                                   res.get("artistName"), itunes_id=res["collectionId"])
        time.sleep(float(os.environ.get("ITUNES_PAUSE", "3")))   # Apple asks for ~20 calls/min
    return out
