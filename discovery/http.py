"""One GET with the two things a feed poller needs: conditional requests and a record of redirects.

A 301/308 means the publisher moved the feed for good, so the stored URL should change.
A 302/307 is temporary, so it should not. urllib follows both silently; this keeps the chain.
"""
from __future__ import annotations

import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

USER_AGENT = "media-discovery/0.1 (+https://github.com/chapse57)"
TIMEOUT = 20


@dataclass
class Response:
    url: str                       # the URL asked for
    final_url: str                 # where the body came from
    status: int                    # 200, 304, 4xx, 5xx; 0 = network error
    body: bytes = b""
    etag: str | None = None
    last_modified: str | None = None
    redirects: list[tuple[int, str]] = field(default_factory=list)   # (code, location) in order
    error: str | None = None
    ms: int = 0

    @property
    def permanent_url(self) -> str | None:
        """The last URL reached through permanent redirects only, if any."""
        url = None
        for code, loc in self.redirects:
            if code in (301, 308):
                url = loc
            else:
                break                       # a temporary hop: stop trusting the chain here
        return url


class _Recorder(urllib.request.HTTPRedirectHandler):
    def __init__(self):
        self.hops: list[tuple[int, str]] = []

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        self.hops.append((code, newurl))
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def get(url: str, etag: str | None = None, last_modified: str | None = None,
        headers: dict[str, str] | None = None) -> Response:
    rec = _Recorder()
    opener = urllib.request.build_opener(rec)
    h = {"User-Agent": USER_AGENT, **(headers or {})}
    if etag:
        h["If-None-Match"] = etag
    if last_modified:
        h["If-Modified-Since"] = last_modified
    t = time.perf_counter()
    try:
        with opener.open(urllib.request.Request(url, headers=h), timeout=TIMEOUT) as r:
            body = r.read()
            return Response(url, r.geturl(), r.status, body, r.headers.get("ETag"), r.headers.get("Last-Modified"),
                            rec.hops, ms=int((time.perf_counter() - t) * 1000))
    except urllib.error.HTTPError as e:
        final = rec.hops[-1][1] if rec.hops else url
        return Response(url, final, e.code, b"", e.headers.get("ETag") if e.headers else None,
                        e.headers.get("Last-Modified") if e.headers else None, rec.hops,
                        None if e.code == 304 else f"HTTP {e.code}", int((time.perf_counter() - t) * 1000))
    except Exception as e:  # noqa: BLE001 - a feed host can fail in any way; record it and move on
        return Response(url, url, 0, redirects=rec.hops, error=f"{type(e).__name__}: {e}"[:300],
                        ms=int((time.perf_counter() - t) * 1000))
