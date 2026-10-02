"""The discovery run against a local fake: RSS hosts, Podcast Index and iTunes Lookup on one
loopback server, so every rule is exercised with real HTTP (304s, 301s, 302s) and a real
Postgres. Needs DISCOVERY_TEST_DSN (a database whose `disc` schema may be dropped)."""
from __future__ import annotations

import datetime as dt
import email.utils
import hashlib
import json
import os
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

psycopg = pytest.importorskip("psycopg")
DSN = os.environ.get("DISCOVERY_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="DISCOVERY_TEST_DSN not set")

from discovery import pipeline, sources  # noqa: E402

KEY, SECRET = "testkey", "testsecret"
NOW = dt.datetime.now(dt.timezone.utc)


def rss(title, guid=None, author="Host", new_url=None, days_ago=3):
    pub = email.utils.format_datetime(NOW - dt.timedelta(days=days_ago))
    return (f'<?xml version="1.0"?><rss xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" '
            f'xmlns:podcast="https://podcastindex.org/namespace/1.0"><channel><title>{title}</title>'
            f'<itunes:author>{author}</itunes:author>'
            + (f"<podcast:guid>{guid}</podcast:guid>" if guid else "")
            + (f"<itunes:new-feed-url>{new_url}</itunes:new-feed-url>" if new_url else "")
            + f"<item><title>ep</title><pubDate>{pub}</pubDate></item></channel></rss>").encode()


class Fake:
    """Routes: path -> ("feed", body, etag) | ("redirect", code, location)."""

    def __init__(self):
        self.routes, self.trending, self.apple, self.auth_ok, self.hits = {}, [], {}, [], []
        self.search = []
        fake = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                u = urllib.parse.urlparse(self.path)
                fake.hits.append(u.path)
                if u.path.startswith("/pi/"):
                    d, k = self.headers.get("X-Auth-Date"), self.headers.get("X-Auth-Key")
                    fake.auth_ok.append(k == KEY and self.headers.get("Authorization")
                                        == hashlib.sha1((KEY + SECRET + d).encode()).hexdigest())
                    return self.json({"feeds": fake.trending})
                if u.path == "/itunes/search":
                    return self.json({"results": fake.search})
                if u.path == "/itunes/lookup":
                    ids = [int(i) for i in urllib.parse.parse_qs(u.query)["id"][0].split(",")]
                    return self.json({"results": [fake.apple[i] for i in ids if i in fake.apple]})
                route = fake.routes.get(u.path)
                if route is None:
                    self.send_response(404); self.end_headers(); return
                if route[0] == "redirect":
                    self.send_response(route[1]); self.send_header("Location", route[2]); self.end_headers(); return
                _, body, etag = route
                if etag and self.headers.get("If-None-Match") == etag:
                    self.send_response(304); self.end_headers(); return
                self.send_response(200)
                if etag:
                    self.send_header("ETag", etag)
                self.end_headers(); self.wfile.write(body)

            def json(self, obj):
                b = json.dumps(obj).encode()
                self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers()
                self.wfile.write(b)

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.base = f"http://127.0.0.1:{self.srv.server_port}"
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def url(self, path):
        return self.base + path

    def listing(self, path, title, guid=None, itunes=None, pi=None, author="Host"):
        return {"url": self.url(path), "title": title, "author": author, "podcastGuid": guid or "", "itunesId": itunes, "id": pi}


@pytest.fixture
def fake(monkeypatch):
    f = Fake()
    monkeypatch.setattr(sources, "PI_BASE", f.url("/pi"))
    monkeypatch.setattr(sources, "ITUNES_BASE", f.url("/itunes"))
    monkeypatch.setenv("PODCASTINDEX_KEY", KEY)
    monkeypatch.setenv("PODCASTINDEX_SECRET", SECRET)
    monkeypatch.setenv("ITUNES_PAUSE", "0")
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute("DROP SCHEMA IF EXISTS disc CASCADE")
    yield f
    f.srv.shutdown()


def go(note=""):
    return pipeline.run(DSN, lambda log: sources.trending(50, log), note)


def q(sql, *a):
    with psycopg.connect(DSN) as c:
        return c.execute(sql, a).fetchall()


def seed(f):
    f.routes.update({
        "/a": ("feed", rss("Alpha Show", "guid-a"), '"a1"'),
        "/b": ("redirect", 301, f.url("/b-new")),                       # host moved for good
        "/b-new": ("feed", rss("Bravo", "guid-b"), None),
        "/c": ("feed", rss("Charlie", "guid-c", new_url=f.url("/c-moved")), None),
        "/c-moved": ("feed", rss("Charlie", "guid-c"), None),
        "/d": ("feed", rss("Delta", "guid-d", days_ago=200), None),     # last episode 200 days ago
        "/t": ("redirect", 302, f.url("/t-tmp")),                       # temporary: keep /t
        "/t-tmp": ("feed", rss("Tango", "guid-t"), None),
    })
    f.trending = [f.listing("/a", "Alpha Show", "guid-a", itunes=111, pi=1), f.listing("/b", "Bravo", "guid-b", pi=2),
                  f.listing("/c", "Charlie", "guid-c", pi=3), f.listing("/d", "Delta", "guid-d", pi=4),
                  f.listing("/t", "Tango", "guid-t", pi=5)]
    f.apple = {111: {"kind": "podcast", "collectionId": 111, "collectionName": "Alpha Show",
                     "feedUrl": f.url("/a-apple-has-this")}}


def test_seed_run_follows_moves_and_judges_activity(fake):
    seed(fake)
    s = go("seed")
    assert s["shows"] == 5 and s["review_this_run"].get("new_show") is None      # the seed is not "new"
    url = dict(q("SELECT title, feed_url FROM disc.show"))
    assert url["Bravo"] == fake.url("/b-new")            # 301 followed and kept
    assert url["Charlie"] == fake.url("/c-moved")        # itunes:new-feed-url followed
    assert url["Tango"] == fake.url("/t")                # 302 is temporary: not kept
    assert dict(q("SELECT title, active FROM disc.show")) == {
        "Alpha Show": True, "Bravo": True, "Charlie": True, "Delta": False, "Tango": True}
    assert s["aliases_by_via"] == {"301": 1, "itunes": 1, "new-feed-url": 1, "seed": 5}
    assert s["review_this_run"] == {"moved": 2}
    assert all(fake.auth_ok) and fake.auth_ok        # Podcast Index auth header is the sha1 it should be


def test_rerun_adds_nothing_and_uses_conditional_get(fake):
    seed(fake)
    go("seed")
    s = go("again")
    assert s["shows"] == 5
    assert s["review_this_run"] == {}                    # no new shows, no repeated moves
    assert s["sources_this_run"]["rss"]["not_modified"] >= 1           # /a answered 304 to its ETag
    assert q("SELECT count(*) FROM disc.feed_alias")[0][0] == 8         # no alias added twice


def test_new_show_rebrand_and_lookalike(fake):
    seed(fake)
    go("seed")
    fake.routes.update({
        "/e": ("feed", rss("Echo", "guid-e"), None),
        "/b-rebrand": ("feed", rss("Bravo Daily", "guid-b"), None),
        "/a2": ("feed", rss("Alpha Show", "guid-other"), None),
        "/a": ("feed", rss("Alpha Show Weekly", "guid-a"), '"a2"'),     # same feed, retitled
    })
    fake.trending += [fake.listing("/e", "Echo", "guid-e", pi=6),
                      fake.listing("/b-rebrand", "Bravo Daily", "guid-b", pi=2),          # same guid, new URL and name
                      fake.listing("/a2", "Alpha Show", "guid-other", pi=99)]             # same title+author, nothing shared
    s = go("third")
    assert s["shows"] == 6                                                    # only Echo is new
    assert s["review_this_run"] == {"ambiguous": 1, "new_show": 1, "renamed": 1}
    bravo = q("SELECT show_id FROM disc.show WHERE guid = 'guid-b'")[0][0]
    assert q("SELECT via FROM disc.feed_alias WHERE url = %s", fake.url("/b-rebrand"))[0][0] == "catalogue"
    assert q("SELECT show_id FROM disc.feed_alias WHERE url = %s", fake.url("/b-rebrand"))[0][0] == bravo
    assert q("SELECT count(*) FROM disc.show WHERE guid = 'guid-other'")[0][0] == 0      # not merged, not created
    assert go("fourth")["review_this_run"] == {}                              # the open questions are not re-asked


def test_conflicting_identifiers_are_never_merged(fake):
    seed(fake)
    go("seed")
    # a listing whose guid is Alpha's but whose URL is Delta's
    fake.trending.append(fake.listing("/d", "Mixed", "guid-a", pi=77))
    s = go("conflict")
    assert s["shows"] == 5
    assert s["review_this_run"] == {"ambiguous": 1}
    assert q("SELECT pi_id FROM disc.show WHERE guid = 'guid-a'")[0][0] == 1          # nothing overwritten


def test_seed_from_apple_search_without_a_podcastindex_key(fake, monkeypatch):
    monkeypatch.delenv("PODCASTINDEX_KEY")
    seed(fake)
    fake.search = [{"kind": "podcast", "collectionId": 111, "collectionName": "Alpha Show", "artistName": "Host",
                    "feedUrl": fake.url("/a")},
                   {"kind": "podcast", "collectionId": 222, "collectionName": "Bravo", "artistName": "Host",
                    "feedUrl": fake.url("/b")}]
    assert not sources.have_podcastindex_keys()
    s = pipeline.run(DSN, lambda log: sources.itunes_search(10, log, terms=("news", "tech")), "apple")
    assert s["shows"] == 2                                   # the same two shows under both terms: not doubled
    assert not any(h.startswith("/pi/") for h in fake.hits)  # no Podcast Index call at all
    assert dict(q("SELECT title, guid FROM disc.show")) == {"Alpha Show": "guid-a", "Bravo": "guid-b"}  # guid from RSS
    assert dict(q("SELECT title, feed_url FROM disc.show"))["Bravo"] == fake.url("/b-new")
