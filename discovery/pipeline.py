"""One discovery run: ask the catalogues what exists, decide which of our shows each listing is,
re-read every known feed (conditionally), follow moves, enrich from Apple, log everything.

Matching rule, strongest first. A listing or feed is the same show when any of these agree
and none disagree:
  podcast:guid  >  Podcast Index id  >  Apple id  >  any URL the show has ever had (url_key)
If two of them point at different shows, nothing is merged: it goes to review as `ambiguous`.
Same title and author with no shared id or URL is also `ambiguous`, never an automatic merge.
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg

from . import feeds, http, sources

log = logging.getLogger("discovery")
ACTIVE_DAYS = 90
SCHEMA = (Path(__file__).parent / "schema.sql").read_text(encoding="utf-8")


class SourceLog:
    def __init__(self):
        self.c = defaultdict(lambda: defaultdict(int))

    def add(self, source: str, r: http.Response) -> None:
        c = self.c[source]
        c["requests"] += 1
        c["ms_total"] += r.ms
        c["ok"] += r.status == 200
        c["not_modified"] += r.status == 304
        c["redirected"] += bool(r.redirects)
        c["errors"] += r.status not in (200, 304)

    def write(self, cur, run_id: int) -> None:
        for s, c in self.c.items():
            cur.execute("INSERT INTO disc.source_log VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                        (run_id, s, c["requests"], c["ok"], c["not_modified"], c["redirected"], c["errors"], c["ms_total"]))


def migrate(conn) -> None:
    conn.execute(SCHEMA)
    conn.commit()


class Run:
    def __init__(self, conn, note: str = ""):
        self.conn, self.cur, self.log = conn, conn.cursor(), SourceLog()
        self.run_id = self.cur.execute("INSERT INTO disc.run (note) VALUES (%s) RETURNING run_id", (note,)).fetchone()[0]
        self.seed = self.cur.execute("SELECT count(*) FROM disc.show").fetchone()[0] == 0
        self.started = dt.datetime.now(dt.timezone.utc)

    # ---------------------------------------------------------------- decisions
    def review(self, kind: str, show_id: int | None, **detail) -> None:
        """Queue it for a person, once: the same open question is not asked again every run."""
        d = json.dumps(detail, default=str)
        if self.cur.execute("SELECT 1 FROM disc.review WHERE NOT resolved AND kind = %s AND show_id IS NOT DISTINCT FROM %s "
                            "AND detail = %s::jsonb", (kind, show_id, d)).fetchone():
            return
        self.cur.execute("INSERT INTO disc.review (run_id, kind, show_id, detail) VALUES (%s,%s,%s,%s)",
                         (self.run_id, kind, show_id, d))

    def _owner(self, url: str) -> int | None:
        r = self.cur.execute("SELECT show_id FROM disc.feed_alias WHERE url_key = %s", (feeds.url_key(url),)).fetchone()
        return r[0] if r else None

    def alias(self, show_id: int, url: str, via: str) -> bool:
        """Attach a URL to a show. False (and a review) if the URL already belongs to another show."""
        owner = self._owner(url)
        if owner == show_id:
            return True
        if owner is not None:
            self.review("ambiguous", show_id, reason=f"{via} points at a URL another show owns", url=url, other_show=owner)
            return False
        self.cur.execute("INSERT INTO disc.feed_alias VALUES (%s,%s,%s,%s,%s)", (feeds.url_key(url), show_id, url, via, self.run_id))
        return True

    def match(self, li: sources.Listing) -> tuple[int | None, bool]:
        """(show_id, ambiguous). show_id None and not ambiguous = a new show."""
        hits = {}
        for col, val in (("guid", li.guid), ("pi_id", li.pi_id), ("itunes_id", li.itunes_id)):
            if val:
                r = self.cur.execute(f"SELECT show_id FROM disc.show WHERE {col} = %s", (val,)).fetchone()
                if r:
                    hits[col] = r[0]
        if (o := self._owner(li.feed_url)) is not None:
            hits["url"] = o
        ids = set(hits.values())
        if len(ids) > 1:
            self.review("ambiguous", None, reason="identifiers point at different shows", listing=vars(li), hits=hits)
            return None, True
        if ids:
            return ids.pop(), False
        nk, ak = feeds.name_key(li.title), feeds.name_key(li.author)
        if nk and ak:
            for sid, t, a in self.cur.execute("SELECT show_id, title, author FROM disc.show WHERE author IS NOT NULL"):
                if feeds.name_key(t) == nk and feeds.name_key(a) == ak:
                    self.review("ambiguous", sid, reason="same title and author, no shared id or URL", listing=vars(li))
                    return None, True
        return None, False

    def take(self, li: sources.Listing) -> int | None:
        """Fold one catalogue listing into the store."""
        # A listing already waiting for a person stays waiting, even if the show it looked like
        # has since been renamed. Otherwise the next run would quietly create it as new.
        if self.cur.execute("SELECT 1 FROM disc.review WHERE kind = 'ambiguous' AND NOT resolved "
                            "AND detail->'listing'->>'feed_url' = %s", (li.feed_url,)).fetchone():
            return None
        sid, ambiguous = self.match(li)
        if ambiguous:
            return None
        if sid is None:
            sid = self.cur.execute(
                "INSERT INTO disc.show (guid, itunes_id, pi_id, title, author, feed_url, first_run) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING show_id",
                (li.guid, li.itunes_id, li.pi_id, li.title, li.author, li.feed_url, self.run_id)).fetchone()[0]
            self.alias(sid, li.feed_url, "seed" if self.seed else "catalogue")
            if not self.seed:
                self.review("new_show", sid, title=li.title, feed_url=li.feed_url, source=li.source)
            return sid
        # known show: fill ids it lacked, remember the catalogue's URL as another name for it
        self.cur.execute("UPDATE disc.show SET guid = coalesce(guid, %s), itunes_id = coalesce(itunes_id, %s), "
                         "pi_id = coalesce(pi_id, %s), author = coalesce(author, %s) WHERE show_id = %s",
                         (li.guid, li.itunes_id, li.pi_id, li.author, sid))
        self.alias(sid, li.feed_url, "catalogue")
        return sid

    # ---------------------------------------------------------------- feeds
    def move(self, sid: int, old: str, new: str, via: str) -> None:
        if feeds.url_key(new) == feeds.url_key(old):
            return
        if self.alias(sid, new, via):
            self.cur.execute("UPDATE disc.show SET feed_url = %s, etag = NULL, last_modified = NULL WHERE show_id = %s", (new, sid))
            self.review("moved", sid, old=old, new=new, via=via)

    def fetch_all(self, workers: int = 8) -> None:
        rows = self.cur.execute("SELECT show_id, feed_url, etag, last_modified, guid, title, active FROM disc.show").fetchall()
        with ThreadPoolExecutor(workers) as pool:
            got = list(pool.map(lambda r: (r, http.get(r[1], r[2], r[3])), rows))
        for (sid, url, _, _, guid, title, was_active), r in got:
            self.log.add("rss", r)
            self.cur.execute("UPDATE disc.show SET last_fetch_run = %s, last_fetch_status = %s WHERE show_id = %s",
                             (self.run_id, r.status, sid))
            if r.status != 200:
                continue
            if r.permanent_url:
                self.move(sid, url, r.permanent_url, "301")
            try:
                f = feeds.parse(r.body)
            except Exception as e:  # noqa: BLE001
                self.review("ambiguous", sid, reason="feed did not parse", error=str(e)[:200], url=url)
                continue
            if f.new_feed_url:
                self.move(sid, r.permanent_url or url, f.new_feed_url, "new-feed-url")
            if f.guid and guid and f.guid != guid:
                self.review("ambiguous", sid, reason="feed guid differs from stored guid", stored=guid, feed=f.guid)
            elif f.guid and not guid:
                other = self.cur.execute("SELECT show_id FROM disc.show WHERE guid = %s", (f.guid,)).fetchone()
                if other:
                    self.review("ambiguous", sid, reason="feed guid belongs to another show", other_show=other[0])
                else:
                    self.cur.execute("UPDATE disc.show SET guid = %s WHERE show_id = %s", (f.guid, sid))
            if title and f.title and feeds.name_key(title) != feeds.name_key(f.title):
                self.review("renamed", sid, old=title, new=f.title)
            active = bool(f.last_episode_at and (self.started - f.last_episode_at).days <= ACTIVE_DAYS)
            if was_active and not active:
                self.review("went_inactive", sid, last_episode_at=f.last_episode_at)
            self.cur.execute(
                "UPDATE disc.show SET title = coalesce(%s, title), author = coalesce(author, %s), etag = %s, "
                "last_modified = %s, last_episode_at = %s, episodes = %s, active = %s WHERE show_id = %s",
                (f.title, f.author, r.etag, r.last_modified, f.last_episode_at, f.episodes, active, sid))

    def enrich_itunes(self) -> None:
        rows = self.cur.execute("SELECT show_id, itunes_id FROM disc.show WHERE itunes_id IS NOT NULL").fetchall()
        apple = sources.itunes_lookup([i for _, i in rows], self.log)
        for sid, iid in rows:
            if iid in apple:
                self.alias(sid, apple[iid].feed_url, "itunes")

    def finish(self) -> dict:
        self.log.write(self.cur, self.run_id)
        self.cur.execute("UPDATE disc.run SET finished_at = now() WHERE run_id = %s", (self.run_id,))
        self.conn.commit()
        return summary(self.conn, self.run_id)


def summary(conn, run_id: int) -> dict:
    q = lambda sql, *a: conn.execute(sql, a).fetchall()  # noqa: E731
    return {
        "run_id": run_id,
        "seconds": q("SELECT extract(epoch FROM finished_at - started_at)::numeric(10,1) FROM disc.run WHERE run_id=%s", run_id)[0][0],
        "shows": q("SELECT count(*) FROM disc.show")[0][0],
        "active": q("SELECT count(*) FROM disc.show WHERE active")[0][0],
        "with_guid": q("SELECT count(*) FROM disc.show WHERE guid IS NOT NULL")[0][0],
        "with_itunes_id": q("SELECT count(*) FROM disc.show WHERE itunes_id IS NOT NULL")[0][0],
        "shows_with_more_than_one_url": q("SELECT count(*) FROM (SELECT show_id FROM disc.feed_alias GROUP BY 1 HAVING count(*) > 1) x")[0][0],
        "aliases_by_via": dict(q("SELECT via, count(*) FROM disc.feed_alias GROUP BY 1 ORDER BY 1")),
        "review_this_run": dict(q("SELECT kind, count(*) FROM disc.review WHERE run_id=%s GROUP BY 1 ORDER BY 1", run_id)),
        "sources_this_run": {r[0]: dict(zip(("requests", "ok", "not_modified", "redirected", "errors", "ms_total"), r[1:]))
                             for r in q("SELECT source, requests, ok, not_modified, redirected, errors, ms_total "
                                        "FROM disc.source_log WHERE run_id=%s ORDER BY 1", run_id)},
    }


def run(dsn: str, listings_fn, note: str = "", itunes: bool = True, workers: int = 8) -> dict:
    with psycopg.connect(dsn) as conn:
        migrate(conn)
        r = Run(conn, note)
        for li in listings_fn(r.log):
            r.take(li)
        r.conn.commit()
        r.fetch_all(workers)
        if itunes:
            r.enrich_itunes()
        return r.finish()
