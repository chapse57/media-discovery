"""Podcast Index's public database dump (one SQLite file, no key, no signup) into Postgres,
and a match of our own shows against it.

    python -m discovery.pidump load  --db data\\podcastindex_feeds.db   > results\\pidump-load.json
    python -m discovery.pidump match                                    > results\\pidump-match.json

`load` copies the dump's feed table into schema `pi` (dropped and rebuilt each time) with
COPY, then adds the primary key and lookup indexes, timing each step separately. Every column
of the dump is kept; two are added: `url_key` and `original_url_key`, computed in Python with
the same `feeds.url_key` the discovery run uses, so a URL normalises identically on both sides.

`match` looks every show in `disc.show` up in `pi.podcasts`, by Apple id, then podcast:guid,
then any URL the show has ever had. Keys that point at different dump rows are not resolved
by priority: the show is reported as ambiguous, the same rule the discovery run follows.
Nothing in `disc` is changed; the per-show result goes to `pi.match`.

Source: https://public.podcastindex.org/podcastindex_feeds.db.tgz (github.com/Podcastindex-org/database).
DSN from DISCOVERY_DSN.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import re
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import psycopg
from psycopg import sql

from .feeds import url_key

PREFERRED_TABLES = ("podcasts", "newsfeeds")
NEEDED = ("id", "url", "itunes_id", "podcast_guid", "original_url", "newest_item_pubdate", "dead")
INDEXES = (("primary key (id)", "ALTER TABLE pi.podcasts ADD PRIMARY KEY (id)"),
           ("itunes_id", "CREATE INDEX ON pi.podcasts (itunes_id)"),
           ("podcast_guid", "CREATE INDEX ON pi.podcasts (podcast_guid)"),
           ("url_key", "CREATE INDEX ON pi.podcasts (url_key)"),
           ("original_url_key", "CREATE INDEX ON pi.podcasts (original_url_key)"))


def snake(name: str) -> str:
    """itunesId -> itunes_id, newestItemPubdate -> newest_item_pubdate, original_url stays."""
    return re.sub(r"(?<=[a-z0-9])([A-Z])", r"_\1", name).lower()


def pick_table(lite: sqlite3.Connection) -> str:
    tables = [r[0] for r in lite.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
    for t in PREFERRED_TABLES:
        if t in tables:
            return t
    for t in tables:
        if "url" in [c[1].lower() for c in lite.execute(f'PRAGMA table_info("{t}")')]:
            return t
    raise SystemExit(f"no feed table in the dump (tables: {tables})")


def pg_type(declared: str) -> str:
    d = (declared or "").upper()
    if "INT" in d:
        return "bigint"
    if any(k in d for k in ("REAL", "FLOA", "DOUB")):
        return "double precision"
    return "text"


class Converter:
    """Per-column cleaning. SQLite keeps whatever was written; Postgres COPY does not.
    A value that cannot be the declared type becomes NULL and is counted, never guessed."""

    def __init__(self, names: list[str], types: list[str]):
        self.names, self.coerced = names, Counter()
        self.fns = [getattr(self, "_" + t.split()[0]) for t in types]

    def row(self, values) -> list:
        return [f(v, n) for f, v, n in zip(self.fns, values, self.names)]

    def _bigint(self, v, name):
        if v is None or isinstance(v, int):
            return v
        s = str(v).strip()
        if s == "":
            return None
        try:
            return int(float(s)) if "." in s or "e" in s.lower() else int(s)
        except ValueError:
            self.coerced[name] += 1
            return None

    def _double(self, v, name):
        if v is None or isinstance(v, (int, float)):
            return v
        try:
            return float(v)
        except (TypeError, ValueError):
            self.coerced[name] += 1
            return None

    def _text(self, v, name):
        if v is None:
            return None
        if isinstance(v, bytes):
            v = v.decode("utf-8", "replace")
        elif not isinstance(v, str):
            v = str(v)
        if "\x00" in v:                       # Postgres text cannot hold NUL
            self.coerced[name] += 1
            v = v.replace("\x00", "")
        return v


def machine() -> dict:
    ram = None
    try:
        if sys.platform == "win32":
            import ctypes

            class MS(ctypes.Structure):
                _fields_ = [("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                            ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                            ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                            ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                            ("sullAvailExtendedVirtual", ctypes.c_ulonglong)]
            m = MS(); m.dwLength = ctypes.sizeof(MS)
            ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m))
            ram = m.ullTotalPhys
        else:
            ram = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except Exception:  # noqa: BLE001 - spec is informational
        pass
    return {"os": platform.platform(), "cpu": platform.processor() or platform.machine(),
            "logical_cpus": os.cpu_count(), "ram_gb": round(ram / 2**30, 1) if ram else None,
            "python": platform.python_version()}


def load(db: Path, dsn: str, batch: int = 20_000, progress=sys.stderr) -> dict:
    lite = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    lite.text_factory = lambda b: b.decode("utf-8", "replace")
    table = pick_table(lite)
    info = list(lite.execute(f'PRAGMA table_info("{table}")'))
    src_cols = [c[1] for c in info]
    names = [snake(c) for c in src_cols]
    types = [pg_type(c[2]) for c in info]
    missing = [n for n in NEEDED if n not in names]
    if missing:
        raise SystemExit(f"{table}: expected columns not found: {missing} (have {names})")
    t0 = time.perf_counter()
    sqlite_rows = lite.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
    count_s = time.perf_counter() - t0
    conv = Converter(names, types)
    i_url, i_orig = names.index("url"), names.index("original_url")

    with psycopg.connect(dsn) as pg:
        pg.execute("DROP SCHEMA IF EXISTS pi CASCADE")
        pg.execute("CREATE SCHEMA pi")
        cols = [sql.SQL("{} {}").format(sql.Identifier(n), sql.SQL(t)) for n, t in zip(names, types)]
        pg.execute(sql.SQL("CREATE TABLE pi.podcasts ({}, url_key text, original_url_key text)").format(sql.SQL(", ").join(cols)))
        copy_cols = sql.SQL(", ").join(map(sql.Identifier, names + ["url_key", "original_url_key"]))
        t0 = time.perf_counter()
        done = 0
        cur = lite.execute(f'SELECT {", ".join(chr(34) + c + chr(34) for c in src_cols)} FROM "{table}"')
        with pg.cursor() as pc, pc.copy(sql.SQL("COPY pi.podcasts ({}) FROM STDIN").format(copy_cols)) as cp:
            while rows := cur.fetchmany(batch):
                for r in rows:
                    v = conv.row(r)
                    v.append(url_key(v[i_url]) if v[i_url] else None)
                    v.append(url_key(v[i_orig]) if v[i_orig] else None)
                    cp.write_row(v)
                done += len(rows)
                if progress and done % 500_000 < batch:
                    print(f"  {done:,} rows, {time.perf_counter() - t0:.0f}s", file=progress, flush=True)
        pg.commit()
        copy_s = time.perf_counter() - t0
        index_s = {}
        for label, stmt in INDEXES:
            t = time.perf_counter()
            pg.execute(stmt)
            pg.commit()
            index_s[label] = round(time.perf_counter() - t, 1)
        t = time.perf_counter()
        pg.execute("ANALYZE pi.podcasts")
        pg.commit()
        analyze_s = round(time.perf_counter() - t, 1)
        q = lambda s: pg.execute(s).fetchone()[0]  # noqa: E731
        stats = {
            "pg_rows": q("SELECT count(*) FROM pi.podcasts"),
            "dead": q("SELECT count(*) FROM pi.podcasts WHERE dead = 1"),
            "with_itunes_id": q("SELECT count(*) FROM pi.podcasts WHERE itunes_id > 0"),
            "with_podcast_guid": q("SELECT count(*) FROM pi.podcasts WHERE coalesce(podcast_guid, '') <> ''"),
            "itunes_ids_on_more_than_one_row": q("SELECT count(*) FROM (SELECT itunes_id FROM pi.podcasts WHERE itunes_id > 0 "
                                                 "GROUP BY 1 HAVING count(*) > 1) d"),
            "url_keys_on_more_than_one_row": q("SELECT count(*) FROM (SELECT url_key FROM pi.podcasts WHERE url_key IS NOT NULL "
                                               "GROUP BY 1 HAVING count(*) > 1) d"),
            "marked_duplicate_of_another_row": q("SELECT count(*) FROM pi.podcasts WHERE duplicate_of > 0") if "duplicate_of" in names else None,
            "newest_item": q("SELECT to_timestamp(max(newest_item_pubdate))::date::text FROM pi.podcasts "
                             "WHERE newest_item_pubdate < extract(epoch FROM now()) + 86400"),
            "table_size": q("SELECT pg_size_pretty(pg_total_relation_size('pi.podcasts'))"),
            "postgres": q("SELECT version()"),
        }
    return {"source_file": str(db), "source_bytes": db.stat().st_size, "sqlite_table": table,
            "sqlite_rows": sqlite_rows, "sqlite_count_seconds": round(count_s, 1), "columns": len(names),
            "copy_seconds": round(copy_s, 1), "rows_per_second": round(sqlite_rows / copy_s) if copy_s else None,
            "index_seconds": index_s, "index_seconds_total": round(sum(index_s.values()), 1),
            "analyze_seconds": analyze_s, "values_nulled_or_cleaned": dict(conv.coerced),
            **stats, "rows_equal": stats["pg_rows"] == sqlite_rows, "machine": machine()}


# ------------------------------------------------------------------------------------- match
ORDER = ("itunes_id", "podcast_guid", "url")


def _pick(rows: list[tuple]) -> tuple:
    """Several dump rows for one key (Podcast Index has duplicates): one the dump does not itself mark
    as a duplicate, then a live one, then the newest episode."""
    return sorted(rows, key=lambda r: ((r[6] or 0) > 0, r[3] == 1, -(r[4] or 0), r[0]))[0]


def match(dsn: str, examples: int = 10) -> dict:
    with psycopg.connect(dsn) as pg:
        shows = pg.execute("SELECT show_id, itunes_id, guid, feed_url, title FROM disc.show ORDER BY show_id").fetchall()
        known = defaultdict(set)
        for sid, k in pg.execute("SELECT show_id, url_key FROM disc.feed_alias"):
            known[sid].add(k)
        pg.execute("CREATE TABLE IF NOT EXISTS pi.match (show_id int PRIMARY KEY, status text NOT NULL, matched_by text, "
                   "pi_id bigint, dump_rows int, dead boolean, dump_url text, reason text)")
        pg.execute("TRUNCATE pi.match")
        has_dup = pg.execute("SELECT 1 FROM information_schema.columns WHERE table_schema = 'pi' "
                             "AND table_name = 'podcasts' AND column_name = 'duplicate_of'").fetchone()
        cols = "id, url, url_key, dead, newest_item_pubdate, original_url_key, " + ("duplicate_of" if has_dup else "NULL")
        out = Counter()
        by_key, moved, dead, dupes, ambiguous, unmatched = Counter(), [], [], 0, [], []
        unmatched_ids = []
        for sid, itunes, guid, feed_url, title in shows:
            keys = known[sid] | {url_key(feed_url)}
            hits = {}
            if itunes:
                hits["itunes_id"] = pg.execute(f"SELECT {cols} FROM pi.podcasts WHERE itunes_id = %s", (itunes,)).fetchall()
            if guid:
                hits["podcast_guid"] = pg.execute(f"SELECT {cols} FROM pi.podcasts WHERE podcast_guid = %s", (guid,)).fetchall()
            hits["url"] = pg.execute(f"SELECT {cols} FROM pi.podcasts WHERE url_key = ANY(%s) OR original_url_key = ANY(%s)",
                                     (list(keys), list(keys))).fetchall()
            found = {k: {r[0] for r in v} for k, v in hits.items() if v}
            if not found:
                tried = ["Apple id"] + (["podcast:guid"] if guid else []) + [f"{len(keys)} URL(s)"]
                reason = ("no podcast:guid in its feed; " if not guid else "") + "none of " + ", ".join(tried) + " in the dump"
                unmatched.append({"show_id": sid, "title": title, "reason": reason})
                unmatched_ids.append(sid)
                pg.execute("INSERT INTO pi.match (show_id, status, reason) VALUES (%s,'unmatched',%s)", (sid, reason))
                out["unmatched"] += 1
                continue
            sets = list(found.values())
            common = set.intersection(*sets)
            if not common:                       # keys disagree: same rule as the discovery run, no merge
                detail = {k: sorted(v)[:3] for k, v in found.items()}
                ambiguous.append({"show_id": sid, "title": title, "hits": detail})
                pg.execute("INSERT INTO pi.match (show_id, status, reason) VALUES (%s,'ambiguous',%s)",
                           (sid, json.dumps(detail)))
                out["ambiguous"] += 1
                continue
            first = next(k for k in ORDER if k in found)
            rows = [r for r in hits[first] if r[0] in common]
            r = _pick(rows)
            n_rows = len(found[first])
            dupes += n_rows > 1
            by_key[first] += 1
            out["matched"] += 1
            is_dead = r[3] == 1
            if is_dead:
                dead.append({"show_id": sid, "title": title, "pi_id": r[0]})
            if r[2] != url_key(feed_url):
                moved.append({"show_id": sid, "title": title, "ours": feed_url, "dump": r[1],
                              "dump_url_already_known_to_us": r[2] in keys})
            pg.execute("INSERT INTO pi.match VALUES (%s,'matched',%s,%s,%s,%s,%s,NULL)",
                       (sid, first, r[0], n_rows, is_dead, r[1]))
        # unmatched: does the dump have a feed with the same title? (a near miss we would never merge on title alone)
        same_title = 0
        if unmatched:
            titles = [(u["title"] or "").strip().lower() for u in unmatched]
            have = {t for (t,) in pg.execute("SELECT DISTINCT lower(btrim(title)) FROM pi.podcasts WHERE lower(btrim(title)) = ANY(%s)",
                                             (titles,))}
            for u, t in zip(unmatched, titles):
                u["same_title_in_dump"] = t in have
                same_title += t in have
        pg.commit()
    return {"shows": len(shows), "matched": out["matched"], "matched_by": {k: by_key[k] for k in ORDER},
            "ambiguous": out["ambiguous"], "unmatched": out["unmatched"],
            "matched_to_more_than_one_dump_row": dupes,
            "dump_url_differs_from_our_feed_url": len(moved),
            "of_which_dump_url_new_to_us": sum(not m["dump_url_already_known_to_us"] for m in moved),
            "dead_in_dump": len(dead),
            "unmatched_with_same_title_in_dump": same_title,
            "examples": {"moved": moved[:examples], "dead": dead[:examples], "ambiguous": ambiguous[:examples],
                         "unmatched": unmatched[:examples]}}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="discovery.pidump")
    ap.add_argument("--dsn", default=os.environ.get("DISCOVERY_DSN", "postgresql://postgres:pw@localhost:5432/discovery"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    lo = sub.add_parser("load")
    lo.add_argument("--db", type=Path, required=True)
    sub.add_parser("match")
    a = ap.parse_args(argv)
    res = load(a.db, a.dsn) if a.cmd == "load" else match(a.dsn)
    # Windows sends redirected output in the console code page (cp949 here), which cannot hold every
    # podcast title. The JSON file is UTF-8 whatever the console is.
    sys.stdout.reconfigure(encoding="utf-8")
    print(json.dumps(res, indent=2, ensure_ascii=False, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
