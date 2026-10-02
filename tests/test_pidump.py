"""The public-dump path on a tiny fake dump: a SQLite file shaped like Podcast Index's export
(camelCase columns, a dirty integer, a NUL byte), loaded into Postgres, then our shows matched
against it. Checks the key order, URL normalisation, conflicts, duplicates, dead and moved feeds.
Needs DISCOVERY_TEST_DSN (its `disc` and `pi` schemas are dropped)."""
from __future__ import annotations

import os
import sqlite3

import pytest

psycopg = pytest.importorskip("psycopg")
DSN = os.environ.get("DISCOVERY_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="DISCOVERY_TEST_DSN not set")

from discovery import pidump, pipeline  # noqa: E402

COLUMNS = ("id INTEGER PRIMARY KEY, url TEXT, title TEXT, dead INTEGER, itunesId INTEGER, originalUrl TEXT, "
           "newestItemPubdate INTEGER, podcastGuid TEXT, description TEXT, popularityScore INTEGER")
DUMP = [
    # id, url, title, dead, itunesId, originalUrl, newest, guid, description, popularity
    (1, "https://feeds.a.com/show", "A", 0, 111, None, 1_700_000_000, "g-a", "fine", 1),
    (2, "https://new-host.com/b.xml", "B", 0, None, "http://old.b.com/rss/", 1_700_000_000, "g-b", "", 0),
    (3, "https://c.example.org/feed", "C", 1, None, None, 1_600_000_000, None, "nul\x00here", 0),
    (4, "https://d.com/feed", "D one", 0, 444, None, 1_600_000_000, "g-d1", None, 0),
    (5, "https://d.com/feed-other", "D two", 0, None, None, 1_600_000_000, "g-d2", None, 0),
    (6, "https://e1.com/feed", "E", 1, 555, None, 1_500_000_000, None, None, 0),
    (7, "https://e2.com/feed", "E", 0, 555, None, 1_700_000_000, "g-e", None, "not a number"),
    (8, "https://f.com/feed", "Lookalike", 0, 999, None, 1_700_000_000, "g-f", None, 0),
]
OURS = [
    # show_id, itunes_id, guid, feed_url, title, extra alias
    (1, 111, "g-a", "https://feeds.a.com/show", "A", None),              # by Apple id, same URL
    (2, 222, "g-b", "https://old.b.com/rss", "B", None),                 # by guid; dump has it at a new host
    (3, 333, None, "HTTP://WWW.C.example.org/feed/?utm=x", "C", None),   # by URL only, after normalising; dead
    (4, 444, "g-d2", "https://d.com/feed", "D", None),                   # Apple id says row 4, guid says row 5
    (5, 555, "g-e", "https://e2.com/feed", "E", None),                   # Apple id on two dump rows: keep the live one
    (6, 666, None, "https://nowhere.com/x", "Lookalike", None),          # nothing shared; same title exists
    (7, 777, "g-zz", "https://gone.com/feed", "Gone", "https://gone-old.com/feed"),  # nothing at all
]


@pytest.fixture
def dump(tmp_path):
    p = tmp_path / "podcastindex_feeds.db"
    c = sqlite3.connect(p)
    c.execute(f"CREATE TABLE podcasts ({COLUMNS})")
    c.executemany("INSERT INTO podcasts VALUES (?,?,?,?,?,?,?,?,?,?)", DUMP)
    c.commit()
    c.close()
    with psycopg.connect(DSN, autocommit=True) as pg:
        pg.execute("DROP SCHEMA IF EXISTS pi CASCADE")
        pg.execute("DROP SCHEMA IF EXISTS disc CASCADE")
    return p


def seed_ours():
    with psycopg.connect(DSN) as pg:
        pipeline.migrate(pg)
        pg.execute("INSERT INTO disc.run (run_id, note) VALUES (1, 'test')")
        for sid, itunes, guid, url, title, extra in OURS:
            pg.execute("INSERT INTO disc.show (show_id, guid, itunes_id, title, feed_url, first_run) VALUES (%s,%s,%s,%s,%s,1)",
                       (sid, guid, itunes, title, url))
            for u in filter(None, (url, extra)):
                pg.execute("INSERT INTO disc.feed_alias VALUES (%s,%s,%s,'seed',1)", (pidump.url_key(u), sid, u))


def test_load_copies_every_row_and_cleans_what_postgres_rejects(dump):
    r = pidump.load(dump, DSN, batch=3, progress=None)
    assert r["sqlite_table"] == "podcasts" and r["sqlite_rows"] == r["pg_rows"] == len(DUMP) and r["rows_equal"]
    assert r["values_nulled_or_cleaned"] == {"popularity_score": 1, "description": 1}
    assert r["dead"] == 2 and r["with_itunes_id"] == 5 and r["itunes_ids_on_more_than_one_row"] == 1
    assert set(r["index_seconds"]) == {"primary key (id)", "itunes_id", "podcast_guid", "url_key", "original_url_key"}
    with psycopg.connect(DSN) as pg:
        cols = {c for (c,) in pg.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'pi'")}
        assert {"itunes_id", "podcast_guid", "original_url", "newest_item_pubdate", "url_key"} <= cols
        assert pg.execute("SELECT original_url_key FROM pi.podcasts WHERE id = 2").fetchone()[0] == "old.b.com/rss"
        assert pg.execute("SELECT description FROM pi.podcasts WHERE id = 3").fetchone()[0] == "nulhere"


def test_match_order_conflicts_and_reasons(dump):
    pidump.load(dump, DSN, progress=None)
    seed_ours()
    m = pidump.match(DSN)
    assert (m["shows"], m["matched"], m["ambiguous"], m["unmatched"]) == (7, 4, 1, 2)
    assert m["matched_by"] == {"itunes_id": 2, "podcast_guid": 1, "url": 1}
    assert m["matched_to_more_than_one_dump_row"] == 1
    assert m["dead_in_dump"] == 1                              # C; E's dead duplicate was not the one picked
    assert m["dump_url_differs_from_our_feed_url"] == 1        # B moved hosts
    assert m["of_which_dump_url_new_to_us"] == 1
    assert m["unmatched_with_same_title_in_dump"] == 1          # Lookalike: reported, never merged on title
    with psycopg.connect(DSN) as pg:
        rows = {r[0]: r[1:] for r in pg.execute("SELECT show_id, status, matched_by, pi_id FROM pi.match")}
    assert rows[1] == ("matched", "itunes_id", 1)
    assert rows[2] == ("matched", "podcast_guid", 2)
    assert rows[3] == ("matched", "url", 3)
    assert rows[4][0] == "ambiguous"
    assert rows[5] == ("matched", "itunes_id", 7)
    assert rows[6][0] == rows[7][0] == "unmatched"
    reasons = {u["show_id"]: u["reason"] for u in m["examples"]["unmatched"]}
    assert reasons[6].startswith("no podcast:guid") and "2 URL(s)" in reasons[7]


def test_url_key_is_the_one_the_discovery_run_uses():
    from discovery import feeds
    assert pidump.url_key is feeds.url_key
    assert pidump.snake("itunesId") == "itunes_id" and pidump.snake("newestItemPubdate") == "newest_item_pubdate"
    assert pidump.snake("original_url") == "original_url"
