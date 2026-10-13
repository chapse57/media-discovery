"""The consumer side of discovery/cdc.py, without Debezium or a real Kafka broker.

A trigger on src.podcasts writes each change, shaped like a Debezium event (op, before, after,
source.ts_us, source.snapshot; a tombstone after each delete), to an outbox table. The tests feed
those events to the sink directly, and in the last test through librdkafka's built-in mock broker
to a consumer process that is SIGKILLed mid-stream. What this proves: idempotent apply, counted
redeliveries, resume from committed offsets, a schema change stopping the consumer without writes,
and the source/sink comparison. What it does not prove: Debezium's own behaviour (that is the live
run, results/2026-10-14-cdc.json). Needs DISCOVERY_TEST_DSN (schemas src, stage, sink, fake dropped).
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")
DSN = os.environ.get("DISCOVERY_TEST_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="DISCOVERY_TEST_DSN not set")

from discovery import cdc  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
TOPIC = cdc.TOPIC

DUMP_COLS = ("id INTEGER PRIMARY KEY, url TEXT, title TEXT, itunesId INTEGER, podcastGuid TEXT, language TEXT, "
             "episodeCount INTEGER, popularityScore INTEGER, dead INTEGER, newestItemPubdate INTEGER, host TEXT, "
             "category1 TEXT, lastUpdate INTEGER, description TEXT")

FAKE_SOURCE = """
DROP SCHEMA IF EXISTS fake CASCADE;
CREATE SCHEMA fake;
CREATE TABLE fake.outbox (seq bigserial PRIMARY KEY, op text, key jsonb, before jsonb, after jsonb,
                          ts_us bigint, snapshot text);
CREATE FUNCTION fake.capture() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    INSERT INTO fake.outbox (op, key, before, after, ts_us)
    VALUES ('d', jsonb_build_object('id', OLD.id), jsonb_build_object('id', OLD.id), NULL,
            (extract(epoch FROM clock_timestamp()) * 1e6)::bigint);
    INSERT INTO fake.outbox (op, key) VALUES ('tombstone', jsonb_build_object('id', OLD.id));
    RETURN OLD;
  END IF;
  INSERT INTO fake.outbox (op, key, before, after, ts_us)
  VALUES (CASE TG_OP WHEN 'INSERT' THEN 'c' ELSE 'u' END, jsonb_build_object('id', NEW.id),
          CASE WHEN TG_OP = 'UPDATE' THEN jsonb_build_object('id', OLD.id) END, to_jsonb(NEW),
          (extract(epoch FROM clock_timestamp()) * 1e6)::bigint);
  RETURN NEW;
END $$;
CREATE TRIGGER capture AFTER INSERT OR UPDATE OR DELETE ON src.podcasts FOR EACH ROW EXECUTE FUNCTION fake.capture();
"""


class Msg:
    """Stands in for confluent_kafka.Message."""

    def __init__(self, part, off, key, value):
        self._p, self._o, self._k, self._v = part, off, key, value

    def topic(self):
        return TOPIC

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def key(self):
        return self._k

    def value(self):
        return self._v

    def error(self):
        return None


def envelope(row) -> tuple[bytes, bytes | None]:
    """One outbox row -> (key, value) bytes as Debezium's JsonConverter (schemas off) would send."""
    _seq, op, key, before, after, ts_us, snapshot = row
    k = json.dumps(key).encode()
    if op == "tombstone":
        return k, None
    source = {"connector": "fake", "ts_us": ts_us, "ts_ms": ts_us // 1000 if ts_us else None, "snapshot": snapshot or "false"}
    return k, json.dumps({"before": before, "after": after, "source": source, "op": op}).encode()


def install_fake_source(pg) -> None:
    pg.execute(FAKE_SOURCE)


def snapshot_to_outbox(pg) -> int:
    """Like Debezium's initial snapshot: every row as op 'r', the last one marked snapshot = 'last'."""
    pg.execute("INSERT INTO fake.outbox (op, key, after, ts_us, snapshot) "
               "SELECT 'r', jsonb_build_object('id', id), to_jsonb(p), (extract(epoch FROM clock_timestamp()) * 1e6)::bigint, "
               "CASE WHEN id = max(id) OVER () THEN 'last' ELSE 'true' END FROM src.podcasts p ORDER BY id")
    return pg.execute("SELECT count(*) FROM fake.outbox").fetchone()[0]


def outbox(pg, after_seq: int = 0) -> list:
    return pg.execute("SELECT seq, op, key, before, after, ts_us, snapshot FROM fake.outbox WHERE seq > %s ORDER BY seq",
                      (after_seq,)).fetchall()


def as_messages(rows, start_off: int = 0) -> list[Msg]:
    return [Msg(0, start_off + i, *envelope(r)) for i, r in enumerate(rows)]


def make_dump(path: Path, n: int) -> Path:
    c = sqlite3.connect(path)
    c.execute(f"CREATE TABLE podcasts ({DUMP_COLS})")
    rows = []
    for i in range(1, n + 1):
        rows.append((i, f"https://feeds.example/{i}", f"Show {i}", 1000 + i, f"g-{i}", "en", i % 50, i % 10, i % 7 == 0,
                     1_700_000_000 + i, "example", "news", 1_700_000_000 + i, "unused"))
    rows[1] = rows[1][:3] + ("not a number",) + rows[1][4:]          # dirty integer -> NULL, counted
    rows[2] = rows[2][:2] + ("nul\x00title",) + rows[2][3:]           # NUL byte -> removed, counted
    rows[3] = rows[3][:12] + (99_999_999_999,) + rows[3][13:]         # timestamp past 2100 -> NULL, counted
    c.executemany(f"INSERT INTO podcasts VALUES ({','.join('?' * 14)})", rows)
    c.commit()
    c.close()
    return path


@pytest.fixture
def pg(tmp_path):
    with psycopg.connect(DSN, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS fake CASCADE")
        res = cdc.seed(DSN, make_dump(tmp_path / "dump.db", 40), rows=30, reserve=10)
        assert res["rows"] == 30 and res["reserve_rows"] == 10
        assert res["values_nulled_or_cleaned"] == {"itunes_id": 1, "title": 1, "updated_at": 1}
        install_fake_source(conn)
        yield conn


def test_seed_takes_lowest_ids_and_keeps_reserve_apart(pg):
    assert pg.execute("SELECT min(id), max(id) FROM src.podcasts").fetchone() == (1, 30)
    assert pg.execute("SELECT min(id), max(id) FROM stage.reserve").fetchone() == (31, 40)
    assert pg.execute("SELECT title FROM src.podcasts WHERE id = 3").fetchone()[0] == "nultitle"
    assert pg.execute("SELECT itunes_id, updated_at FROM src.podcasts WHERE id = 2").fetchone()[0] is None
    assert pg.execute("SELECT updated_at FROM src.podcasts WHERE id = 4").fetchone()[0] is None
    assert pg.execute("SELECT count(*) FROM sink.podcasts").fetchone()[0] == 0


def test_snapshot_applied_twice_is_counted_not_doubled(pg):
    snapshot_to_outbox(pg)
    msgs = as_messages(outbox(pg))
    sink = cdc.Sink(pg, "g", "snapshot")
    nxt = sink.apply(msgs)
    assert nxt == {(TOPIC, 0): 30} and sink.snapshot_done
    assert cdc.compare(pg)["equal"]
    sink.apply(msgs)                                     # same messages again, as after a crash
    cmp = cdc.compare(pg)
    assert cmp["equal"] and cmp["sink_rows"] == 30
    assert pg.execute("SELECT count(*), min(times), max(times) FROM sink.applied WHERE grp = 'g'").fetchone() == (30, 2, 2)


def test_inserts_updates_deletes_end_equal_to_source(pg):
    snapshot_to_outbox(pg)
    gen = cdc.changes(DSN, 120, rate=0, seed=5)
    assert gen["insert"] + gen["update"] + gen["delete"] == 120 and gen["delete"] > 0 and gen["insert"] > 0
    rows = outbox(pg)
    sink = cdc.Sink(pg, "g", "stream")
    for i in range(0, len(rows), 17):                    # several batches, cut anywhere
        sink.apply(as_messages(rows[i:i + 17], start_off=i))
    cmp = cdc.compare(pg)
    assert cmp["equal"], cmp
    ops = dict(pg.execute("SELECT op, count(*) FROM sink.applied GROUP BY op").fetchall())
    assert ops["d"] == ops["tombstone"] == gen["delete"]
    lat = cdc.latency(pg, "g", "stream")
    assert lat["changes"] == gen["insert"] + gen["update"] + gen["delete"] and lat["p50_ms"] >= 0


def test_compare_names_each_kind_of_difference(pg):
    pg.execute("INSERT INTO sink.podcasts SELECT * FROM src.podcasts")
    assert cdc.compare(pg)["equal"]
    pg.execute("DELETE FROM sink.podcasts WHERE id = 1")
    pg.execute("UPDATE sink.podcasts SET title = 'changed' WHERE id = 2")
    pg.execute("UPDATE sink.podcasts SET updated_at = updated_at + interval '1 microsecond' WHERE id = 5")
    pg.execute("INSERT INTO sink.podcasts (id, url) VALUES (999, 'x')")
    cmp = cdc.compare(pg)
    assert (cmp["missing_in_sink"], cmp["extra_in_sink"], cmp["different"], cmp["equal"]) == (1, 1, 2, False)


def test_new_column_stops_the_batch_before_any_write(pg):
    snapshot_to_outbox(pg)
    sink = cdc.Sink(pg, "g", "snapshot")
    sink.apply(as_messages(outbox(pg)))
    seen = pg.execute("SELECT max(seq) FROM fake.outbox").fetchone()[0]
    pg.execute("ALTER TABLE src.podcasts ADD COLUMN region text")
    cdc.changes(DSN, 5, rate=0, seed=1, mix=(0, 1, 0), extra_set={"region": ["KR"]})
    msgs = as_messages(outbox(pg, seen), start_off=seen)
    with pytest.raises(cdc.SchemaDrift) as e:
        cdc.Sink(pg, "g", "schema").apply(msgs)
    assert e.value.extra == ["region"] and e.value.where["offset"] == seen
    assert pg.execute("SELECT count(*) FROM sink.applied WHERE phase = 'schema'").fetchone()[0] == 0
    behind = cdc.compare(pg)
    touched = {r[2]["id"] for r in outbox(pg, seen) if r[1] == "u"}
    assert behind["columns_only_in_source"] == ["region"] and behind["different"] == len(touched) > 0
    pg.execute("ALTER TABLE sink.podcasts ADD COLUMN region text")
    cdc.Sink(pg, "g", "schema-fixed").apply(msgs)
    cmp = cdc.compare(pg)
    assert cmp["equal"] and cmp["columns_compared"] == 14


# ---------------------------------------------------------------------------- through a broker --
kafka = pytest.importorskip("confluent_kafka")


def _mock_broker():
    p = kafka.Producer({"test.mock.num.brokers": 1, "linger.ms": 5})
    md = p.list_topics(timeout=10)
    return p, ",".join(f"{b.host}:{b.port}" for b in md.brokers.values())


def _pump(pg, producer, seen: int) -> int:
    for r in outbox(pg, seen):
        k, v = envelope(r)
        producer.produce(TOPIC, key=k, value=v)
        seen = r[0]
    producer.flush(10)
    return seen


def _consumer(bootstrap, grp, phase, idle):
    return subprocess.Popen([sys.executable, "-m", "discovery.cdc", "--dsn", DSN, "--bootstrap", bootstrap, "consume",
                             "--group", grp, "--phase", phase, "--until-idle", str(idle), "--commit-every", "0.5"],
                            cwd=ROOT, stdout=subprocess.PIPE, text=True)


def test_consumer_killed_mid_stream_resumes_from_committed_offsets(pg, tmp_path):
    cdc.seed(DSN, make_dump(tmp_path / "big.db", 2600), rows=2000, reserve=600)
    install_fake_source(pg)
    producer, bootstrap = _mock_broker()
    snapshot_to_outbox(pg)
    seen = _pump(pg, producer, 0)
    for k in range(4):                                   # 4 x 500 changes, pumped as they happen
        cdc.changes(DSN, 500, rate=0, seed=7 + k)
        seen = _pump(pg, producer, seen)
    total = pg.execute("SELECT count(*) FROM fake.outbox").fetchone()[0]

    proc = _consumer(bootstrap, "g", "kill", 0)
    t0 = time.monotonic()
    while pg.execute("SELECT count(*) FROM sink.applied WHERE grp = 'g'").fetchone()[0] < total // 2:
        assert proc.poll() is None and time.monotonic() - t0 < 60
        time.sleep(0.02)
    proc.kill()
    proc.wait()
    at_kill = cdc.offsets(bootstrap, "g")["partitions"]
    applied = cdc._applied_max(pg, "g")
    uncommitted = sum(o + 1 - max(at_kill[p]["committed"], 0) for p, o in applied.items())
    assert pg.execute("SELECT ended_at FROM sink.consumer_runs WHERE phase = 'kill'").fetchone()[0] is None

    out, _ = _consumer(bootstrap, "g", "restart", 3).communicate(timeout=120)
    res = json.loads(out.splitlines()[-1])
    assert res["exit_reason"] == "idle"
    for p, v in at_kill.items():                          # picked up exactly where the group had committed
        if v["committed"] >= 0 and f"{TOPIC}[{p}]" in res["first_offsets"]:
            assert res["first_offsets"][f"{TOPIC}[{p}]"] == v["committed"]
    red = cdc._redelivered(pg, "g")
    print(f"messages {total}, applied before kill {sum(o + 1 for o in applied.values())}, "
          f"applied but not committed {uncommitted}, redelivered {red}")
    assert red["extra_deliveries"] == uncommitted          # every applied-but-uncommitted message came again, once
    assert pg.execute("SELECT count(*) FROM sink.applied WHERE grp = 'g'").fetchone()[0] == total
    assert cdc.offsets(bootstrap, "g")["lag_total"] == 0
    cmp = cdc.compare(pg)
    assert cmp["equal"], cmp
