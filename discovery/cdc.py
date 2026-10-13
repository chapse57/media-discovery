"""Postgres change capture through Kafka: Debezium reads one table's changes from the WAL, Kafka
holds them, and a Python consumer applies them to a copy. Each claim below is measured by `run`.

    cd media-discovery
    docker compose -f cdc/docker-compose.yml up -d --build srcdb kafka connect
    docker compose -f cdc/docker-compose.yml run --rm tools python -m discovery.cdc run --out results/2026-10-14-cdc.json
    docker compose -f cdc/docker-compose.yml down -v      (topics, replication slot and data all go)

Steps of `run` (seed, connector, consume, changes, compare are also subcommands):
  seed       the 100,000 lowest ids of the public Podcast Index dump into src.podcasts, the next
             20,000 into stage.reserve (rows the change generator inserts later; not captured).
  connector  Debezium's Postgres connector (pgoutput, filtered publication on src.podcasts only,
             initial snapshot) registered through Kafka Connect's REST API; waits until it runs.
  snapshot   a consumer applies the snapshot to sink.podcasts: rows, checksum, row-by-row diff.
  stream     single-statement transactions (insert / update / delete) at a fixed rate while the
             consumer runs. Delay = sink write time - source commit time (Debezium source.ts_us),
             both from the same Postgres clock, so no clock skew between two machines.
  kill       the consumer is SIGKILLed mid-stream and started again: committed offsets at the kill,
             where it resumed, how many messages it was given twice, and whether sink == source.
  replay     sink emptied, rebuilt from offset 0 by a new consumer group; checksum again.
  schema     a column is added to the source table. The consumer stops at the first message that
             carries it, without committing past it; the column is added to the sink; the consumer
             starts again from that message. Checksum including the new column.

Consumer contract: a Kafka offset is committed only after the sink transaction holding that message
has committed (at least once). Applying is idempotent by primary key (upsert, delete), so a message
delivered twice changes nothing. Every applied message is logged in sink.applied by (group, topic,
partition, offset); a second delivery increments `times`, so redeliveries are counted, not hidden.
A message whose columns differ from the sink table stops the consumer: nothing is guessed or dropped.

All of it runs in its own containers (cdc/docker-compose.yml): Postgres on 5435, Kafka, Kafka
Connect, and a tools container for this module. The x-metrics Postgres (5432) and the pgvector
container (5434) are not touched; the dump in data/ is opened read only.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import random
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

import psycopg
from psycopg import sql

from .pidump import Converter, machine

DSN = os.environ.get("CDC_DSN", "postgresql://cdc:cdc@localhost:5435/cdc")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
CONNECTOR = "podcasts-cdc"
PREFIX = "cdc"
TOPIC = f"{PREFIX}.src.podcasts"
SLOT = "dbz_podcasts"

# (column, type, column in the dump). Integers are bigint so Debezium sends plain JSON numbers;
# updated_at is timestamptz so one value crosses as an ISO-8601 string (Debezium ZonedTimestamp).
COLUMNS = (
    ("id", "bigint", "id"),
    ("url", "text", "url"),
    ("title", "text", "title"),
    ("itunes_id", "bigint", "itunesId"),
    ("podcast_guid", "text", "podcastGuid"),
    ("language", "text", "language"),
    ("episode_count", "bigint", "episodeCount"),
    ("popularity_score", "bigint", "popularityScore"),
    ("dead", "bigint", "dead"),
    ("newest_item_pubdate", "bigint", "newestItemPubdate"),
    ("host", "text", "host"),
    ("category1", "text", "category1"),
    ("updated_at", "timestamptz", "lastUpdate"),        # epoch seconds in the dump
)
EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


def log(*a):
    print(*a, file=sys.stderr, flush=True)


# ------------------------------------------------------------------------------------------ seed --
def _ddl() -> str:
    cols = ",\n    ".join(f"{c} {t}" for c, t, _ in COLUMNS)
    return f"""
DROP SCHEMA IF EXISTS src CASCADE;
DROP SCHEMA IF EXISTS stage CASCADE;
DROP SCHEMA IF EXISTS sink CASCADE;
CREATE SCHEMA src;
CREATE SCHEMA stage;
CREATE TABLE src.podcasts (
    {cols},
    PRIMARY KEY (id)
);
CREATE TABLE stage.reserve (LIKE src.podcasts INCLUDING ALL);
"""


SINK_DDL = """
CREATE SCHEMA IF NOT EXISTS sink;
CREATE TABLE IF NOT EXISTS sink.podcasts (LIKE src.podcasts INCLUDING ALL);
CREATE TABLE IF NOT EXISTS sink.applied (
    grp text NOT NULL,                  -- consumer group
    topic text NOT NULL,
    part int NOT NULL,
    off bigint NOT NULL,
    op text NOT NULL,                   -- r (snapshot read) c u d, or tombstone
    id bigint,
    phase text,                         -- label of the consumer run that first applied it
    src_commit timestamptz,             -- Debezium source.ts_us: commit time in the source
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    times int NOT NULL DEFAULT 1,       -- > 1: delivered again after a crash, applied again (no-op)
    last_applied_at timestamptz,
    PRIMARY KEY (grp, topic, part, off)
);
CREATE TABLE IF NOT EXISTS sink.consumer_runs (
    run_id serial PRIMARY KEY,
    grp text NOT NULL,
    phase text,
    pid int,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    committed_at_start jsonb,           -- the group's committed offsets when partitions were assigned
    first_offsets jsonb,                -- first offset actually received per partition
    ended_at timestamptz,               -- stays NULL when the process was killed
    exit_reason text,
    messages int,
    detail jsonb
);
"""


def _epoch_to_ts(v, name: str, coerced: Counter):
    if v is None:
        return None
    if 0 < v < 4_102_444_800:                       # before 2100-01-01
        return EPOCH + dt.timedelta(seconds=v)
    coerced[name] += 1
    return None


def seed(dsn: str, db: Path, rows: int = 100_000, reserve: int = 20_000) -> dict:
    """Lowest ids of the dump; the same Converter as the full load, so a dirty value is NULLed and
    counted, never guessed. Refuses to run over a database a connector has already read."""
    t0 = time.perf_counter()
    lite = sqlite3.connect(f"file:{db}?mode=ro&immutable=1", uri=True)
    lite.text_factory = lambda b: b.decode("utf-8", "replace")
    names = [c for c, _, _ in COLUMNS]
    conv = Converter(names, ["bigint" if t == "timestamptz" else t for _, t, _ in COLUMNS])
    i_ts = names.index("updated_at")
    sel = ", ".join(f'"{d}"' for _, _, d in COLUMNS)
    cur = lite.execute(f'SELECT {sel} FROM podcasts ORDER BY id LIMIT ?', (rows + reserve,))
    copy_cols = sql.SQL(", ").join(map(sql.Identifier, names))
    with psycopg.connect(dsn) as pg:
        if pg.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (SLOT,)).fetchone():
            raise SystemExit(f"replication slot {SLOT} exists: a connector already read this database. "
                             "Start clean: docker compose -f cdc/docker-compose.yml down -v")
        pg.execute(_ddl())
        pg.execute(SINK_DDL)
        n = {"src.podcasts": 0, "stage.reserve": 0}
        with pg.cursor() as pc:
            for table, limit in (("src.podcasts", rows), ("stage.reserve", reserve)):
                stmt = sql.SQL("COPY {} ({}) FROM STDIN").format(sql.SQL(table), copy_cols)
                with pc.copy(stmt) as cp:
                    while n[table] < limit and (batch := cur.fetchmany(min(10_000, limit - n[table]))):
                        for r in batch:
                            v = conv.row(r)
                            v[i_ts] = _epoch_to_ts(v[i_ts], "updated_at", conv.coerced)
                            cp.write_row(v)
                        n[table] += len(batch)
        pg.execute("ANALYZE src.podcasts")
        pg.commit()
        span = pg.execute("SELECT min(id), max(id) FROM src.podcasts").fetchone()
        rspan = pg.execute("SELECT min(id), max(id) FROM stage.reserve").fetchone()
    return {"source_file": str(db), "rows": n["src.podcasts"], "id_range": list(span),
            "reserve_rows": n["stage.reserve"], "reserve_id_range": list(rspan),
            "values_nulled_or_cleaned": dict(conv.coerced), "seconds": round(time.perf_counter() - t0, 1)}


# ------------------------------------------------------------------------------------ connector --
def connector_config(host: str = "srcdb", port: int = 5432, user: str = "cdc", password: str = "cdc",
                     dbname: str = "cdc") -> dict:
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max": "1",
        "database.hostname": host, "database.port": str(port), "database.user": user,
        "database.password": password, "database.dbname": dbname,
        "topic.prefix": PREFIX,
        "plugin.name": "pgoutput",                 # built into Postgres 10+, nothing to install
        "slot.name": SLOT,
        "publication.name": SLOT,
        "publication.autocreate.mode": "filtered",  # publication covers src.podcasts only
        "table.include.list": "src.podcasts",
        "snapshot.mode": "initial",
        "tombstones.on.delete": "true",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "topic.creation.default.replication.factor": "1",
        "topic.creation.default.partitions": "3",  # key = id, so one row's changes stay in order
    }


def _http(method: str, url: str, body: dict | None = None, timeout: float = 10.0):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
        return r.status, (json.loads(raw) if raw else None)


def wait_connect(url: str, timeout: float = 240.0) -> dict:
    t0 = time.monotonic()
    while True:
        try:
            return _http("GET", url + "/")[1]
        except (urllib.error.URLError, ConnectionError, OSError) as e:
            if time.monotonic() - t0 > timeout:
                raise SystemExit(f"Kafka Connect not answering at {url} after {timeout:.0f}s: {e}")
            time.sleep(3)


def register(url: str, cfg: dict, timeout: float = 180.0) -> dict:
    info = wait_connect(url)
    plugins = _http("GET", url + "/connector-plugins")[1]
    plugin = next((p for p in plugins if p["class"] == cfg["connector.class"]), None)
    if not plugin:
        raise SystemExit(f"{cfg['connector.class']} not installed in Kafka Connect")
    t0 = time.perf_counter()
    _http("PUT", f"{url}/connectors/{CONNECTOR}/config", cfg)
    while True:
        try:
            st = _http("GET", f"{url}/connectors/{CONNECTOR}/status")[1]
        except urllib.error.HTTPError:
            st = None
        if st:
            states = [st["connector"]["state"]] + [t["state"] for t in st.get("tasks", [])]
            if "FAILED" in states:
                trace = st["connector"].get("trace") or next((t.get("trace") for t in st["tasks"] if t.get("trace")), "")
                raise SystemExit(f"connector failed: {states}\n{trace[:2000]}")
            if st.get("tasks") and all(s == "RUNNING" for s in states):
                break
        if time.perf_counter() - t0 > timeout:
            raise SystemExit(f"connector not RUNNING after {timeout:.0f}s: {st}")
        time.sleep(1)
    return {"connect_version": info.get("version"), "plugin": plugin["class"], "plugin_version": plugin.get("version"),
            "seconds_to_running": round(time.perf_counter() - t0, 1), "config": {k: v for k, v in cfg.items()
                                                                                if "password" not in k}}


# ------------------------------------------------------------------------------------- applying --
class SchemaDrift(Exception):
    """A message's row has columns the sink table does not (or lacks some it has)."""

    def __init__(self, topic: str, part: int, off: int, extra: list[str], missing: list[str]):
        self.where = {"topic": topic, "partition": part, "offset": off}
        self.extra, self.missing = extra, missing
        super().__init__(f"{topic}[{part}]@{off}: columns not in sink {extra}, sink columns not in message {missing}")


def _ts_us(source: dict | None):
    """Debezium source block -> commit time. ts_us where present (Debezium 2.6+), else ts_ms."""
    if not source:
        return None
    if source.get("ts_us") is not None:
        return EPOCH + dt.timedelta(microseconds=int(source["ts_us"]))
    if source.get("ts_ms") is not None:
        return EPOCH + dt.timedelta(milliseconds=int(source["ts_ms"]))
    return None


def decode(msg) -> dict:
    """A Kafka message (confluent_kafka.Message or anything with the same methods) -> event."""
    key = json.loads(msg.key()) if msg.key() else None
    raw = msg.value()
    ev = {"topic": msg.topic(), "part": msg.partition(), "off": msg.offset(), "key": key}
    if raw is None:                                   # tombstone after a delete (for log compaction)
        ev.update(op="tombstone", after=None, src_commit=None, snapshot=None)
        return ev
    v = json.loads(raw)
    ev.update(op=v["op"], after=v.get("after"), before=v.get("before"), src_commit=_ts_us(v.get("source")),
              snapshot=(v.get("source") or {}).get("snapshot"))
    if ev["op"] not in ("r", "c", "u", "d"):
        raise ValueError(f"unexpected op {ev['op']!r} at {ev['topic']}[{ev['part']}]@{ev['off']}")
    return ev


class Sink:
    """Applies Debezium change events for one table to a copy, idempotently, in one transaction per batch."""

    def __init__(self, pg: psycopg.Connection, grp: str, phase: str, table: str = "sink.podcasts"):
        self.pg, self.grp, self.phase, self.table = pg, grp, phase, table
        schema, name = table.split(".")
        self.types = dict(pg.execute(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
            "JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
            (schema, name)).fetchall())
        if not self.types:
            raise SystemExit(f"{table} does not exist")
        cols = list(self.types)
        target = sql.SQL(table)
        self.upsert = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (id) DO UPDATE SET {}").format(
            target, sql.SQL(", ").join(map(sql.Identifier, cols)),
            sql.SQL(", ").join(sql.SQL("%s::" + self.types[c]) for c in cols),
            sql.SQL(", ").join(sql.SQL("{0} = EXCLUDED.{0}").format(sql.Identifier(c)) for c in cols if c != "id"))
        self.delete = sql.SQL("DELETE FROM {} WHERE id = %s").format(target)
        self.cols = cols
        self.snapshot_done = False      # Debezium marks the snapshot's final record source.snapshot = "last"

    def check(self, ev: dict) -> None:
        row = ev["after"] if ev["op"] in ("r", "c", "u") else None
        if row is None:
            return
        have, want = set(row), set(self.cols)
        if have != want:
            raise SchemaDrift(ev["topic"], ev["part"], ev["off"], sorted(have - want), sorted(want - have))

    def apply(self, msgs: list) -> dict:
        """All messages in one sink transaction. Returns {(topic, partition): next offset} to commit.
        A schema drift anywhere in the batch raises before anything is written."""
        events = [decode(m) for m in msgs]
        for ev in events:
            self.check(ev)
        log_rows, nxt = [], {}
        with self.pg.transaction():
            with self.pg.cursor() as cur:
                run_kind, run = None, []

                def flush():
                    if run:
                        cur.executemany(self.upsert if run_kind == "up" else self.delete, run)
                        run.clear()
                for ev in events:
                    if ev["op"] in ("r", "c", "u"):
                        kind, params = "up", [ev["after"][c] for c in self.cols]
                        rid = ev["after"]["id"]
                    elif ev["op"] == "d":
                        rid = (ev["key"] or {}).get("id", (ev.get("before") or {}).get("id"))
                        kind, params = "del", [rid]
                    else:                                       # tombstone: nothing to change
                        kind, params, rid = None, None, (ev["key"] or {}).get("id")
                    if kind:
                        if kind != run_kind:
                            flush()
                            run_kind = kind
                        run.append(params)
                    log_rows.append((self.grp, ev["topic"], ev["part"], ev["off"], ev["op"], rid, self.phase, ev["src_commit"]))
                    nxt[(ev["topic"], ev["part"])] = ev["off"] + 1
                    if ev.get("snapshot") in ("last", "last_in_data_collection"):
                        self.snapshot_done = True
                flush()
                cur.executemany(
                    "INSERT INTO sink.applied (grp, topic, part, off, op, id, phase, src_commit) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT (grp, topic, part, off) DO UPDATE SET times = sink.applied.times + 1, "
                    "last_applied_at = clock_timestamp()", log_rows)
        return nxt


# ------------------------------------------------------------------------------------- consumer --
def consume(dsn: str, bootstrap: str, grp: str, phase: str, topic: str = TOPIC, until_idle: float = 10.0,
            commit_every: float = 1.0, batch: int = 500, poll: float = 0.1, max_seconds: float | None = None,
            until_snapshot_done: bool = False) -> dict:
    """Read the topic, apply each batch in one sink transaction, then (at most every `commit_every`
    seconds, and always before exiting) commit the offsets of what has been applied. Exits after
    `until_idle` seconds without messages (0 = never; with until_snapshot_done, only once the
    snapshot's last record has been applied), or with exit_reason schema_drift."""
    from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

    pg = psycopg.connect(dsn, autocommit=True)
    sink = Sink(pg, grp, phase)
    run_id = pg.execute("INSERT INTO sink.consumer_runs (grp, phase, pid) VALUES (%s,%s,%s) RETURNING run_id",
                        (grp, phase, os.getpid())).fetchone()[0]
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": grp, "enable.auto.commit": False,
                  "auto.offset.reset": "earliest", "session.timeout.ms": 10_000,
                  "client.id": f"cdc-{phase}-{os.getpid()}"})
    pending: dict = {}
    first: dict = {}
    state = {"assigned": False, "last_commit": time.monotonic()}

    def commit():
        if pending:
            c.commit(offsets=[TopicPartition(t, p, o) for (t, p), o in pending.items()], asynchronous=False)
            pending.clear()
        state["last_commit"] = time.monotonic()

    def on_assign(consumer, parts):
        if not parts:
            return
        state["assigned"] = True
        try:
            got = consumer.committed(parts, timeout=10)
            pos = {f"{tp.topic}[{tp.partition}]": tp.offset for tp in got}   # -1001: nothing committed yet
            pg.execute("UPDATE sink.consumer_runs SET committed_at_start = coalesce(committed_at_start, '{}'::jsonb) || %s::jsonb "
                       "WHERE run_id = %s", (json.dumps(pos), run_id))
        except KafkaException as e:  # informational only
            log(f"committed() at assign failed: {e}")

    def on_revoke(consumer, parts):
        commit()

    c.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
    t0 = time.monotonic()
    idle_from = None
    n = 0
    reason, detail = "idle", None
    try:
        while True:
            if max_seconds and time.monotonic() - t0 > max_seconds:
                reason = "max_seconds"
                break
            msgs = c.consume(batch, poll)
            real = []
            for m in msgs:
                if m.error():
                    if m.error().code() in (KafkaError._PARTITION_EOF, KafkaError.UNKNOWN_TOPIC_OR_PART):
                        continue                  # end of partition / topic not created yet: keep polling
                    raise KafkaException(m.error())
                real.append(m)
            now = time.monotonic()
            if not real:
                if pending:
                    commit()
                if state["assigned"] and (sink.snapshot_done or not until_snapshot_done):
                    idle_from = idle_from or now
                    if until_idle and now - idle_from >= until_idle:
                        break
                continue
            idle_from = None
            new_first = {}
            for m in real:
                k = f"{m.topic()}[{m.partition()}]"
                if k not in first:
                    first[k] = new_first[k] = m.offset()
            if new_first:
                pg.execute("UPDATE sink.consumer_runs SET first_offsets = coalesce(first_offsets, '{}'::jsonb) || %s::jsonb "
                           "WHERE run_id = %s", (json.dumps(new_first), run_id))
            try:
                nxt = sink.apply(real)
            except SchemaDrift as e:
                commit()                      # what was applied before this batch, nothing after
                reason = "schema_drift"
                detail = {**e.where, "columns_not_in_sink": e.extra, "sink_columns_not_in_message": e.missing}
                log(f"STOP: {e}")
                break
            pending.update(nxt)
            n += len(real)
            if now - state["last_commit"] >= commit_every:
                commit()
        commit()
    finally:
        c.close()
    pg.execute("UPDATE sink.consumer_runs SET ended_at = clock_timestamp(), exit_reason = %s, messages = %s, detail = %s "
               "WHERE run_id = %s", (reason, n, json.dumps(detail) if detail else None, run_id))
    pg.close()
    return {"run_id": run_id, "group": grp, "phase": phase, "messages": n, "exit_reason": reason,
            "snapshot_last_record_seen": sink.snapshot_done, "first_offsets": first, "detail": detail,
            "seconds": round(time.monotonic() - t0, 1)}


# ------------------------------------------------------------------------------------- changes --
def changes(dsn: str, n: int, rate: float, seed: int = 1, mix=(0.2, 0.7, 0.1), extra_set: dict | None = None) -> dict:
    """n single-statement transactions on src.podcasts at `rate` per second (0 = as fast as possible):
    insert a reserve row / update a live row / delete a live row, in proportion `mix`.
    `extra_set` adds column = random choice to every update (used after the schema change)."""
    rng = random.Random(seed)
    counts = Counter()
    with psycopg.connect(dsn, autocommit=True) as pg:
        alive = [r[0] for r in pg.execute("SELECT id FROM src.podcasts ORDER BY id")]
        pos = {i: k for k, i in enumerate(alive)}
        reserve = [r[0] for r in pg.execute("SELECT id FROM stage.reserve r WHERE NOT EXISTS "
                                            "(SELECT 1 FROM src.podcasts s WHERE s.id = r.id) ORDER BY id")]
        reserve.reverse()
        cols = [c for c, _, _ in COLUMNS if c != "updated_at"]
        ins = sql.SQL("INSERT INTO src.podcasts ({}, updated_at) SELECT {}, now() FROM stage.reserve WHERE id = %s").format(
            sql.SQL(", ").join(map(sql.Identifier, cols)), sql.SQL(", ").join(map(sql.Identifier, cols)))
        extra = ""
        if extra_set:
            (col, _choices), = extra_set.items()
            extra = sql.SQL(", {} = %s").format(sql.Identifier(col)).as_string(pg)
        upd = ("UPDATE src.podcasts SET popularity_score = %s, episode_count = coalesce(episode_count, 0) + 1, "
               "newest_item_pubdate = extract(epoch FROM now())::bigint, updated_at = now()" + extra + " WHERE id = %s")

        def drop(i):
            k = pos.pop(i)
            last = alive.pop()
            if last != i:
                alive[k], pos[last] = last, k

        t0 = time.perf_counter()
        for k in range(n):
            if rate:
                wait = t0 + k / rate - time.perf_counter()
                if wait > 0:
                    time.sleep(wait)
            r = rng.random()
            if not alive and not reserve:
                break
            if (r < mix[0] or not alive) and reserve:
                i = reserve.pop()
                pg.execute(ins, (i,))
                pos[i] = len(alive)
                alive.append(i)
                counts["insert"] += 1
            elif r < mix[0] + mix[1]:
                i = rng.choice(alive)
                params = [rng.randint(0, 9)]
                if extra_set:
                    params.append(rng.choice(next(iter(extra_set.values()))))
                pg.execute(upd, params + [i])
                counts["update"] += 1
            else:
                i = rng.choice(alive)
                pg.execute("DELETE FROM src.podcasts WHERE id = %s", (i,))
                drop(i)
                counts["delete"] += 1
        secs = time.perf_counter() - t0
    return {"transactions": sum(counts.values()), **counts, "seconds": round(secs, 1), "per_second": round(n / secs, 1) if secs else None,
            "target_per_second": rate, "seed": seed}


# ------------------------------------------------------------------------------------- compare --
def _cols(pg, schema: str) -> list[str]:
    return [r[0] for r in pg.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = 'podcasts' "
        "ORDER BY ordinal_position", (schema,))]


def compare(pg) -> dict:
    """src.podcasts vs sink.podcasts on the columns both have: rows, md5 over every row in id order,
    and a row-by-row diff (missing in sink, extra in sink, same id with different values)."""
    s_cols, k_cols = _cols(pg, "src"), _cols(pg, "sink")
    common = [c for c in s_cols if c in k_cols]
    ident = sql.SQL(", ").join(map(sql.Identifier, common))

    def side(table):
        q = sql.SQL("SELECT count(*), md5(coalesce(string_agg(ROW({})::text, E'\\n' ORDER BY id), '')) FROM {}").format(
            ident, sql.SQL(table))
        return pg.execute(q).fetchone()
    (sn, smd5), (kn, kmd5) = side("src.podcasts"), side("sink.podcasts")
    rs = sql.SQL(", ").join(sql.SQL("s.{}").format(sql.Identifier(c)) for c in common)
    rk = sql.SQL(", ").join(sql.SQL("k.{}").format(sql.Identifier(c)) for c in common)
    miss, extra, diff = pg.execute(sql.SQL(
        "SELECT count(*) FILTER (WHERE k.id IS NULL), count(*) FILTER (WHERE s.id IS NULL), "
        "count(*) FILTER (WHERE s.id IS NOT NULL AND k.id IS NOT NULL AND ROW({}) IS DISTINCT FROM ROW({})) "
        "FROM src.podcasts s FULL JOIN sink.podcasts k ON k.id = s.id").format(rs, rk)).fetchone()
    return {"source_rows": sn, "sink_rows": kn, "source_md5": smd5, "sink_md5": kmd5,
            "missing_in_sink": miss, "extra_in_sink": extra, "different": diff,
            "columns_compared": len(common), "columns_only_in_source": [c for c in s_cols if c not in k_cols],
            "equal": smd5 == kmd5 and sn == kn and miss == extra == diff == 0}


def latency(pg, grp: str, phase: str) -> dict:
    row = pg.execute(
        "SELECT count(*), percentile_cont(0.5) WITHIN GROUP (ORDER BY ms), percentile_cont(0.95) WITHIN GROUP (ORDER BY ms), "
        "max(ms), min(ms) FROM (SELECT extract(epoch FROM applied_at - src_commit) * 1000 AS ms FROM sink.applied "
        "WHERE grp = %s AND phase = %s AND op IN ('c','u','d') AND src_commit IS NOT NULL) x", (grp, phase)).fetchone()
    n, p50, p95, mx, mn = row
    r = lambda v: round(float(v), 1) if v is not None else None  # noqa: E731
    return {"changes": n, "p50_ms": r(p50), "p95_ms": r(p95), "max_ms": r(mx), "min_ms": r(mn)}


# ------------------------------------------------------------------------------------ kafka side --
def _partitions(bootstrap: str, topic: str) -> list[int]:
    from confluent_kafka import Consumer
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "cdc-inspect"})
    try:
        md = c.list_topics(topic, timeout=10)
        t = md.topics.get(topic)
        return sorted(t.partitions) if t and not t.error else []
    finally:
        c.close()


def wait_topic(bootstrap: str, topic: str = TOPIC, timeout: float = 180.0) -> dict:
    t0 = time.monotonic()
    while not (parts := _partitions(bootstrap, topic)):
        if time.monotonic() - t0 > timeout:
            raise SystemExit(f"topic {topic} not created {timeout:.0f}s after the connector started")
        time.sleep(1)
    return {"topic": topic, "partitions": len(parts), "seconds_waited": round(time.monotonic() - t0, 1)}


def offsets(bootstrap: str, grp: str, topic: str = TOPIC) -> dict:
    """Committed offset and high watermark per partition for a group; lag = high - committed."""
    from confluent_kafka import Consumer, TopicPartition
    parts = _partitions(bootstrap, topic)
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": grp, "enable.auto.commit": False})
    try:
        committed = {tp.partition: tp.offset for tp in c.committed([TopicPartition(topic, p) for p in parts], timeout=10)}
        out = {}
        for p in parts:
            lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
            co = committed.get(p, -1001)
            out[str(p)] = {"low": lo, "committed": co, "high": hi, "lag": hi - (co if co >= 0 else lo)}
        return {"partitions": out, "lag_total": sum(v["lag"] for v in out.values())}
    finally:
        c.close()


def _applied_max(pg, grp: str) -> dict:
    return {str(p): o for p, o in pg.execute("SELECT part, max(off) FROM sink.applied WHERE grp = %s GROUP BY part ORDER BY part",
                                             (grp,))}


def _redelivered(pg, grp: str) -> dict:
    n, extra = pg.execute("SELECT count(*), coalesce(sum(times - 1), 0) FROM sink.applied WHERE grp = %s AND times > 1",
                          (grp,)).fetchone()
    return {"messages_delivered_more_than_once": n, "extra_deliveries": int(extra)}


# ------------------------------------------------------------------------------------------ run --
class Ctx:
    def __init__(self, a):
        self.dsn, self.bootstrap, self.connect_url, self.topic = a.dsn, a.bootstrap, a.connect_url, TOPIC
        self.idle = a.idle

    def consumer(self, grp: str, phase: str, until_idle: float | None = None, snapshot: bool = False,
                 max_seconds: float | None = None) -> subprocess.Popen:
        cmd = [sys.executable, "-m", "discovery.cdc", "--dsn", self.dsn, "--bootstrap", self.bootstrap,
               "consume", "--group", grp, "--phase", phase, "--until-idle", str(self.idle if until_idle is None else until_idle)]
        if snapshot:
            cmd.append("--until-snapshot-done")
        if max_seconds:
            cmd += ["--max-seconds", str(max_seconds)]
        return subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)

    @staticmethod
    def finish(proc: subprocess.Popen, timeout: float = 1800) -> dict:
        out, _ = proc.communicate(timeout=timeout)
        lines = [ln for ln in out.splitlines() if ln.startswith("{")]
        res = json.loads(lines[-1]) if lines else {}
        res["returncode"] = proc.returncode
        return res

    def wait_assigned(self, pg, grp: str, phase: str, timeout: float = 60) -> None:
        t0 = time.monotonic()
        while not pg.execute("SELECT 1 FROM sink.consumer_runs WHERE grp = %s AND phase = %s AND committed_at_start IS NOT NULL",
                             (grp, phase)).fetchone():
            if time.monotonic() - t0 > timeout:
                raise SystemExit(f"consumer {grp}/{phase} not assigned partitions after {timeout}s")
            time.sleep(0.5)


def preflight(ctx: Ctx, pg) -> None:
    problems = []
    try:
        if CONNECTOR in (_http("GET", ctx.connect_url + "/connectors")[1] or []):
            problems.append(f"connector {CONNECTOR} already registered")
    except (urllib.error.URLError, OSError):
        pass                                          # Connect still starting; register() waits for it
    if pg.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (SLOT,)).fetchone():
        problems.append(f"replication slot {SLOT} exists")
    if _partitions(ctx.bootstrap, ctx.topic):
        problems.append(f"topic {ctx.topic} exists")
    if problems:
        raise SystemExit("not a fresh stack (" + "; ".join(problems) + "). "
                         "Start clean: docker compose -f cdc/docker-compose.yml down -v, then up again.")


def run(a) -> dict:
    from confluent_kafka import libversion, version
    ck = version()
    ctx = Ctx(a)
    t_all = time.perf_counter()
    res: dict = {"machine": machine(), "settings": {
        "rows": a.rows, "reserve": a.reserve, "stream_changes": a.changes, "kill_changes": a.kill_changes,
        "rate_per_second": a.rate, "kill_after_messages": a.kill_after, "consumer_batch": 500, "consumer_poll_s": 0.1,
        "commit_every_s": 1.0, "idle_exit_s": a.idle, "partitions": 3}}
    out = Path(a.out) if a.out else None

    def save():
        if out:
            out.write_text(json.dumps(res, indent=2, ensure_ascii=False, default=str), encoding="utf-8")

    pg = psycopg.connect(ctx.dsn, autocommit=True)
    preflight(ctx, pg)
    res["versions"] = {"postgres": pg.execute("SHOW server_version").fetchone()[0],
                       "kafka_image": os.environ.get("KAFKA_IMAGE"), "connect_image": os.environ.get("CONNECT_IMAGE"),
                       "confluent_kafka": ck[0] if isinstance(ck, tuple) else ck, "librdkafka": libversion()[0]}

    log("seed ...")
    res["seed"] = seed(ctx.dsn, Path(a.db), a.rows, a.reserve)
    save()
    log("connector ...")
    if a.fake_source:                                  # rehearsal without Kafka Connect (tests only)
        res["connector"] = a.fake_source(ctx)
    else:
        res["connector"] = register(ctx.connect_url, connector_config())
    res["connector"]["topic"] = wait_topic(ctx.bootstrap, ctx.topic)
    res["versions"]["connect"] = res["connector"].get("connect_version")
    res["versions"]["debezium_postgres"] = res["connector"].get("plugin_version")
    save()

    g1, g2 = "sink-main", "sink-replay"

    log("snapshot ...")
    snap = ctx.finish(ctx.consumer(g1, "snapshot", snapshot=True, max_seconds=900))
    span = pg.execute("SELECT count(*), extract(epoch FROM max(applied_at) - min(applied_at)) FROM sink.applied "
                      "WHERE grp = %s AND phase = 'snapshot'", (g1,)).fetchone()
    res["snapshot"] = {"consumer": snap, "messages_applied": span[0],
                       "apply_seconds": round(float(span[1] or 0), 1),
                       "compare": compare(pg), "offsets": offsets(ctx.bootstrap, g1)}
    save()

    log("stream ...")
    proc = ctx.consumer(g1, "stream")
    ctx.wait_assigned(pg, g1, "stream")
    gen = changes(ctx.dsn, a.changes, a.rate, seed=1)
    res["stream"] = {"generator": gen, "consumer": ctx.finish(proc), "latency": latency(pg, g1, "stream"),
                     "compare": compare(pg), "offsets": offsets(ctx.bootstrap, g1)}
    save()

    log("kill ...")
    before = _redelivered(pg, g1)
    proc = ctx.consumer(g1, "kill", until_idle=0)
    ctx.wait_assigned(pg, g1, "kill")
    gen_out: dict = {}
    th = threading.Thread(target=lambda: gen_out.update(changes(ctx.dsn, a.kill_changes, a.rate, seed=2)))
    th.start()
    t0 = time.monotonic()
    while pg.execute("SELECT count(*) FROM sink.applied WHERE grp = %s AND phase = 'kill'", (g1,)).fetchone()[0] < a.kill_after:
        if time.monotonic() - t0 > 300 or proc.poll() is not None:
            raise SystemExit("kill phase: consumer did not reach the kill point")
        time.sleep(0.05)
    proc.kill()                                         # SIGKILL: no commit, no cleanup
    proc.wait()
    at_kill = {"committed": offsets(ctx.bootstrap, g1)["partitions"], "applied_max": _applied_max(pg, g1),
               "applied_in_phase": pg.execute("SELECT count(*) FROM sink.applied WHERE grp = %s AND phase = 'kill'",
                                              (g1,)).fetchone()[0]}
    uncommitted = 0
    for p, o in at_kill["applied_max"].items():
        co = at_kill["committed"].get(p, {}).get("committed", -1)
        uncommitted += max(0, o + 1 - max(co, 0))
    killed_run = pg.execute("SELECT run_id, ended_at FROM sink.consumer_runs WHERE grp = %s AND phase = 'kill'", (g1,)).fetchone()
    time.sleep(2)
    proc2 = ctx.consumer(g1, "kill-restart")
    th.join()
    restart = ctx.finish(proc2)
    resumed = pg.execute("SELECT committed_at_start, first_offsets FROM sink.consumer_runs WHERE grp = %s AND phase = 'kill-restart'",
                         (g1,)).fetchone()
    first = resumed[1] or {}
    resumed_at_committed = all(first.get(f"{ctx.topic}[{p}]", v["committed"]) == v["committed"]
                               for p, v in at_kill["committed"].items() if v["committed"] >= 0)
    after = _redelivered(pg, g1)
    res["kill"] = {"generator": gen_out, "killed_after_messages_in_phase": at_kill["applied_in_phase"],
                   "killed_run_ended_at": killed_run[1], "returncode_of_killed": proc.returncode,
                   "at_kill": at_kill, "applied_but_not_committed_at_kill": uncommitted,
                   "restart": restart, "restart_committed_at_start": resumed[0], "restart_first_offsets": first,
                   "resumed_at_committed_offsets": resumed_at_committed,
                   "redelivered": {k: after[k] - before[k] for k in after},
                   "compare": compare(pg), "offsets": offsets(ctx.bootstrap, g1)}
    save()

    log("replay ...")
    pg.execute("TRUNCATE sink.podcasts")
    t0 = time.perf_counter()
    rep = ctx.finish(ctx.consumer(g2, "replay"))
    rep_off = offsets(ctx.bootstrap, g2)
    res["replay"] = {"group": g2, "consumer": rep, "seconds_including_idle_exit": round(time.perf_counter() - t0, 1),
                     "topic_starts_at_offset_0": all(v["low"] == 0 for v in rep_off["partitions"].values()),
                     "compare": compare(pg), "offsets": rep_off}
    save()

    log("schema ...")
    pg.execute("ALTER TABLE src.podcasts ADD COLUMN region text")
    proc = ctx.consumer(g1, "schema")
    ctx.wait_assigned(pg, g1, "schema")
    gen = changes(ctx.dsn, a.schema_changes, a.rate, seed=3, mix=(0.0, 1.0, 0.0), extra_set={"region": ["KR", "US", "DE", "BR"]})
    stopped = ctx.finish(proc, timeout=300)
    at_stop = offsets(ctx.bootstrap, g1)
    behind = compare(pg)                                # sink is behind on purpose: the updates after ALTER are not applied
    pg.execute("ALTER TABLE sink.podcasts ADD COLUMN region text")
    fixed = ctx.finish(ctx.consumer(g1, "schema-fixed"))
    fixed_row = pg.execute("SELECT first_offsets FROM sink.consumer_runs WHERE grp = %s AND phase = 'schema-fixed'", (g1,)).fetchone()
    drift = stopped.get("detail") or {}
    res["schema"] = {"change": "ALTER TABLE src.podcasts ADD COLUMN region text", "generator": gen,
                     "consumer_stopped": stopped, "offsets_at_stop": at_stop, "compare_while_stopped": behind,
                     "fix": "ALTER TABLE sink.podcasts ADD COLUMN region text", "consumer_after_fix": fixed,
                     "restarted_at_drift_message": bool(drift) and (fixed_row[0] or {}).get(
                         f"{drift.get('topic')}[{drift.get('partition')}]") == drift.get("offset"),
                     "compare": compare(pg), "offsets": offsets(ctx.bootstrap, g1)}
    res["seconds_total"] = round(time.perf_counter() - t_all, 1)
    res["summary"] = summarize(res)
    save()
    pg.close()
    return res


def summarize(res: dict) -> dict:
    """The numbers worth quoting, each taken from the phase that measured it."""
    sn, st, k, rp, sc = (res.get(x, {}) for x in ("snapshot", "stream", "kill", "replay", "schema"))
    eq = lambda ph: ph.get("compare", {}).get("equal")  # noqa: E731
    return {
        "snapshot": {"rows": sn.get("compare", {}).get("source_rows"), "apply_seconds": sn.get("apply_seconds"),
                     "equal": eq(sn), "lag": sn.get("offsets", {}).get("lag_total")},
        "stream": {"transactions": st.get("generator", {}).get("transactions"),
                   "per_second": st.get("generator", {}).get("per_second"),
                   "delay_p50_ms": st.get("latency", {}).get("p50_ms"), "delay_p95_ms": st.get("latency", {}).get("p95_ms"),
                   "delay_max_ms": st.get("latency", {}).get("max_ms"), "equal": eq(st)},
        "kill": {"killed_after_messages": k.get("killed_after_messages_in_phase"),
                 "applied_not_committed": k.get("applied_but_not_committed_at_kill"),
                 "redelivered": k.get("redelivered", {}).get("extra_deliveries"),
                 "resumed_at_committed_offsets": k.get("resumed_at_committed_offsets"), "equal": eq(k),
                 "lag": k.get("offsets", {}).get("lag_total")},
        "replay": {"messages": rp.get("consumer", {}).get("messages"), "from_offset_0": rp.get("topic_starts_at_offset_0"),
                   "equal": eq(rp)},
        "schema": {"consumer_stopped": sc.get("consumer_stopped", {}).get("exit_reason"),
                   "rows_behind_while_stopped": sc.get("compare_while_stopped", {}).get("different"),
                   "restarted_at_drift_message": sc.get("restarted_at_drift_message"), "equal": eq(sc)},
    }


# ------------------------------------------------------------------------------------------ cli --
def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="discovery.cdc")
    ap.add_argument("--dsn", default=DSN)
    ap.add_argument("--bootstrap", default=BOOTSTRAP)
    ap.add_argument("--connect-url", default=CONNECT_URL)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--db", default="data/podcastindex_feeds.db")
    s.add_argument("--rows", type=int, default=100_000)
    s.add_argument("--reserve", type=int, default=20_000)
    sub.add_parser("connector")
    c = sub.add_parser("consume")
    c.add_argument("--group", required=True)
    c.add_argument("--phase", default="manual")
    c.add_argument("--until-idle", type=float, default=10.0)
    c.add_argument("--commit-every", type=float, default=1.0)
    c.add_argument("--until-snapshot-done", action="store_true")
    c.add_argument("--max-seconds", type=float)
    g = sub.add_parser("changes")
    g.add_argument("--n", type=int, default=1000)
    g.add_argument("--rate", type=float, default=50.0)
    g.add_argument("--seed", type=int, default=1)
    sub.add_parser("compare")
    o = sub.add_parser("offsets")
    o.add_argument("--group", required=True)
    r = sub.add_parser("run")
    r.add_argument("--db", default="data/podcastindex_feeds.db")
    r.add_argument("--rows", type=int, default=100_000)
    r.add_argument("--reserve", type=int, default=20_000)
    r.add_argument("--changes", type=int, default=3000, help="stream phase transactions")
    r.add_argument("--kill-changes", type=int, default=3000)
    r.add_argument("--kill-after", type=int, default=1000, help="SIGKILL once the consumer applied this many in the kill phase")
    r.add_argument("--schema-changes", type=int, default=50)
    r.add_argument("--rate", type=float, default=50.0, help="source transactions per second")
    r.add_argument("--idle", type=float, default=10.0, help="a consumer exits after this many idle seconds")
    r.add_argument("--out")
    a = ap.parse_args(argv)
    sys.stdout.reconfigure(encoding="utf-8")
    if a.cmd == "seed":
        res = seed(a.dsn, Path(a.db), a.rows, a.reserve)
    elif a.cmd == "connector":
        res = register(a.connect_url, connector_config())
    elif a.cmd == "consume":
        res = consume(a.dsn, a.bootstrap, a.group, a.phase, until_idle=a.until_idle, commit_every=a.commit_every,
                      until_snapshot_done=a.until_snapshot_done, max_seconds=a.max_seconds)
    elif a.cmd == "changes":
        res = changes(a.dsn, a.n, a.rate, a.seed)
    elif a.cmd == "compare":
        with psycopg.connect(a.dsn) as pg:
            res = compare(pg)
    elif a.cmd == "offsets":
        res = offsets(a.bootstrap, a.group)
    else:
        a.fake_source = None
        res = run(a)
    print(json.dumps(res, indent=None if a.cmd == "consume" else 2, ensure_ascii=False, default=str))
    if a.cmd == "consume" and res.get("exit_reason") == "schema_drift":
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
