"""Podcast descriptions as vectors in pgvector: "shows like this one", measured against exact search.

    docker compose -f vector/docker-compose.yml up -d vectordb
    docker compose -f vector/docker-compose.yml run --rm embed python -m discovery.vectors all --out results/2026-10-13-vectors.json

Steps (all of them with `all`):
  sample   read a deterministic sample from pi.podcasts in the `discovery` database (read only):
           active in 2026, language en*, description >= 200 characters, not dead, not marked as a
           duplicate, with an Apple id. Copied into the separate pgvector database (vec.podcasts).
  embed    title + description -> all-MiniLM-L6-v2 (384 dimensions, normalised), on CPU, locally.
  index    HNSW (cosine, m=16, ef_construction=64); build time and size.
  evaluate 200 sample shows as queries: exact top 10 (sequential scan) vs HNSW top 10 at
           ef_search 40 and 100 -> recall@10 and latency; how often a neighbour shares the query's
           first category, against what random rows would give; three text searches as examples.
  neardup  every row's nearest neighbour through HNSW; how many pairs are almost identical.

Nothing is written to the discovery database. The pgvector database is its own container (port
5434), so the x-metrics Postgres (5432) keeps its image and settings.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import sys
import time
from collections import Counter
from pathlib import Path

from .lake import YEAR_SQL, log

MODEL = "sentence-transformers/all-MiniLM-L6-v2"
DIM = 384
QUERIES = 200
TEXT_SEARCHES = ("true crime investigations of unsolved murders",
                 "personal finance and investing for beginners",
                 "machine learning and artificial intelligence research")

SAMPLE_SQL = f"""
SELECT id, itunes_id, title, description, lower(btrim(category1)) AS category, host
FROM pi.podcasts
WHERE {YEAR_SQL} = '2026'
  AND lower(language) LIKE 'en%%'
  AND length(description) >= 200
  AND coalesce(dead, 0) = 0
  AND coalesce(duplicate_of, 0) = 0
  AND itunes_id > 0
ORDER BY md5(id::text)
LIMIT %s
"""

DDL = f"""
CREATE EXTENSION IF NOT EXISTS vector;
CREATE SCHEMA IF NOT EXISTS vec;
DROP TABLE IF EXISTS vec.podcasts;
CREATE TABLE vec.podcasts (
    id bigint PRIMARY KEY,           -- Podcast Index id
    itunes_id bigint,
    title text,
    description text,
    category text,
    host text,
    embedding vector({DIM})
);
"""


def _connect(dsn: str):
    import psycopg
    from pgvector.psycopg import register_vector
    # prepare_threshold=None: plan every execution with its own parameters. With psycopg's default
    # (prepare after 5 runs) the timing loops reused one cached plan — the 10-13 first run measured
    # a sequential scan in the HNSW loop too, because the exact loop had prepared the same query.
    conn = psycopg.connect(dsn, connect_timeout=10, autocommit=True, prepare_threshold=None)
    conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
    register_vector(conn)
    return conn


def sample(src_dsn: str, vec_dsn: str, limit: int) -> dict:
    import psycopg
    t0 = time.perf_counter()
    with psycopg.connect(src_dsn, connect_timeout=10) as src:
        src.read_only = True
        rows = src.execute(SAMPLE_SQL, (limit,)).fetchall()
        eligible = src.execute("SELECT count(*) FROM (" + SAMPLE_SQL.replace("LIMIT %s", "") + ") s").fetchone()[0]
    read_s = time.perf_counter() - t0
    with _connect(vec_dsn) as vec:
        vec.execute(DDL)
        with vec.cursor() as cur, cur.copy("COPY vec.podcasts (id, itunes_id, title, description, category, host) FROM STDIN") as cp:
            for r in rows:
                cp.write_row(r)
        n = vec.execute("SELECT count(*) FROM vec.podcasts").fetchone()[0]
    log(f"  sample: {n:,} of {eligible:,} eligible rows, {read_s:.1f}s to read")
    return {"rows": n, "eligible_in_dump": eligible, "read_seconds": round(read_s, 1),
            "filter": "active in 2026, language en*, description >= 200 chars, not dead, not duplicate_of, has Apple id",
            "order": "md5(id) — deterministic, not by popularity"}


def embed(vec_dsn: str, batch: int = 256) -> dict:
    import numpy as np
    import torch
    from sentence_transformers import SentenceTransformer

    t_load = time.perf_counter()
    model = SentenceTransformer(MODEL, device="cpu")
    load_s = time.perf_counter() - t_load
    with _connect(vec_dsn) as vec:
        rows = vec.execute("SELECT id, title, description FROM vec.podcasts ORDER BY id").fetchall()
        texts = [f"{t or ''}. {(d or '')[:1500]}" for _, t, d in rows]
        t0 = time.perf_counter()
        emb = model.encode(texts, batch_size=batch, normalize_embeddings=True, show_progress_bar=False,
                           convert_to_numpy=True).astype(np.float32)
        enc_s = time.perf_counter() - t0
        t1 = time.perf_counter()
        with vec.cursor() as cur:
            cur.execute("CREATE TEMP TABLE e (id bigint, embedding vector(%s))" % DIM)
            with cur.copy("COPY e (id, embedding) FROM STDIN WITH (FORMAT BINARY)") as cp:
                cp.set_types(["int8", "vector"])
                for (i, _, _), v in zip(rows, emb):
                    cp.write_row((i, v))
            cur.execute("UPDATE vec.podcasts p SET embedding = e.embedding FROM e WHERE e.id = p.id")
        write_s = time.perf_counter() - t1
        missing = vec.execute("SELECT count(*) FROM vec.podcasts WHERE embedding IS NULL").fetchone()[0]
        vec.execute("VACUUM ANALYZE vec.podcasts")
    log(f"  embed: {len(rows):,} rows in {enc_s:.1f}s ({len(rows) / enc_s:.0f}/s), written in {write_s:.1f}s")
    return {"model": MODEL, "dimensions": DIM, "device": "cpu", "torch_threads": torch.get_num_threads(),
            "max_seq_length_tokens": model.max_seq_length, "text": "title + '. ' + first 1500 chars of description",
            "rows": len(rows), "model_load_seconds": round(load_s, 1), "encode_seconds": round(enc_s, 1),
            "rows_per_second": round(len(rows) / enc_s), "write_seconds": round(write_s, 1),
            "rows_without_embedding": missing}


def index(vec_dsn: str) -> dict:
    with _connect(vec_dsn) as vec:
        vec.execute("DROP INDEX IF EXISTS vec.podcasts_embedding_hnsw")
        vec.execute("SET maintenance_work_mem = '1GB'")
        t0 = time.perf_counter()
        vec.execute("CREATE INDEX podcasts_embedding_hnsw ON vec.podcasts "
                    "USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)")
        build_s = time.perf_counter() - t0
        q = lambda s: vec.execute(s).fetchone()[0]  # noqa: E731
        out = {"type": "hnsw", "opclass": "vector_cosine_ops", "m": 16, "ef_construction": 64,
               "maintenance_work_mem": "1GB", "build_seconds": round(build_s, 1),
               "index_size": q("SELECT pg_size_pretty(pg_relation_size('vec.podcasts_embedding_hnsw'))"),
               "table_size": q("SELECT pg_size_pretty(pg_table_size('vec.podcasts'))"),
               "pgvector": q("SELECT extversion FROM pg_extension WHERE extname = 'vector'"),
               "postgres": q("SHOW server_version")}
    log(f"  index: {out['build_seconds']}s, {out['index_size']}")
    return out


NEIGHBOURS = "SELECT id FROM vec.podcasts WHERE id <> %s ORDER BY embedding <=> %s LIMIT 10"


def _timed(vec, qid, qvec) -> tuple[list[int], float]:
    t = time.perf_counter()
    ids = [r[0] for r in vec.execute(NEIGHBOURS, (qid, qvec)).fetchall()]
    return ids, (time.perf_counter() - t) * 1000


def _ms(xs: list[float]) -> dict:
    xs = sorted(xs)
    return {"p50_ms": round(statistics.median(xs), 2), "p95_ms": round(xs[int(len(xs) * 0.95) - 1], 2),
            "mean_ms": round(statistics.fmean(xs), 2)}


def _plan(conn, q) -> list[str]:
    """EXPLAIN ANALYZE of one query, vector literal cut out, so the plan that actually ran is on record."""
    rows = conn.execute("EXPLAIN (ANALYZE, BUFFERS OFF) " + NEIGHBOURS, (q[0], q[1])).fetchall()
    return [r[0].split("'[")[0] + ("<vector>" if "'[" in r[0] else "") for r in rows]


def evaluate(vec_dsn: str, n: int = QUERIES) -> dict:
    with _connect(vec_dsn) as vec:
        qs = vec.execute("SELECT id, embedding, category FROM vec.podcasts ORDER BY md5('q' || id::text) LIMIT %s",
                         (n,)).fetchall()
        cat_of = dict(vec.execute("SELECT id, category FROM vec.podcasts").fetchall())
        freq = Counter(c for c in cat_of.values() if c)
        total = len(cat_of)

    plans = {}
    with _connect(vec_dsn) as ex:                       # its own connection: nothing shared with the HNSW runs
        ex.execute("SET enable_indexscan = off")
        plans["exact"] = _plan(ex, qs[0])
        exact, exact_ms = {}, []
        for qid, qv, _ in qs:
            exact[qid], ms = _timed(ex, qid, qv)
            exact_ms.append(ms)

    with _connect(vec_dsn) as vec:
        approx = {}
        for ef in (40, 100):
            vec.execute(f"SET hnsw.ef_search = {ef}")
            plans[f"hnsw_ef_search_{ef}"] = _plan(vec, qs[0])
            got, ms_all, recall = {}, [], []
            for qid, qv, _ in qs:
                got[qid], ms = _timed(vec, qid, qv)
                ms_all.append(ms)
                recall.append(len(set(got[qid]) & set(exact[qid])) / 10)
            approx[f"ef_search_{ef}"] = {"recall_at_10": round(statistics.fmean(recall), 4),
                                         "queries_with_full_recall": sum(r == 1.0 for r in recall),
                                         "worst_recall": min(recall), **_ms(ms_all)}

        # does a neighbour share the query's first category, more often than chance?
        with_cat = [(qid, c) for qid, _, c in qs if c]
        share = statistics.fmean(sum(cat_of.get(i) == c for i in exact[qid]) / 10 for qid, c in with_cat)
        chance = statistics.fmean(freq[c] / total for _, c in with_cat)

        searches = []
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer(MODEL, device="cpu")
        vec.execute("SET hnsw.ef_search = 40")
        for text in TEXT_SEARCHES:
            v = model.encode([text], normalize_embeddings=True)[0]
            rows = vec.execute("SELECT title, category, round((1 - (embedding <=> %s))::numeric, 3) "
                               "FROM vec.podcasts ORDER BY embedding <=> %s LIMIT 5", (v, v)).fetchall()
            searches.append({"query": text, "top5": [[t, c, float(s)] for t, c, s in rows]})
    out_exact_p50 = _ms(exact_ms)["p50_ms"]
    out = {"queries": len(qs), "exact": {"method": "sequential scan + sort (index scans off)", **_ms(exact_ms)},
           **approx, "plans": plans,
           "index_used_check": {"hnsw_p50_over_exact_p50": round(approx["ef_search_40"]["p50_ms"] / out_exact_p50, 3),
                                "suspect_no_index": approx["ef_search_40"]["p50_ms"] > 0.5 * out_exact_p50},
           "first_category_shared_by_neighbours": {"queries_with_a_category": len(with_cat),
                                                    "share_of_top10": round(share, 4),
                                                    "if_neighbours_were_random": round(chance, 4)},
           "text_searches": searches}
    log(f"  evaluate: recall@10 ef40 {approx['ef_search_40']['recall_at_10']}, ef100 {approx['ef_search_100']['recall_at_10']}; "
        f"exact p50 {out['exact']['p50_ms']} ms vs hnsw p50 {approx['ef_search_40']['p50_ms']} ms; "
        f"category {share:.2f} vs chance {chance:.2f}")
    return out


def neardup(vec_dsn: str) -> dict:
    """Every row's nearest neighbour (HNSW, ef_search 40). Pairs counted once."""
    with _connect(vec_dsn) as vec:
        vec.execute("SET hnsw.ef_search = 40")
        rows = vec.execute("SELECT id, embedding FROM vec.podcasts").fetchall()
        t0 = time.perf_counter()
        pairs = {}
        for qid, qv in rows:
            r = vec.execute("SELECT id, 1 - (embedding <=> %s) FROM vec.podcasts WHERE id <> %s "
                            "ORDER BY embedding <=> %s LIMIT 1", (qv, qid, qv)).fetchone()
            if r:
                pairs[tuple(sorted((qid, r[0])))] = float(r[1])
        secs = time.perf_counter() - t0
        sims = list(pairs.values())
        top = sorted(pairs.items(), key=lambda kv: -kv[1])[:10]
        info = {i: (t, h) for i, t, h in vec.execute(
            "SELECT id, title, host FROM vec.podcasts WHERE id = ANY(%s)", ([i for p, _ in top for i in p],)).fetchall()}
    bands = {f">= {b}": sum(s >= b for s in sims) for b in (0.99, 0.95, 0.90)}
    out = {"rows": len(rows), "distinct_pairs": len(pairs), "seconds": round(secs, 1),
           "pairs_by_similarity": bands,
           "most_similar_pairs": [{"similarity": round(s, 4), "a": info.get(a), "b": info.get(b)} for (a, b), s in top]}
    log(f"  neardup: {len(rows):,} lookups in {secs:.0f}s, pairs {bands}")
    return out


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("step", help="all, or steps separated by commas: sample,embed,index,evaluate,neardup")
    p.add_argument("--src", default=os.environ.get("DISCOVERY_DSN"))
    p.add_argument("--vec", default=os.environ.get("VECTOR_DSN"))
    p.add_argument("--limit", type=int, default=50_000)
    p.add_argument("--out")
    a = p.parse_args(argv)
    known = ("sample", "embed", "index", "evaluate", "neardup")
    steps = known if a.step == "all" else tuple(a.step.split(","))
    if any(s not in known for s in steps):
        sys.exit(f"unknown step in {a.step!r}; known: {', '.join(known)}")
    if not a.vec or ("sample" in steps and not a.src):
        sys.exit("set DISCOVERY_DSN and VECTOR_DSN")
    result = {"machine": {"platform": platform.platform(), "cpus": os.cpu_count()}}
    if "sample" not in steps and a.out and Path(a.out).exists():
        # continuing a run: keep what the earlier steps recorded
        result = {**json.loads(Path(a.out).read_text(encoding="utf-8")), **result}
    t0 = time.perf_counter()
    for s in steps:
        result[s] = (sample(a.src, a.vec, a.limit) if s == "sample" else
                     embed(a.vec) if s == "embed" else
                     index(a.vec) if s == "index" else
                     evaluate(a.vec) if s == "evaluate" else neardup(a.vec))
        if a.out:   # keep what is done so far, in case a later step fails
            Path(a.out).write_text(json.dumps(result, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    result.setdefault("invocations", []).append({"steps": list(steps), "seconds": round(time.perf_counter() - t0, 1)})
    text = json.dumps(result, indent=2, ensure_ascii=False, default=str)
    if a.out:
        Path(a.out).write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
