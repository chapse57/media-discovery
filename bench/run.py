"""Load the synthetic 24-source dataset into Postgres with COPY, build stable keys and indexes,
find duplicate candidates, and time every step. Scores the duplicate query against truth.csv.

FAKE DATA (see generate.py). The timings are real.

    python bench/generate.py --out bench/data
    python bench/run.py --data bench/data --dsn postgresql://postgres:pw@localhost:5432/bench

Writes bench/results.json and prints a Markdown table.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import time
from pathlib import Path

import psycopg

RAW = """
DROP SCHEMA IF EXISTS bench CASCADE;
CREATE SCHEMA bench;
CREATE TABLE bench.raw (
  source_id smallint NOT NULL,
  source_record_id text NOT NULL,
  category text NOT NULL,
  name text NOT NULL,
  url text NOT NULL,
  language text,
  country text,
  last_activity date
);
"""

# One row per input row, plus the two keys every later step uses.
#   url_key : scheme, "www.", query/fragment and trailing slashes removed; host lower-cased,
#             path kept as is (some feed paths are case-sensitive).
#   name_key: lower-case, leading "the " removed, punctuation and runs of spaces collapsed.
NORMALIZE = r"""
CREATE TABLE bench.entry AS
SELECT r.*,
       lower(split_part(u.rest, '/', 1)) || substr(u.rest, length(split_part(u.rest, '/', 1)) + 1) AS url_key,
       btrim(regexp_replace(regexp_replace(lower(r.name), '^\s*the\s+', ''), '[^a-z0-9]+', ' ', 'g')) AS name_key,
       r.last_activity < date '2026-10-02' - interval '2 years' AS stale
FROM bench.raw r,
LATERAL (SELECT regexp_replace(regexp_replace(          -- query first, then the slashes it leaves behind
                  regexp_replace(regexp_replace(r.url, '^[a-zA-Z]+://', ''), '^www\.', '', 'i'),
                  '[?#].*$', ''), '/+$', '') AS rest) u;
"""

DUP_CANDIDATES = """
CREATE TABLE bench.dup_candidate AS
SELECT url_key, count(*) AS rows, count(DISTINCT source_id) AS sources,
       array_agg(source_id || ':' || source_record_id ORDER BY source_id) AS members,
       count(DISTINCT name_key) > 1 AS names_differ
FROM bench.entry
GROUP BY url_key
HAVING count(*) > 1;
"""

NAME_ONLY = """
SELECT count(*) FROM (
  SELECT category, name_key FROM bench.entry GROUP BY category, name_key HAVING count(DISTINCT url_key) > 1
) g;
"""


def timed(conn, label, sql, results, fetch=False):
    t = time.perf_counter()
    with conn.cursor() as cur:
        cur.execute(sql)
        out = cur.fetchall() if fetch else None
    conn.commit()
    results[label] = round(time.perf_counter() - t, 2)
    print(f"{label:<34} {results[label]:>8.2f} s", flush=True)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, default=Path("bench/data"))
    ap.add_argument("--dsn", default=os.environ.get("BENCH_DSN", "postgresql://postgres@localhost:5432/bench"))
    ap.add_argument("--out", type=Path, default=Path("bench/results.json"))
    a = ap.parse_args()
    files = sorted(a.data.glob("source_*.csv"))
    sec, facts = {}, {}
    with psycopg.connect(a.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(RAW)
        conn.commit()

        t = time.perf_counter()
        with conn.cursor() as cur:
            for f in files:
                sid = int(f.stem.split("_")[1])
                cur.execute(f"ALTER TABLE bench.raw ALTER COLUMN source_id SET DEFAULT {sid}")  # tag rows with their file
                with cur.copy("COPY bench.raw (source_record_id, category, name, url, language, country, last_activity) "
                              "FROM STDIN WITH (FORMAT csv, HEADER true)") as cp, open(f, "rb") as fh:
                    while chunk := fh.read(1 << 20):
                        cp.write(chunk)
        conn.commit()
        sec["copy_24_files"] = round(time.perf_counter() - t, 2)
        print(f"{'copy_24_files':<34} {sec['copy_24_files']:>8.2f} s", flush=True)

        timed(conn, "normalize_keys (CTAS)", NORMALIZE, sec)
        timed(conn, "index url_key", "CREATE INDEX entry_url_key ON bench.entry (url_key)", sec)
        timed(conn, "index (category, name_key)", "CREATE INDEX entry_name_key ON bench.entry (category, name_key)", sec)
        timed(conn, "analyze", "ANALYZE bench.entry", sec)
        timed(conn, "duplicate candidates by url_key", DUP_CANDIDATES, sec)
        facts["name_only_collisions"] = timed(conn, "name-only groups (for contrast)", NAME_ONLY, sec, fetch=True)[0][0]

        with conn.cursor() as cur:
            cur.execute("SELECT url_key FROM bench.entry ORDER BY random() LIMIT 1")
            probe = cur.fetchone()[0]
            cur.execute("EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM bench.entry WHERE url_key = %s", (probe,))
            plan = cur.fetchone()[0][0]
            facts["single_lookup_ms"] = round(plan["Execution Time"], 3)
            facts["single_lookup_plan"] = plan["Plan"]["Node Type"]
            for k, q in {
                "rows": "SELECT count(*) FROM bench.entry",
                "sources": "SELECT count(DISTINCT source_id) FROM bench.entry",
                "stale_rows": "SELECT count(*) FROM bench.entry WHERE stale",
                "candidate_groups": "SELECT count(*) FROM bench.dup_candidate",
                "candidate_groups_cross_source": "SELECT count(*) FROM bench.dup_candidate WHERE sources > 1",
                "candidate_groups_renamed": "SELECT count(*) FROM bench.dup_candidate WHERE names_differ",
                "table_mb": "SELECT pg_total_relation_size('bench.entry') / 1048576",
                "server": "SELECT version()",
            }.items():
                cur.execute(q)
                facts[k] = cur.fetchone()[0]
            for s in ("shared_buffers", "maintenance_work_mem", "work_mem", "max_wal_size"):
                cur.execute(f"SHOW {s}")
                facts[s] = cur.fetchone()[0]

            # score against the planted duplicates
            cur.execute("CREATE TEMP TABLE truth (entity bigint, original_source int, original_record_id text,"
                        " dupe_source int, dupe_record_id text, difference text)")
            with cur.copy("COPY truth FROM STDIN WITH (FORMAT csv, HEADER true)") as cp, open(a.data / "truth.csv", "rb") as fh:
                while chunk := fh.read(1 << 20):
                    cp.write(chunk)
            cur.execute("""SELECT count(*), count(*) FILTER (WHERE d.url_key IS NOT NULL)
                           FROM truth t JOIN bench.entry e ON e.source_record_id = t.dupe_record_id
                           LEFT JOIN bench.dup_candidate d ON d.url_key = e.url_key""")
            facts["planted_dupes"], facts["planted_dupes_found"] = cur.fetchone()
            cur.execute("""SELECT count(*) FROM bench.dup_candidate d
                           WHERE NOT EXISTS (SELECT 1 FROM bench.entry e JOIN truth t ON t.dupe_record_id = e.source_record_id
                                             WHERE e.url_key = d.url_key)""")
            facts["candidate_groups_without_planted_dupe"] = cur.fetchone()[0]
            cur.execute("SELECT difference, count(*) FROM truth GROUP BY 1 ORDER BY 2 DESC LIMIT 8")
            facts["top_differences"] = cur.fetchall()

    facts["machine"] = {"os": platform.platform(), "cpus": os.cpu_count(),
                        "ram_gb": round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30, 1)
                        if hasattr(os, "sysconf") else None}
    facts["input_mb"] = round(sum(f.stat().st_size for f in files) / 2**20)
    a.out.write_text(json.dumps({"seconds": sec, "facts": facts}, indent=2, default=str), encoding="utf-8")
    print(json.dumps(facts, indent=2, default=str))


if __name__ == "__main__":
    main()
