"""Time, in Postgres, the aggregates the Glue Spark job computes (discovery/glue_job.py).

    python -m discovery.pg_aggregates --out results/2026-10-13-pg-aggregates.json

Same five questions over pi.podcasts (4.7 M rows): row count, per-year feeds / feeds with an
Apple id / episodes, feeds per language, Apple ids on more than one row, URL keys on more than one
row. Each pass runs all five in one connection; three passes, so the first (colder cache) and the
others can be told apart. The answers are checked against the Spark run in results/2026-10-13-glue.json.

Read only. Not the same work as the Spark job: Postgres reads its own local pages (and may use
indexes); Spark read Parquet from S3 and also wrote its results back to S3.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import statistics
import sys
import time
from pathlib import Path

from .lake import YEAR_SQL, log

QUERIES = {
    "rows": "SELECT count(*) FROM pi.podcasts",
    "by_year": f"SELECT {YEAR_SQL} AS y, count(*), sum(CASE WHEN itunes_id > 0 THEN 1 ELSE 0 END), "
               f"sum(episode_count) FROM pi.podcasts GROUP BY 1",
    "by_language": "SELECT lower(btrim(language)) AS l, count(*) FROM pi.podcasts GROUP BY 1 ORDER BY 2 DESC, 1",
    "itunes_ids_on_more_than_one_row": "SELECT count(*) FROM (SELECT itunes_id FROM pi.podcasts WHERE itunes_id > 0 "
                                       "GROUP BY 1 HAVING count(*) > 1) d",
    "url_keys_on_more_than_one_row": "SELECT count(*) FROM (SELECT url_key FROM pi.podcasts WHERE url_key IS NOT NULL "
                                     "GROUP BY 1 HAVING count(*) > 1) d",
}
SETTINGS = ("server_version", "shared_buffers", "work_mem", "max_parallel_workers_per_gather", "max_parallel_workers")


def one_pass(conn) -> tuple[dict, dict]:
    seconds, answers = {}, {}
    for name, sql in QUERIES.items():
        t = time.perf_counter()
        rows = conn.execute(sql).fetchall()
        seconds[name] = round(time.perf_counter() - t, 3)
        answers[name] = rows
    seconds["total"] = round(sum(seconds.values()), 3)
    return seconds, answers


def plan(conn, sql: str) -> list[str]:
    """The plan's node types (Seq Scan / Index Only Scan / Gather ...), from EXPLAIN without ANALYZE."""
    lines = [r[0] for r in conn.execute("EXPLAIN " + sql).fetchall()]
    return [l.strip().lstrip("-> ").split("  ")[0] for l in lines if "Scan" in l or "Gather" in l or "Aggregate" in l]


def run(dsn: str, passes: int = 3, spark_file: str | None = None) -> dict:
    import psycopg

    with psycopg.connect(dsn, connect_timeout=10) as conn:
        conn.read_only = True
        settings = {s: conn.execute(f"SHOW {s}").fetchone()[0] for s in SETTINGS}
        size = conn.execute("SELECT pg_size_pretty(pg_table_size('pi.podcasts')), "
                            "pg_size_pretty(pg_total_relation_size('pi.podcasts'))").fetchone()
        plans = {n: plan(conn, q) for n, q in QUERIES.items()}
        runs, answers = [], None
        for i in range(passes):
            s, answers = one_pass(conn)
            log(f"  pass {i + 1}: {s['total']} s  {s}")
            runs.append(s)
    out = {
        "machine": {"python_platform": platform.platform(), "note": "Postgres runs in Docker on the same PC"},
        "postgres": settings,
        "table_size": size[0], "table_plus_indexes": size[1],
        "plans": plans,
        "passes": runs,
        "total_seconds": {"first": runs[0]["total"],
                          "median": statistics.median(r["total"] for r in runs),
                          "min": min(r["total"] for r in runs)},
        "answers": {"rows": answers["rows"][0][0],
                    "years": len(answers["by_year"]),
                    "languages": len(answers["by_language"]),
                    "itunes_ids_on_more_than_one_row": answers["itunes_ids_on_more_than_one_row"][0][0],
                    "url_keys_on_more_than_one_row": answers["url_keys_on_more_than_one_row"][0][0]},
    }
    if spark_file and Path(spark_file).exists():
        sp = json.loads(Path(spark_file).read_text(encoding="utf-8"))
        figures = {c["figure"]: c for c in sp["checks"]}
        out["same_as_spark"] = {
            "rows": out["answers"]["rows"] == figures["rows"]["spark"],
            "itunes_ids_on_more_than_one_row": out["answers"]["itunes_ids_on_more_than_one_row"]
                                               == figures["itunes_ids_on_more_than_one_row"]["spark"],
            "url_keys_on_more_than_one_row": out["answers"]["url_keys_on_more_than_one_row"]
                                             == figures["url_keys_on_more_than_one_row"]["spark"],
            "years": out["answers"]["years"] * 3 == figures["per-year feeds / with_itunes_id / episodes"]["cells"],
            "languages": out["answers"]["languages"] == sp["languages"],
        }
        out["spark_for_reference"] = {"execution_seconds": sp["execution_seconds"],
                                      "inside_script": sp["spark_seconds"], "usd_estimate": sp["usd_estimate"]}
    return out


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--dsn", default=os.environ.get("DISCOVERY_DSN"))
    p.add_argument("--passes", type=int, default=3)
    p.add_argument("--spark", default="results/2026-10-13-glue.json")
    p.add_argument("--out")
    a = p.parse_args(argv)
    if not a.dsn:
        sys.exit("set DISCOVERY_DSN")
    result = run(a.dsn, a.passes, a.spark)
    text = json.dumps(result, indent=2, ensure_ascii=False, default=str)
    if a.out:
        Path(a.out).write_text(text, encoding="utf-8")
    print(text)
    return 0 if all(result.get("same_as_spark", {}).values()) else 1


if __name__ == "__main__":
    sys.exit(main())
