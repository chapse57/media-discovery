"""The Podcast Index dump (pi.podcasts, 4,731,573 rows) as a small data lake: Parquet on S3, queried by Athena.

    python -m discovery.lake export   # Postgres -> data/lake/podcasts/newest_year=YYYY/part-0.parquet
    python -m discovery.lake upload   # -> s3://<bucket>/media/podcasts/newest_year=YYYY/part-0.parquet
    python -m discovery.lake athena   # DDL + partitions, the 10-02 figures again, scan cost experiments
    python -m discovery.lake all      # the three in order; prints one JSON document

Why partition by the year of the newest episode: "which shows are active" is the question this
data is asked most, and a filter on the partition column lets Athena skip every other year's files.

Every figure that the 10-02 Postgres load recorded (results/2026-10-02-pidump-load.json) is computed
again in Athena over the Parquet files and, in the same run, in Postgres. The run fails if any differ.

Then the same kind of question is asked three ways, to measure what Parquet and partitions buy:
a count (metadata only), a one-column aggregate, and a scan of the largest text column with and
without a partition filter. Athena bills by bytes scanned (USD 5 per TB, 10 MB minimum per query);
each result carries its bytes and that price.

Runs where pyarrow and boto3 are installed (the xmetrics-airflow image has both):
    docker run --rm --env-file ..\\x-metrics-pipeline\\airflow\\.env -e AWS_DEFAULT_REGION=ap-northeast-2 ^
      -e DISCOVERY_DSN=postgresql://xmetrics:xmetrics@host.docker.internal:5432/discovery ^
      --add-host host.docker.internal:host-gateway -v "%cd%":/work -w /work ^
      -e CONNECTION_CHECK_MAX_COUNT=0 xmetrics-airflow:3.3.2 python -m discovery.lake all --out results/2026-10-13-lake.json

Postgres is only read. Nothing in pi or disc changes.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

BUCKET = os.environ.get("LAKE_BUCKET", "ether-xmetrics-lake")
PREFIX = "media/podcasts"
DATABASE = "media_lake"
TABLE = "podcasts"
WORKGROUP = "primary"
RESULTS = f"s3://{BUCKET}/athena-results/"
LOCAL = Path("data/lake/podcasts")
PRICE_PER_TB = 5.0
MIN_BYTES = 10 * 1024 * 1024

# year of the newest episode; 0, NULL and timestamps in the future go to 'unknown'
YEAR_SQL = ("CASE WHEN newest_item_pubdate > 0 AND newest_item_pubdate <= extract(epoch FROM now()) + 86400 "
            "THEN extract(year FROM to_timestamp(newest_item_pubdate))::int::text ELSE 'unknown' END")


def log(*a):
    print(*a, file=sys.stderr, flush=True)


# ------------------------------------------------------------------------------------------ export --
def export(dsn: str, batch: int = 100_000) -> dict:
    import psycopg
    import pyarrow as pa
    import pyarrow.parquet as pq

    arrow = {20: pa.int64(), 23: pa.int32(), 25: pa.string()}
    athena = {20: "bigint", 23: "int", 25: "string"}
    if LOCAL.exists():
        for f in LOCAL.rglob("*.parquet"):
            f.write_bytes(b"")                    # emptied, then overwritten below (no deletes on this folder)
    LOCAL.mkdir(parents=True, exist_ok=True)
    writers, schema, columns, rows_by_year = {}, None, None, {}
    t0 = time.perf_counter()
    done = 0
    with psycopg.connect(dsn) as conn, conn.cursor(name="lake") as cur:
        conn.read_only = True
        cur.itersize = batch
        cur.execute(f"SELECT p.*, {YEAR_SQL} AS newest_year FROM pi.podcasts p")
        while rows := cur.fetchmany(batch):
            if schema is None:
                desc = cur.description
                columns = [(d.name, athena.get(d.type_code, "string")) for d in desc[:-1]]
                schema = pa.schema([(d.name, arrow.get(d.type_code, pa.string())) for d in desc[:-1]])
            groups: dict[str, list] = {}
            for r in rows:
                groups.setdefault(r[-1], []).append(r[:-1])
            for year, part in groups.items():
                cols = list(zip(*part))
                table = pa.table([pa.array(c, type=f.type) for c, f in zip(cols, schema)], schema=schema)
                if year not in writers:
                    d = LOCAL / f"newest_year={year}"
                    d.mkdir(parents=True, exist_ok=True)
                    writers[year] = pq.ParquetWriter(d / "part-0.parquet", schema, compression="snappy")
                writers[year].write_table(table)
                rows_by_year[year] = rows_by_year.get(year, 0) + len(part)
            done += len(rows)
            if done % 500_000 < batch:
                log(f"  export {done:,} rows, {time.perf_counter() - t0:.0f}s")
    for w in writers.values():
        w.close()
    files = sorted(LOCAL.rglob("*.parquet"))
    size = sum(f.stat().st_size for f in files if f.stat().st_size)
    return {"rows": done, "partitions": len(writers), "files": len([f for f in files if f.stat().st_size]),
            "parquet_bytes": size, "seconds": round(time.perf_counter() - t0, 1),
            "rows_by_year": dict(sorted(rows_by_year.items())), "columns": columns}


# ------------------------------------------------------------------------------------------ upload --
def upload() -> dict:
    import boto3
    s3 = boto3.client("s3")
    t0 = time.perf_counter()
    sent, total = 0, 0
    for f in sorted(LOCAL.rglob("*.parquet")):
        if not f.stat().st_size:
            continue
        key = f"{PREFIX}/{f.relative_to(LOCAL).as_posix()}"
        s3.upload_file(str(f), BUCKET, key)
        sent += 1
        total += f.stat().st_size
        log(f"  upload {key} ({f.stat().st_size / 2**20:.1f} MiB)")
    return {"files": sent, "bytes": total, "seconds": round(time.perf_counter() - t0, 1),
            "location": f"s3://{BUCKET}/{PREFIX}/"}


# ------------------------------------------------------------------------------------------ athena --
def athena(client, sql: str, database: str | None = DATABASE, timeout: int = 300) -> dict:
    kw = {"QueryString": sql, "WorkGroup": WORKGROUP, "ResultConfiguration": {"OutputLocation": RESULTS}}
    if database:
        kw["QueryExecutionContext"] = {"Database": database}
    qid = client.start_query_execution(**kw)["QueryExecutionId"]
    t0 = time.time()
    while True:
        q = client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
        state = q["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.time() - t0 > timeout:
            client.stop_query_execution(QueryExecutionId=qid)
            raise TimeoutError(f"Athena query {qid} still {state} after {timeout}s")
        time.sleep(0.5)
    if state != "SUCCEEDED":
        raise RuntimeError(f"Athena {state}: {q['Status'].get('StateChangeReason', '')}\n{sql[:300]}")
    rows = []
    if q.get("StatementType") == "DML":
        res = client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
        rows = [[c.get("VarCharValue") for c in r["Data"]] for r in res][1:]
    st = q.get("Statistics", {})
    scanned = st.get("DataScannedInBytes", 0)
    return {"rows": rows, "scanned_bytes": scanned, "ms": st.get("EngineExecutionTimeInMillis", 0),
            "usd": round(max(scanned, MIN_BYTES) / 1e12 * PRICE_PER_TB, 8) if q.get("StatementType") == "DML" else 0}


def ddl(columns: list[list[str]]) -> str:
    cols = ",\n  ".join(f"`{c}` {t}" for c, t in columns)
    return (f"CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{TABLE} (\n  {cols}\n)\n"
            f"PARTITIONED BY (newest_year string)\nSTORED AS PARQUET\nLOCATION 's3://{BUCKET}/{PREFIX}/'")


# The 10-02 load figures (pidump.load), same SQL on both sides. Athena reads Parquet, Postgres its table.
FIGURES = {
    "rows": ("SELECT count(*) FROM {t}",),
    "dead": ("SELECT count(*) FROM {t} WHERE dead = 1",),
    "with_itunes_id": ("SELECT count(*) FROM {t} WHERE itunes_id > 0",),
    "with_podcast_guid": ("SELECT count(*) FROM {t} WHERE coalesce(podcast_guid, '') <> ''",),
    "itunes_ids_on_more_than_one_row": ("SELECT count(*) FROM (SELECT itunes_id FROM {t} WHERE itunes_id > 0 "
                                        "GROUP BY 1 HAVING count(*) > 1) d",),
    "url_keys_on_more_than_one_row": ("SELECT count(*) FROM (SELECT url_key FROM {t} WHERE url_key IS NOT NULL "
                                      "GROUP BY 1 HAVING count(*) > 1) d",),
    "marked_duplicate_of_another_row": ("SELECT count(*) FROM {t} WHERE duplicate_of > 0",),
    "newest_item": ("SELECT CAST(CAST(from_unixtime(max(newest_item_pubdate)) AS date) AS varchar) FROM {t}",
                    "SELECT to_timestamp(max(newest_item_pubdate))::date::text FROM {t}"),
}

# What Parquet and partitions buy: the same table, four questions.
EXPERIMENTS = {
    "count_rows": "SELECT count(*) FROM {t}",
    "one_int_column_all_years": "SELECT count(DISTINCT itunes_id) FROM {t} WHERE itunes_id > 0",
    "largest_text_column_all_years": "SELECT count(*) FROM {t} WHERE strpos(description, 'artificial intelligence') > 0",
    "largest_text_column_2026_only": "SELECT count(*) FROM {t} WHERE {year} = '2026' AND strpos(description, 'artificial intelligence') > 0",
}


def run_athena(dsn: str, exp: dict) -> dict:
    import boto3
    import psycopg
    client = boto3.client("athena")
    athena(client, f"CREATE DATABASE IF NOT EXISTS {DATABASE}", database=None)
    athena(client, ddl(exp["columns"]))
    athena(client, f"MSCK REPAIR TABLE {DATABASE}.{TABLE}")
    parts = athena(client, f"SELECT newest_year, count(*) FROM {TABLE} GROUP BY 1")
    by_year_athena = {r[0]: int(r[1]) for r in parts["rows"]}

    out = {"figures": [], "experiments": []}
    with psycopg.connect(dsn) as conn:
        conn.read_only = True
        pg_years = dict(conn.execute(f"SELECT {YEAR_SQL}, count(*) FROM pi.podcasts GROUP BY 1").fetchall())
        out["rows_by_year_equal"] = {k: int(v) for k, v in pg_years.items()} == by_year_athena
        for name, sqls in FIGURES.items():
            a_sql, p_sql = sqls[0], (sqls[1] if len(sqls) > 1 else sqls[0])
            a = athena(client, a_sql.format(t=TABLE))
            t = time.perf_counter()
            p = conn.execute(p_sql.format(t="pi.podcasts")).fetchone()[0]
            p_ms = round((time.perf_counter() - t) * 1000)
            av = a["rows"][0][0]
            out["figures"].append({"figure": name, "postgres": str(p), "athena": str(av), "same": str(p) == str(av),
                                   "athena_scanned_bytes": a["scanned_bytes"], "athena_ms": a["ms"], "postgres_ms": p_ms})
            log(f"  {'OK  ' if str(p) == str(av) else 'DIFF'} {name}: postgres {p} athena {av}")
        for name, q in EXPERIMENTS.items():
            a = athena(client, q.format(t=TABLE, year="newest_year"))
            t = time.perf_counter()
            p = conn.execute(q.format(t="pi.podcasts", year=f"({YEAR_SQL})")).fetchone()[0]
            p_ms = round((time.perf_counter() - t) * 1000)
            out["experiments"].append({"question": name, "result_athena": a["rows"][0][0], "result_postgres": str(p),
                                       "same": str(p) == a["rows"][0][0], "athena_scanned_bytes": a["scanned_bytes"],
                                       "athena_ms": a["ms"], "athena_usd": a["usd"], "postgres_ms": p_ms})
            log(f"  {name}: scanned {a['scanned_bytes'] / 2**20:.1f} MiB, {a['ms']} ms, ${a['usd']:.6f}")
    out["agree"] = sum(f["same"] for f in out["figures"])
    out["of"] = len(out["figures"])
    return out


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("step", choices=("export", "upload", "athena", "all"))
    p.add_argument("--dsn", default=os.environ.get("DISCOVERY_DSN"))
    p.add_argument("--out", help="also write the JSON here")
    a = p.parse_args(argv)
    result: dict = {"bucket": BUCKET, "prefix": PREFIX, "database": DATABASE}
    state = LOCAL.parent / "export.json"
    if a.step in ("export", "all"):
        if not a.dsn:
            sys.exit("set DISCOVERY_DSN")
        result["export"] = export(a.dsn)
        state.write_text(json.dumps(result["export"]), encoding="utf-8")
    if a.step in ("upload", "all"):
        result["upload"] = upload()
    if a.step in ("athena", "all"):
        exp = result.get("export") or json.loads(state.read_text(encoding="utf-8"))
        result["athena"] = run_athena(a.dsn, exp)
    text = json.dumps(result, indent=2, ensure_ascii=False)
    if a.out:
        Path(a.out).write_text(text, encoding="utf-8")
    print(text)
    ok = "athena" not in result or (result["athena"]["agree"] == result["athena"]["of"] and result["athena"]["rows_by_year_equal"])
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
