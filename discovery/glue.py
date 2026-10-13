"""Run discovery/glue_job.py on AWS Glue (Spark) and check its numbers against Postgres.

    python -m discovery.glue run --out results/2026-10-13-glue.json

1. upload discovery/glue_job.py to s3://<bucket>/glue-scripts/
2. create the job `xmetrics-podcast-aggregates` (or update it): Glue 5.1, 2 x G.1X, 15-minute timeout,
   role `xmetrics-glue-job` (S3 read of the lake, write of media/aggregates/ only)
3. start a run, wait, record its state, execution time, workers and the DPU cost
4. read the run's summary from S3 and compare it with the same figures computed in Postgres
   (pi.podcasts): row count, duplicate Apple ids, duplicate URL keys, and per year feeds,
   feeds with an Apple id and episodes. Exit 1 if any differ or the run did not succeed.

Same container as discovery.lake (pyarrow is not needed here; boto3 and psycopg are).
Postgres is only read.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from .lake import BUCKET, YEAR_SQL, log

JOB = "xmetrics-podcast-aggregates"
ROLE = "xmetrics-glue-job"
SCRIPT_KEY = "glue-scripts/podcast_aggregates.py"
SOURCE, TARGET = "media/podcasts", "media/aggregates"
GLUE_VERSION, WORKER, WORKERS = "5.1", "G.1X", 2
USD_PER_DPU_HOUR = 0.44           # AWS pricing page example rate; Seoul may differ


def ensure_job(glue, role_arn: str) -> str:
    spec = {
        "Role": role_arn,
        "Command": {"Name": "glueetl", "ScriptLocation": f"s3://{BUCKET}/{SCRIPT_KEY}", "PythonVersion": "3"},
        "DefaultArguments": {"--job-language": "python", "--TempDir": f"s3://{BUCKET}/glue-temp/",
                             "--bucket": BUCKET, "--source": SOURCE, "--target": TARGET},
        "GlueVersion": GLUE_VERSION, "WorkerType": WORKER, "NumberOfWorkers": WORKERS,
        "Timeout": 15, "MaxRetries": 0,
    }
    try:
        glue.get_job(JobName=JOB)
        glue.update_job(JobName=JOB, JobUpdate=spec)
        return "updated"
    except glue.exceptions.EntityNotFoundException:
        glue.create_job(Name=JOB, Description="podcast lake aggregates (media-discovery)", **spec)
        return "created"


def run(dsn: str) -> dict:
    import boto3
    import psycopg

    account = boto3.client("sts").get_caller_identity()["Account"]
    role_arn = f"arn:aws:iam::{account}:role/{ROLE}"
    s3, glue = boto3.client("s3"), boto3.client("glue")
    s3.upload_file(str(Path(__file__).with_name("glue_job.py")), BUCKET, SCRIPT_KEY)
    job_state = ensure_job(glue, role_arn)
    tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = glue.start_job_run(JobName=JOB, Arguments={"--run_tag": tag})["JobRunId"]
    log(f"  glue job {job_state}, run {run_id} started")
    t0 = time.time()
    while True:
        jr = glue.get_job_run(JobName=JOB, RunId=run_id)["JobRun"]
        state = jr["JobRunState"]
        if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR", "EXPIRED"):
            break
        log(f"  {state} {time.time() - t0:.0f}s")
        time.sleep(15)
    secs = jr.get("ExecutionTime", 0)
    dpu_seconds = jr.get("DPUSeconds") or secs * WORKERS          # G.1X = 1 DPU per worker
    out = {"job": JOB, "run_id": run_id, "run_tag": tag, "state": state, "glue_version": jr.get("GlueVersion"),
           "worker_type": jr.get("WorkerType"), "workers": jr.get("NumberOfWorkers"),
           "execution_seconds": secs, "dpu_seconds": dpu_seconds,
           "usd_estimate": round(dpu_seconds / 3600 * USD_PER_DPU_HOUR, 4),
           "wall_seconds_from_start_call": round(time.time() - t0),
           "error": jr.get("ErrorMessage")}
    if state != "SUCCEEDED":
        return out

    summary = json.loads(s3.get_object(Bucket=BUCKET, Key=f"{TARGET}/_summary/{tag}.json")["Body"].read())
    out["spark"] = summary["spark"]
    out["spark_seconds"] = {"read_and_cache": summary["seconds_read_and_cache"], "total": summary["seconds_total"]}
    checks = []
    with psycopg.connect(dsn) as conn:
        conn.read_only = True
        q = lambda s: conn.execute(s).fetchone()[0]  # noqa: E731
        pg = {
            "rows": q("SELECT count(*) FROM pi.podcasts"),
            "itunes_ids_on_more_than_one_row": q("SELECT count(*) FROM (SELECT itunes_id FROM pi.podcasts WHERE itunes_id > 0 "
                                                 "GROUP BY 1 HAVING count(*) > 1) d"),
            "url_keys_on_more_than_one_row": q("SELECT count(*) FROM (SELECT url_key FROM pi.podcasts WHERE url_key IS NOT NULL "
                                               "GROUP BY 1 HAVING count(*) > 1) d"),
        }
        for k, v in pg.items():
            checks.append({"figure": k, "postgres": int(v), "spark": int(summary[k]), "same": int(v) == int(summary[k])})
        years = conn.execute(f"SELECT {YEAR_SQL} AS y, count(*), sum(CASE WHEN itunes_id > 0 THEN 1 ELSE 0 END), "
                             f"sum(episode_count) FROM pi.podcasts GROUP BY 1").fetchall()
        langs = conn.execute("SELECT lower(btrim(language)) AS l, count(*) FROM pi.podcasts GROUP BY 1 "
                             "ORDER BY 2 DESC, 1 LIMIT 10").fetchall()
    pg_years = {y: {"feeds": int(a), "with_itunes_id": int(b), "episodes": int(c or 0)} for y, a, b, c in years}
    sp_years = {y: {k: int(v or 0) for k, v in d.items()} for y, d in summary["by_year"].items()}
    year_cells = [(y, k) for y in pg_years.keys() | sp_years.keys() for k in ("feeds", "with_itunes_id", "episodes")]
    diff_cells = [(y, k) for y, k in year_cells if pg_years.get(y, {}).get(k) != sp_years.get(y, {}).get(k)]
    checks.append({"figure": "per-year feeds / with_itunes_id / episodes", "cells": len(year_cells),
                   "differ": len(diff_cells), "same": not diff_cells, "first_differences": diff_cells[:5]})
    pg_langs = [[l, int(n)] for l, n in langs]
    checks.append({"figure": "top 10 languages", "postgres": pg_langs, "spark": summary["top_languages"],
                   "same": pg_langs == [[l, int(n)] for l, n in summary["top_languages"]]})
    for c in checks:
        log(f"  {'OK  ' if c['same'] else 'DIFF'} {c['figure']}")
    out["checks"] = checks
    out["agree"] = sum(c["same"] for c in checks)
    out["of"] = len(checks)
    out["languages"] = summary["languages"]
    out["output"] = f"s3://{BUCKET}/{TARGET}/"
    return out


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("step", choices=("run",))
    p.add_argument("--dsn", default=os.environ.get("DISCOVERY_DSN"))
    p.add_argument("--out")
    a = p.parse_args(argv)
    if not a.dsn:
        sys.exit("set DISCOVERY_DSN")
    result = run(a.dsn)
    text = json.dumps(result, indent=2, ensure_ascii=False)
    if a.out:
        Path(a.out).write_text(text, encoding="utf-8")
    print(text)
    return 0 if result["state"] == "SUCCEEDED" and result.get("agree") == result.get("of") else 1


if __name__ == "__main__":
    sys.exit(main())
