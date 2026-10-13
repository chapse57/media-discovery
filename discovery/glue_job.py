"""AWS Glue (Spark) job: aggregate the podcast lake that discovery/lake.py put on S3.

Runs on Glue, not locally (it needs awsglue). discovery/glue.py uploads this file to
s3://<bucket>/glue-scripts/, creates or updates the job, starts it, and checks its numbers
against Postgres.

Reads   s3://<bucket>/media/podcasts/            (Parquet, partitioned by newest_year)
Writes  s3://<bucket>/media/aggregates/by_year/      feeds, with Apple id, episodes per year
        s3://<bucket>/media/aggregates/by_language/  feeds per language (lower-cased, trimmed)
        s3://<bucket>/media/aggregates/_summary/<run_tag>.json   the figures the runner compares
"""
import json
import sys
import time

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["bucket", "source", "target", "run_tag"])
bucket, source, target, tag = args["bucket"], args["source"], args["target"], args["run_tag"]

spark = (SparkSession.builder
         .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")   # '2026' and 'unknown' stay strings
         .getOrCreate())
t0 = time.time()

# only the five columns the questions need; Parquet reads nothing else
df = (spark.read.option("basePath", f"s3://{bucket}/{source}/").parquet(f"s3://{bucket}/{source}/")
      .select("newest_year", "itunes_id", "url_key", "episode_count", "language")
      .cache())
rows = df.count()
t_read = time.time() - t0

by_year = (df.groupBy("newest_year")
           .agg(F.count(F.lit(1)).alias("feeds"),
                F.sum(F.when(F.col("itunes_id") > 0, 1).otherwise(0)).alias("with_itunes_id"),
                F.sum("episode_count").alias("episodes"))
           .orderBy("newest_year"))
by_language = (df.select(F.lower(F.trim(F.col("language"))).alias("language"))
               .groupBy("language").agg(F.count(F.lit(1)).alias("feeds"))
               .orderBy(F.desc("feeds"), F.asc("language")))
dup_itunes = df.filter(F.col("itunes_id") > 0).groupBy("itunes_id").count().filter("count > 1").count()
dup_url = df.filter(F.col("url_key").isNotNull()).groupBy("url_key").count().filter("count > 1").count()

by_year.write.mode("overwrite").parquet(f"s3://{bucket}/{target}/by_year/")
by_language.write.mode("overwrite").parquet(f"s3://{bucket}/{target}/by_language/")

summary = {
    "run_tag": tag,
    "spark": spark.version,
    "rows": rows,
    "input_partitions": df.rdd.getNumPartitions(),
    "itunes_ids_on_more_than_one_row": dup_itunes,
    "url_keys_on_more_than_one_row": dup_url,
    "by_year": {r["newest_year"]: {"feeds": r["feeds"], "with_itunes_id": r["with_itunes_id"],
                                   "episodes": r["episodes"]} for r in by_year.collect()},
    "top_languages": [[r["language"], r["feeds"]] for r in by_language.limit(10).collect()],
    "languages": by_language.count(),
    "seconds_read_and_cache": round(t_read, 1),
    "seconds_total": round(time.time() - t0, 1),
}
boto3.client("s3").put_object(Bucket=bucket, Key=f"{target}/_summary/{tag}.json",
                              Body=json.dumps(summary).encode("utf-8"), ContentType="application/json")
print(json.dumps({k: v for k, v in summary.items() if k != "by_year"}))
