# media-discovery

Two things for building a media directory (podcasts first) on Postgres:

1. **`discovery/`**: a podcast discovery run. It takes shows from official catalogues
   (Podcast Index API, Apple iTunes Search and Lookup) and their RSS feeds, and keeps **one row per show
   however often the show moves or renames**. New shows, changes and doubtful matches go to a
   review table instead of being decided silently.
2. **`bench/`**: a 6.58-million-row load and duplicate benchmark on synthetic data
   (24 sources, COPY, stable keys, indexes, duplicate candidates scored against planted truth).
   Results and machine spec: [bench/README.md](bench/README.md).
3. **`discovery/pidump.py`**: Podcast Index's **public database dump** (one SQLite file, no key,
   no signup) copied into Postgres, and our shows matched against it. This is where the
   real-data numbers come from; see [Podcast Index public dump](#podcast-index-public-dump).

No web page scraping. Only the Podcast Index API and its public database dump, Apple's public
Search and Lookup APIs, and the RSS feeds publishers put out.

**Podcast Index API: built and tested, not run live.** Its signup does not accept free email
providers (Gmail, Outlook and similar), so there is no key. The API client and its auth header are
covered by the tests against a local fake server. Real Podcast Index data comes from the public dump
instead (below).

## How a show stays one show

Identifiers, strongest first: `podcast:guid` → Podcast Index id → Apple id → **any URL the show has
ever had**. A listing or a feed is the same show when one of these matches and none conflict.

| Situation | What happens |
|---|---|
| Feed answers **301/308** | New URL is stored as an alias; the show is fetched there from now on; `moved` review |
| Feed answers **302/307** | Followed for this fetch only; stored URL unchanged (temporary) |
| Feed has **`itunes:new-feed-url`** | Same as 301: alias, refetch there, `moved` review |
| Catalogue lists a known **guid at a new URL** (host change, rebrand) | Linked to the existing show; URL kept as an alias |
| Apple's record for the show has a **different feed URL** | Kept as an alias (`via = itunes`) so a later listing at that URL matches |
| Two identifiers point at **different shows** | Nothing merged; `ambiguous` review |
| **Same title and author**, no shared id or URL | Nothing merged or created; `ambiguous` review, held until a person resolves it |
| Feed title changes | `renamed` review |
| Last episode older than 90 days | `active = false`; `went_inactive` review the first time |
| Feed unchanged | `ETag` / `Last-Modified` sent; a 304 costs no parse |

Every run writes a `disc.run` row and, per source (`podcastindex`, `itunes`, `rss`), requests,
200s, 304s, redirects, errors and total time to `disc.source_log`. An open review is not asked
again on the next run.

## Tests

`tests/test_pipeline.py` runs the whole thing against one loopback server that plays RSS hosts,
Podcast Index and iTunes (real HTTP: 301, 302, 304, ETag), on a real Postgres:
seed run, re-run (0 new, conditional GETs, no duplicate aliases), new show + rebrand by guid +
same-title lookalike, conflicting identifiers, Podcast Index auth header checked byte for byte.
`tests/test_bench.py` scores the benchmark's duplicate query at small scale.
`tests/test_pidump.py` loads a fake dump (camelCase columns, a dirty integer, a NUL byte) and checks the
match order, URL normalisation, conflicting keys, duplicate dump rows, dead and moved feeds.
CI runs all of them against Postgres 16 and fails on any skip.

## Run it live (Windows, the x-metrics Postgres container)

```
cd /d C:\Users\PC\Desktop\Upwork\claude-work\media-discovery
python -m venv .venv
.venv\Scripts\activate
pip install -e .[dev]
copy env.example.cmd env.cmd          & rem then put your Podcast Index key and secret in env.cmd
call env.cmd
docker compose -f ..\x-metrics-pipeline\docker-compose.yml up -d postgres
docker compose -f ..\x-metrics-pipeline\docker-compose.yml exec postgres createdb -U xmetrics discovery
python -m discovery run --trending 300 --note seed > run1.json
python -m discovery run --trending 300 --note rerun > run2.json
```

Without a Podcast Index key the run seeds from Apple's Search API instead (`--source auto`, the
default, picks Podcast Index when `PODCASTINDEX_KEY` and `PODCASTINDEX_SECRET` are set). The
show's `podcast:guid` then comes from its own RSS feed.

## Live results (2026-10-02, Windows PC, Postgres 16 in Docker, 8 parallel feed fetches)

Seeded from Apple's Search API (no Podcast Index key yet): the first 300 distinct shows from four
searches (news, technology, business, true crime; US store), then Apple Lookup for all 300 ids
(2 calls). Then the same command again. Raw output: [`results/`](results/).

| | Run 1 (seed) | Run 2 (same command again) |
|---|---:|---:|
| Shows in the store | 300 | 300 |
| New shows | (seed) | **0** |
| Feeds fetched | 300 | 300 |
| ... full download (200) | 294 | 35 |
| ... not modified (304, ETag / Last-Modified) | 0 | **259** |
| ... errors (unreachable, 4xx/5xx) | 6 | 6 |
| ... answered with a redirect | 7 | 11 |
| Shows moved by `itunes:new-feed-url`, linked to the same row | **4** | 0 (already linked) |
| Permanent redirects to a *different* address | 0 | 0 |
| Review items opened | 4 moved, 2 title differs from Apple's, 1 ambiguous | **0** |
| Feed fetch time, summed over the 8 workers | 1,372 s | 165 s |
| Apple API calls | 6 | 6 |

Of the 300 shows, 261 had an episode in the last 90 days, and only 38 (13%) carry `podcast:guid`
in their feed, so for most shows the URL history and the Apple id are what keep them one row.
The 7 to 11 redirects were all either temporary or to the same address (for example `http` to
`https`), which the URL key already treats as one. Run 2 took about a minute. Run 1's own
`seconds` field said 17 s; that was a bug (the finish time was the transaction start), fixed after this
run. Its real length was roughly 3 to 4 minutes: the feed fetches alone add up to 1,372 s over 8 workers.

The "title differs" items are Apple's listing name not matching the feed's `<title>`, not renames
over time. A second run a week later is what shows real renames and moves.

## Podcast Index public dump

Podcast Index publishes its whole feed table as one SQLite file:
`https://public.podcastindex.org/podcastindex_feeds.db.tgz` (linked from
[Podcastindex-org/database](https://github.com/Podcastindex-org/database), whose contents are MIT
licensed; the index itself is offered "free, for any use"). The file is not committed here (`data/` is
git-ignored).

```
python -m discovery.pidump load --db data\podcastindex_feeds.db > results\pidump-load.json
python -m discovery.pidump match > results\pidump-match.json
```

`load` copies every column into `pi.podcasts` with `COPY`, adds `url_key` / `original_url_key`
(computed with the same `feeds.url_key` as the discovery run), then builds the primary key and four
lookup indexes, timing each step. A value Postgres would reject (a non-number in an integer column,
a NUL byte in text) becomes NULL or is cleaned, and is counted, not guessed.

`match` looks up each of our shows by Apple id, then `podcast:guid`, then every URL the show has
ever had (against both the dump's current and original URL). If two keys point at different dump
rows, the show is reported as ambiguous instead of picking one by priority. Nothing in `disc` is
changed; per-show results go to `pi.match`. `tests/test_pidump.py` checks all of this on a tiny
fake dump.

### Results (2026-10-02, real data, Windows PC, Postgres 16.15 in Docker)

Machine: Windows (build 10.0.26200), AMD, 12 logical CPUs, 31 GB RAM; Postgres in Docker Desktop on the
same PC. Raw output: [`results/2026-10-02-pidump-load.json`](results/2026-10-02-pidump-load.json),
[`results/2026-10-02-pidump-match.json`](results/2026-10-02-pidump-match.json) and the two CSVs next to them.

**Load**

| | |
|---|---:|
| Download (`.tgz`, 1,828,580,952 bytes) | 468 s |
| Unpacked SQLite file (dated 2026-09-27, newest episode 2026-09-26) | 5,097,177,088 bytes |
| Rows in the dump's `podcasts` table / rows in Postgres | **4,731,573 / 4,731,573** |
| Columns kept | 42 (+ 2 URL keys) |
| `COPY` (Python reads SQLite, computes the URL keys, streams to Postgres) | **100.4 s** (47,112 rows/s) |
| Primary key + 4 indexes (`itunes_id`, `podcast_guid`, `url_key`, `original_url_key`) | **34.3 s** (5.6 + 1.5 + 6.0 + 10.5 + 10.7) |
| Table + indexes in Postgres | 6,327 MB |
| Values Postgres would have rejected | 0 |

Inside the dump: 2,970,036 feeds carry an Apple id; 1,656 Apple ids sit on more than one row and
3,756 URL keys do too; 2,769 rows are marked `duplicateOf` another. Every row has `dead = 0`, so the
dump's dead flag says nothing here (the export either leaves dead feeds out or does not set it); our own
90-day activity check is what judges liveness.

**Our 300 Apple-seeded shows against it**

| | Shows |
|---|---:|
| Matched | **278** |
| ... by Apple id | 274 |
| ... by `podcast:guid` (no Apple id hit) | 1 |
| ... by a URL the show has had (no id hit) | 3 |
| Ambiguous: two keys point at two different dump rows, not merged | **8** |
| Not in the dump | **14** |
| Matched, but the dump has the feed at a different address | 40 (32 another host, 8 same host) |
| Matched to more than one dump row | 0 |

- **The 8 ambiguous ones are duplicates inside the dump**, not our mistakes: the row with our Apple id and the
  row at our feed URL are evidently the same show, and neither is marked `duplicateOf`. In 7 of 8 the two rows have the same
  title (or a near one), episode count and newest episode; the eighth (Bloomberg Surveillance) is an old feed that
  stopped in 2021 next to its current one. In four, the two rows even carry two different Apple ids. A person
  would merge them; the code does not, on purpose.
- **The 14 not found:** 12 have Apple ids of 6.80 billion or more (recent listings; the dump is from 2026-09-27).
  Of the other two, one feed now answers 404 and one (Sharp Tech) is live but simply not in the dump.
  Three share a title with a different, older show in the dump; for example our
  "Lex Fridman Podcast" is a Spreaker feed with Apple id 6814008109, while the dump's is `lexfridman.com` with Apple
  id 1434243584. A title-only match would have merged two different shows.
- **The 40 address differences are candidates, not confirmed moves.** Neither side was re-fetched to see which
  address is current. The most common pairs: Dow Jones' own feed vs Simplecast (5), Simplecast vs NBC News (4),
  `rss.pdrl.fm` vs Megaphone (4), and `rss.libsyn.com` vs `<show>.libsyn.com` (two Libsyn address
  forms for one feed, 6).

## The dump as a data lake: Parquet on S3, queried by Athena

`discovery/lake.py` reads `pi.podcasts` (the table above) and writes it as Parquet, one file per
year of each feed's newest episode, uploads it to S3 (ap-northeast-2), creates the Athena table by
DDL (no Glue crawler), and asks Athena and Postgres the same questions in the same run. It exits
non-zero if any answer differs. Postgres is only read.

    docker run --rm --env-file ..\x-metrics-pipeline\airflow\.env -e AWS_DEFAULT_REGION=ap-northeast-2 ^
      -e DISCOVERY_DSN=postgresql://xmetrics:xmetrics@host.docker.internal:5432/discovery ^
      --add-host host.docker.internal:host-gateway -e CONNECTION_CHECK_MAX_COUNT=0 -v "%cd%":/work -w /work ^
      xmetrics-airflow:3.3.2 python -m discovery.lake all --out results/2026-10-13-lake.json

(The x-metrics Airflow image is reused because it already has pyarrow and boto3; the AWS keys are
those of the IAM user `xmetrics-pipeline`, whose policy is `x-metrics-pipeline/aws/iam-policy-xmetrics-lake.json`.)

### Results (2026-10-13, the same PC)

| step | result |
|---|---|
| Postgres → Parquet | 4,731,573 rows, 44 columns, **2,564,977,092 bytes** (snappy) in 56 partitions (`newest_year=1970` … `2026`, `unknown`), 566 s |
| → S3 | 56 files, 225 s, `s3://ether-xmetrics-lake/media/podcasts/` |
| Athena == Postgres | the eight 10-02 load figures **8 of 8 equal** (rows 4,731,573 · with Apple id 2,970,036 · Apple ids on >1 row 1,656 · URL keys on >1 row 3,756 · `duplicateOf` set 2,769 · dead 0 · with podcast:guid 4,731,428 · newest item 2026-09-26), and the row count of every one of the 56 partitions equal |

What columnar files and a partition buy, measured (Athena bills USD 5 per TB scanned, 10 MB minimum):

| question (same 2.56 GB table) | Athena scanned | Athena | Postgres | answer (both) |
|---|---|---|---|---|
| `count(*)` | 0 bytes (Parquet metadata) | 0.7 s | 0.1 s | 4,731,573 |
| distinct `itunes_id` | 35.4 MB (1.4 %) | 2.2 s | 0.2 s | 2,968,370 |
| `description` contains "artificial intelligence", all years | 813.1 MB (32 %) | 1.6 s | 0.7 s | 3,951 |
| the same, `newest_year = '2026'` only | 177.2 MB (−78 % vs all years) | 1.1 s | 0.8 s | 1,546 |

Read honestly: on this PC Postgres answers faster — the table fits in 31 GB of RAM and has indexes,
and every Athena query pays about a second of start-up. Athena's case is no server and paying for
bytes read; the table shows how much Parquet (read only the columns asked for) and a partition
filter (skip the other years) cut those bytes. Not done: incremental loads, a Glue crawler.

## The same lake, aggregated by a Spark job on AWS Glue

`discovery/glue_job.py` is a PySpark script for AWS Glue. `discovery/glue.py` uploads it to S3,
creates (or updates) the Glue job `xmetrics-podcast-aggregates`, starts one run, waits for it, then
computes the same figures in Postgres and compares them. Exit 1 if the run fails or any figure differs.

    docker run --rm --env-file ..\x-metrics-pipeline\airflow\.env -e AWS_DEFAULT_REGION=ap-northeast-2 ^
      -e DISCOVERY_DSN=postgresql://xmetrics:xmetrics@host.docker.internal:5432/discovery ^
      --add-host host.docker.internal:host-gateway -v "%cd%":/work -w /work ^
      xmetrics-airflow:3.3.2 python -m discovery.glue run --out results/2026-10-13-glue.json

The Spark job reads only five of the 44 columns from `s3://…/media/podcasts/` (partition
columns kept as strings, so `2026` and `unknown` stay what they are) and writes
`media/aggregates/by_year/` (feeds, feeds with an Apple id, episodes per year),
`media/aggregates/by_language/` (feeds per lower-cased, trimmed language tag) and a JSON summary.

Permissions are split in two (files in `x-metrics-pipeline/aws/`):
- the job runs as the IAM role `xmetrics-glue-job`: it can read the podcast files and the script,
  and write only `media/aggregates/` and `glue-temp/`;
- the IAM user `xmetrics-pipeline` that starts the job can hand over that one role and no other
  (`iam:PassRole` on `role/xmetrics-glue-job`, only to `glue.amazonaws.com`).

### Results (2026-10-13)

| | |
|---|---|
| run | `SUCCEEDED`, Glue 5.1 (Spark 3.5.6-amzn-1), 2 × G.1X |
| time | 142 s billed execution (inside the script: 58.6 s to read and cache 4.7 M rows, 73.1 s total); 166 s from start call to finish |
| cost | 284 DPU-seconds ≈ **USD 0.035** at USD 0.44 per DPU-hour (AWS's example rate; the Seoul rate may differ) |
| Spark == Postgres | **5 of 5**: rows 4,731,573 · Apple ids on >1 row 1,656 · URL keys on >1 row 3,756 · per-year feeds / with Apple id / episodes, 168 cells, 0 different · top 10 languages with counts identical |
| output | 952 language groups, 56 year rows (`by_year`, `by_language` Parquet in `s3://ether-xmetrics-lake/media/aggregates/`) |
| console | `results/glue-run-2026-10-13.png` — the run in the Glue console: Succeeded, 0 retries, 8 s start-up, 2 min 22 s execution, 0.078889 DPU-hours |

**The same aggregates in Postgres** (`python -m discovery.pg_aggregates`, read only,
`results/2026-10-13-pg-aggregates.json`): the five questions the Spark job answers, three passes in one
connection, answers checked against the Spark run.

| | Postgres 16.15 (Docker, this PC, default settings) | Glue Spark (2 × G.1X) |
|---|---|---|
| the five aggregates | **3.2 s** (3.243 / 3.314 / 3.215) | 73.1 s inside the script (58.6 s of it reading and caching from S3) |
| billed | — | 142 s execution, ≈ USD 0.035 |
| answers | same as Spark: rows, 56 years, 952 languages, 1,656 and 3,756 duplicates | — |

Not the same work: Postgres read its own pages, and three of the five queries were Index Only Scans;
Spark pulled Parquet over the network from S3 and wrote its results back. Nor a cold cache: the first
pass was as fast as the others, so the 5.3 GB table was already in the PC's memory (shared_buffers is
the default 128 MB, so mostly the OS cache). What it shows is narrow: at 2.5 GB, on one PC with 31 GB
of RAM, Spark is the wrong tool — Postgres on this PC holds the whole table.
The point of this run is the shape: a Spark job on Glue reading partitioned Parquet from S3,
with a least-privilege role, and its output checked against an independent source before anyone
uses it. Not scheduled, not incremental, no Glue crawler.

**From Airflow.** The same three steps (`glue.start` / `glue.wait` / `glue.compare`) run as the DAG
`media_lake_glue` in `x-metrics-pipeline/airflow` (start never retried, so a retry cannot pay for a
second Spark run). Its first run, 2026-10-13 13:52 KST: 2 min 25 s for the whole DAG, 127 s Glue
execution, 255 DPU-seconds ≈ USD 0.031, Spark == Postgres **5 of 5** again. The first run above was
started from the command line.

## Shows like this one: pgvector over 50,000 descriptions

`discovery/vectors.py` turns podcast descriptions into vectors with a local model and searches them in
Postgres with pgvector. It runs in its own containers (`vector/docker-compose.yml`: `pgvector/pgvector:0.8.0-pg16`
on port 5434, and a CPU-only Python image), so the Postgres that holds `discovery` keeps its image and settings;
`discovery` is only read.

    docker compose -f vector/docker-compose.yml up -d --build vectordb
    docker compose -f vector/docker-compose.yml build embed
    docker compose -f vector/docker-compose.yml run --rm embed python -m discovery.vectors all --out results/2026-10-13-vectors.json

Sample: 50,000 of the 247,972 dump rows that were active in 2026, in English (`en*`), have a description of
at least 200 characters, an Apple id, and are neither dead nor marked `duplicateOf` — taken by `md5(id)`,
not by popularity. Text = title + first 1,500 characters of the description; model
`sentence-transformers/all-MiniLM-L6-v2` (384 dimensions, normalised, CPU).

### Results (2026-10-13, 진우's PC)

| step | result |
|---|---|
| embed | 50,000 rows in 872 s (**57 rows/s** — torch used 6 threads of 12; not tuned) |
| HNSW index | cosine, m 16, ef_construction 64: **6.3 s** to build, 98 MB (table 143 MB) |
| exact top 10 | sequential scan + sort: p50 **74.5 ms**, p95 85.4 ms |
| HNSW top 10, ef_search 40 | p50 **0.94 ms**, p95 2.32 ms, **recall@10 0.964** (154 of 200 queries all 10 right, worst 5 of 10) |
| HNSW top 10, ef_search 100 | p50 1.34 ms, p95 2.11 ms, **recall@10 0.980** (170 of 200, worst 6 of 10) |
| does it find similar shows? | 46.0 % of the exact top 10 share the query's first category, against **8.6 %** if neighbours were random (199 queries) |
| near-duplicates | each row's nearest neighbour (50,000 HNSW lookups, 36 s): 84 pairs at cosine ≥ 0.99, 159 ≥ 0.95, 243 ≥ 0.90 |

Recall is measured, not assumed: 200 sample shows are the queries, exact search is the answer key, and
the plans that actually ran are in the result file (`EXPLAIN ANALYZE`: Seq Scan + top-N heapsort for exact,
Index Scan using `podcasts_embedding_hnsw` for HNSW).

**The first evaluation was wrong, and how it showed.** It reported HNSW at 71 ms — the same as exact
search — and recall exactly 1.000 at both ef_search values. psycopg prepares a query after five runs; the
exact loop (index scans off) had prepared the same query text, and the HNSW loops reused that cached
sequential-scan plan, so they were measuring exact search twice. Fixed by planning every execution
(`prepare_threshold=None`) and giving exact and HNSW their own connections; the script now records
`index_used_check` and flags a run where HNSW is not clearly faster. The wrong run is kept as
`results/2026-10-13-vectors-run1-cached-plan.json`.

Read honestly: 50,000 rows is a sample (the eligible set is 247,972; the dump 4.7 M). The category check
is a weak label — Podcast Index's first category, and many shows have none or a broad one — so it shows
the neighbours are far from random, not that they are right. The near-duplicate pairs are candidates:
most of the top ten have the same title and the same or a moved host, but none was checked by hand or
against Apple ids. No tests run the model in CI.
