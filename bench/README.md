# 6.58M-row load and duplicate benchmark

**The data is synthetic.** `generate.py` makes it up from a fixed seed. The timings are real.

The question this answers: if 24 directories hand over 6.58 million records of podcasts,
journalists, YouTube channels and the like, with the same entity listed more than once, how
long does it take to load them into Postgres, give every row a stable key, index it, and list
the duplicate candidates? And does the duplicate query actually find the duplicates?

## Result (2026-10-02)

| Step | Time |
|---|---:|
| `COPY` 24 CSV files (553 MB) into `bench.raw` | 11.2 s |
| Build `url_key` and `name_key` for every row (`CREATE TABLE AS`) | 51.4 s |
| Index on `url_key` | 14.1 s |
| Index on `(category, name_key)` | 11.3 s |
| `ANALYZE` | 0.3 s |
| Duplicate candidates: group by `url_key`, more than one row | 23.6 s |
| One lookup by `url_key` ("do we already have this show?"), index scan | 0.072 ms |

| Count | Value |
|---|---:|
| Rows loaded | 6,580,000 |
| Sources | 24 |
| Rows with last activity more than 2 years before 2026-10-02 | 986,387 |
| Planted duplicates (same entity, different source) | 580,000 |
| Planted duplicates found by the `url_key` query | **580,000** |
| Candidate groups that are not a planted duplicate | **0** |
| Duplicate groups where the name differs between sources | 58,131 |
| Groups of *different* entities a name-only match would merge | 538,284 |
| Table size with indexes | 1,386 MB |

Machine: cloud Linux container, 2 vCPU (Intel Xeon 2.1 GHz), 7.8 GB RAM. PostgreSQL 16.13,
`shared_buffers=1GB`, `maintenance_work_mem=1GB`, `work_mem=4MB`, `max_wal_size=4GB`.
Not tuned beyond that, no parallel load.

## What the numbers do and do not show

- **0 false candidates is by construction.** Every synthetic entity has its own URL, so two
  different entities never share a `url_key`. Real directories do: shared hosting pages,
  redirect URLs, a network's feed that lists several shows. The recall number (580,000 of
  580,000) is the meaningful one: the key survives `http`/`https`, `www.`, trailing slashes,
  tracking queries, host letter case, and renames.
- **Name matching is not enough.** 538,284 groups of different entities share a category and a
  normalized name ("Daily Tech", "Morning Brief"). That is why names only go to review, and the
  stable key is the URL (for podcasts, the `podcast:guid` where the feed has one).
- The slowest step is computing the keys with regular expressions (51 s). Done at load time per
  batch, it disappears into the load.
- The first version of the key had a bug: removing `?utm_source=...` left the trailing `/`
  behind, so 698 of 5,800 planted duplicates were missed at 1% scale. Scoring against the
  planted truth caught it; timing alone would not have.

## Run it

```
python -m venv .venv && .venv\Scripts\activate          # Windows; source .venv/bin/activate elsewhere
pip install "psycopg[binary]"
python bench/generate.py --out bench/data               # 6.58M rows, ~550 MB, ~70 s
docker run -d --name benchpg -e POSTGRES_PASSWORD=pw -p 5433:5432 postgres:16 ^
  -c shared_buffers=1GB -c maintenance_work_mem=1GB -c max_wal_size=4GB
docker exec benchpg createdb -U postgres bench
python bench/run.py --data bench/data --dsn postgresql://postgres:pw@localhost:5433/bench
```

`--scale 0.01` on `generate.py` gives 65,800 rows for a quick try. Results go to
`bench/results.json`.

Tests (`tests/test_bench.py`, needs `BENCH_DSN`): a 13,160-row run must find all 1,160 planted
duplicates and no others, and the URL key is checked on hand-written edge cases using the same
SQL the benchmark runs.
