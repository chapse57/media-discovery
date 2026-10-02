# media-discovery

Two things for building a media directory (podcasts first) on Postgres:

1. **`discovery/`**: a podcast discovery run. It takes shows from official catalogues
   (Podcast Index API, Apple iTunes Search and Lookup) and their RSS feeds, and keeps **one row per show
   however often the show moves or renames**. New shows, changes and doubtful matches go to a
   review table instead of being decided silently.
2. **`bench/`**: a 6.58-million-row load and duplicate benchmark on synthetic data
   (24 sources, COPY, stable keys, indexes, duplicate candidates scored against planted truth).
   Results and machine spec: [bench/README.md](bench/README.md).

No web page scraping. Only the Podcast Index API, Apple's public Search and Lookup APIs and the RSS feeds
publishers put out.

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
