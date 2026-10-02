"""Generate a synthetic media-directory dataset shaped like a multi-source discovery job.

FAKE DATA. Every name, URL and date here is made up by a seeded random generator.
It exists to measure load and dedup-query times at a realistic size, nothing else.

Shape:
  - 24 input files (one per source), 6,580,000 rows total
  - categories: podcast, journalist, youtube, newsletter, blog, radio
  - 6,000,000 distinct entities + 580,000 planted duplicates: the same entity appearing
    again in a *different* source with the kinds of differences real directories have
    (http vs https, www, trailing slash, tracking query, letter case, "The " prefix,
    punctuation, extra spaces). Some duplicates are renamed (same URL, new name).
  - about 15% of rows have a last-activity date more than two years old
  - names come from a small vocabulary, so different entities often share a name.
    That is on purpose: a name-only match would merge them; the URL key should not.

The ground truth (which rows are planted duplicates of which entity) is written to
truth.csv so the dedup query can be scored, not just timed.

    python bench/generate.py --out bench/data            # ~1 GB of CSV
    python bench/generate.py --out bench/data --scale 0.01   # 65,800 rows, for a quick try
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import random
from pathlib import Path

SOURCES = 24
DISTINCT = 6_000_000
PLANTED_DUPES = 580_000
CATEGORIES = ["podcast", "journalist", "youtube", "newsletter", "blog", "radio"]
CAT_WEIGHTS = [30, 20, 20, 12, 12, 6]
HOSTS = {"podcast": ["feeds.{s}.fm", "{s}.libsyn.com", "anchor.fm/s/{s}", "{s}.podbean.com"],
         "journalist": ["{s}.substack.com", "muckrack.com/{s}", "twitter.com/{s}", "{s}.journo.site"],
         "youtube": ["youtube.com/@{s}", "youtube.com/c/{s}"],
         "newsletter": ["{s}.substack.com", "{s}.beehiiv.com", "buttondown.email/{s}"],
         "blog": ["{s}.wordpress.com", "{s}.medium.com", "{s}.blog"],
         "radio": ["{s}.radio.net", "tunein.com/radio/{s}"]}
WORDS = ("daily morning tech money market crypto health mind body sport football cricket rugby "
         "politics world local city news talk show hour weekly brief signal noise future past "
         "history science space code data design startup founder growth sales travel food "
         "wine coffee music indie jazz comedy true crime law parenting faith garden farm").split()
LANGS = ["en", "en", "en", "en", "es", "de", "fr", "pt", "ja", "ko"]
COUNTRIES = ["US", "US", "US", "GB", "AU", "CA", "DE", "ES", "BR", "JP", "KR", "IN"]
TODAY = dt.date(2026, 10, 2)


def entity(i: int, rng: random.Random) -> dict:
    cat = rng.choices(CATEGORIES, CAT_WEIGHTS)[0]
    n = rng.randint(1, 3)
    name = " ".join(rng.choice(WORDS).capitalize() for _ in range(n))
    slug = f"{name.lower().replace(' ', '')}{i:x}"           # unique per entity
    url = "https://" + rng.choice(HOSTS[cat]).format(s=slug)
    old = rng.random() < 0.15
    days = rng.randint(731, 3650) if old else rng.randint(0, 730)
    return {"category": cat, "name": name, "url": url, "lang": rng.choice(LANGS),
            "country": rng.choice(COUNTRIES), "last": (TODAY - dt.timedelta(days=days)).isoformat()}


def vary(e: dict, rng: random.Random) -> tuple[dict, str]:
    """The same entity as another directory would list it. Returns (row, kind of difference)."""
    d = dict(e)
    kinds = []
    url = d["url"]
    if rng.random() < 0.5:
        url = url.replace("https://", "http://"); kinds.append("scheme")
    if rng.random() < 0.4 and "://www." not in url:
        url = url.replace("://", "://www."); kinds.append("www")
    if rng.random() < 0.4:
        url += "/"; kinds.append("slash")
    if rng.random() < 0.3:
        url += "?utm_source=directory"; kinds.append("query")
    if rng.random() < 0.2:
        scheme, rest = url.split("://", 1)
        host, _, path = rest.partition("/")
        url = f"{scheme}://{host.upper()}" + (f"/{path}" if path else ""); kinds.append("host_case")
    d["url"] = url
    r = rng.random()
    if r < 0.10:
        d["name"] = "The " + " ".join(rng.choice(WORDS).capitalize() for _ in range(2)); kinds.append("renamed")
    elif r < 0.40:
        d["name"] = d["name"].upper(); kinds.append("name_case")
    elif r < 0.55:
        d["name"] = "The " + d["name"]; kinds.append("the")
    elif r < 0.70:
        d["name"] = d["name"].replace(" ", "  ") + " "; kinds.append("spaces")
    elif r < 0.80:
        d["name"] = d["name"] + "!"; kinds.append("punct")
    if rng.random() < 0.3:                                    # the other directory saw it later
        d["last"] = (dt.date.fromisoformat(d["last"]) + dt.timedelta(days=rng.randint(1, 60))).isoformat()
        if d["last"] > TODAY.isoformat():
            d["last"] = TODAY.isoformat()
    return d, "+".join(kinds) or "identical"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=Path("bench/data"))
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--seed", type=int, default=20261002)
    a = ap.parse_args()
    distinct, dupes = int(DISTINCT * a.scale), int(PLANTED_DUPES * a.scale)
    rng = random.Random(a.seed)
    a.out.mkdir(parents=True, exist_ok=True)
    files = [open(a.out / f"source_{s:02d}.csv", "w", newline="", encoding="utf-8") for s in range(1, SOURCES + 1)]
    writers = [csv.writer(f) for f in files]
    header = ["source_record_id", "category", "name", "url", "language", "country", "last_activity"]
    for w in writers:
        w.writerow(header)
    counters = [0] * SOURCES
    home = []                                    # (source, entity) for planting duplicates later
    sample_every = max(1, distinct // (dupes * 3))
    for i in range(distinct):
        e = entity(i, rng)
        s = rng.randrange(SOURCES)
        counters[s] += 1
        writers[s].writerow([f"s{s + 1:02d}-{counters[s]}", e["category"], e["name"], e["url"], e["lang"], e["country"], e["last"]])
        if i % sample_every == 0:
            home.append((s, i, e))
    with open(a.out / "truth.csv", "w", newline="", encoding="utf-8") as tf:
        tw = csv.writer(tf)
        tw.writerow(["entity", "original_source", "original_record_id", "dupe_source", "dupe_record_id", "difference"])
        picks = rng.sample(home, dupes) if dupes <= len(home) else [rng.choice(home) for _ in range(dupes)]
        for s0, i, e in picks:
            s = rng.randrange(SOURCES - 1)
            s = s + 1 if s >= s0 else s                          # always a different source
            d, kind = vary(e, rng)
            counters[s] += 1
            rid = f"s{s + 1:02d}-{counters[s]}"
            writers[s].writerow([rid, d["category"], d["name"], d["url"], d["lang"], d["country"], d["last"]])
            tw.writerow([i, s0 + 1, "", s + 1, rid, kind])
    for f in files:
        f.close()
    print(f"{distinct + dupes:,} rows in {SOURCES} files ({distinct:,} entities + {dupes:,} planted duplicates) -> {a.out}")


if __name__ == "__main__":
    main()
