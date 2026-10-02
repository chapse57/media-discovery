"""
    python -m discovery run --trending 300            # seed on the first run, discover on later ones
    python -m discovery run --trending 300 --new 100  # also Podcast Index's newest feeds
    python -m discovery run --source itunes --trending 300   # no Podcast Index key: Apple Search API

--source auto (default) uses Podcast Index when its keys are set, Apple otherwise.
    python -m discovery report

DSN from DISCOVERY_DSN (default postgresql://postgres:pw@localhost:5432/discovery).
Podcast Index keys from PODCASTINDEX_KEY / PODCASTINDEX_SECRET.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys

import psycopg

from . import pipeline, sources


def main() -> None:
    ap = argparse.ArgumentParser(prog="discovery")
    ap.add_argument("--dsn", default=os.environ.get("DISCOVERY_DSN", "postgresql://postgres:pw@localhost:5432/discovery"))
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--trending", type=int, default=300)
    r.add_argument("--new", type=int, default=0, help="also take this many of Podcast Index's newest feeds")
    r.add_argument("--lang", default="en")
    r.add_argument("--source", choices=["auto", "podcastindex", "itunes"], default="auto")
    r.add_argument("--no-itunes", action="store_true")
    r.add_argument("--note", default="")
    sub.add_parser("report")
    a = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if a.cmd == "run":
        src = a.source if a.source != "auto" else ("podcastindex" if sources.have_podcastindex_keys() else "itunes")
        print(f"catalogue: {src}", file=sys.stderr)        # stdout is the JSON summary only

        def listings(log):
            if src == "itunes":
                return sources.itunes_search(a.trending, log)
            out = sources.trending(a.trending, log, a.lang)
            if a.new:
                out += sources.recent_new(a.new, log)
            return out
        print(json.dumps(pipeline.run(a.dsn, listings, f"{a.note} [{src}]".strip(), itunes=not a.no_itunes),
                         indent=2, default=str))
    else:
        with psycopg.connect(a.dsn) as conn:
            last = conn.execute("SELECT max(run_id) FROM disc.run").fetchone()[0]
            print(json.dumps(pipeline.summary(conn, last), indent=2, default=str))


if __name__ == "__main__":
    main()
