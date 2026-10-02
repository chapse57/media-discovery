"""The benchmark's duplicate query is scored, not just timed: at small scale it must find every
planted duplicate and nothing else. Needs BENCH_DSN (any database; uses schema `bench`)."""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DSN = os.environ.get("BENCH_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="BENCH_DSN not set")


def test_small_run_finds_every_planted_duplicate_and_nothing_else(tmp_path):
    data, out = tmp_path / "data", tmp_path / "r.json"
    subprocess.run([sys.executable, "bench/generate.py", "--out", str(data), "--scale", "0.002"], cwd=ROOT, check=True)
    subprocess.run([sys.executable, "bench/run.py", "--data", str(data), "--dsn", DSN, "--out", str(out)],
                   cwd=ROOT, check=True, capture_output=True)
    f = json.loads(out.read_text())["facts"]
    assert f["rows"] == 13_160 and f["sources"] == 24
    assert f["planted_dupes_found"] == f["planted_dupes"] == 1_160
    assert f["candidate_groups_without_planted_dupe"] == 0
    assert f["name_only_collisions"] > 0          # the reason the key is the URL, not the name


def test_url_key_normalization():
    """Runs the benchmark's own NORMALIZE statement, so the test cannot drift from it."""
    import psycopg
    sys.path.insert(0, str(ROOT / "bench"))
    import run
    cases = {
        "https://feeds.dailytech.fm": "feeds.dailytech.fm",
        "http://www.feeds.dailytech.fm/": "feeds.dailytech.fm",
        "HTTP://WWW.FEEDS.DailyTech.FM/?utm_source=directory": "feeds.dailytech.fm",
        "https://anchor.fm/s/Ab12//": "anchor.fm/s/Ab12",          # path case is kept
        "https://youtube.com/@show#about": "youtube.com/@show",
    }
    with psycopg.connect(DSN) as c:
        c.execute(run.RAW)
        for i, url in enumerate(cases):
            c.execute("INSERT INTO bench.raw VALUES (1, %s, 'podcast', 'The  Daily Tech!', %s, 'en', 'US', '2026-01-01')",
                      (f"r{i}", url))
        c.execute(run.NORMALIZE)
        got = dict(c.execute("SELECT url, url_key FROM bench.entry").fetchall())
        names = {r[0] for r in c.execute("SELECT name_key FROM bench.entry")}
    assert got == cases
    assert names == {"daily tech"}
