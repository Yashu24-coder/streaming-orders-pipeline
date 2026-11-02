"""End-to-end: micro-batches, idempotent sink, dead-letter routing."""

from __future__ import annotations

import json

import duckdb

from streaming.local_runner import run
from streaming.producer import generate_events, to_file


def test_pipeline_routes_defects_and_writes_parquet(cfg):
    events = generate_events(cfg)
    path = to_file(events, cfg)
    summary = run(cfg, events_path=path, verbose=False)

    assert summary["events_read"] == len(events)
    assert summary["events_accepted"] > 0
    assert summary["events_dead_lettered"] > 0
    assert summary["dead_letter_rate_pct"] < 15, "too many events rejected"

    # every reason code that fired is a known code
    from streaming.events import REASONS
    assert set(summary["reason_counts"]) <= set(REASONS)

    files = list(cfg.output_dir.glob("*.parquet"))
    assert files, "no parquet written"
    con = duckdb.connect()
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{cfg.output_dir.as_posix()}/*.parquet')").fetchone()[0]
    con.close()
    assert rows == summary["window_rows_written"]


def test_rerun_is_idempotent(cfg):
    events = generate_events(cfg)
    path = to_file(events, cfg)
    first = run(cfg, events_path=path, verbose=False)
    second = run(cfg, events_path=path, verbose=False)
    assert first["window_rows_written"] == second["window_rows_written"]
    files = sorted(p.name for p in cfg.output_dir.glob("*.parquet"))
    assert len(files) == first["micro_batches"], "a re-run must not append duplicate parts"


def test_duplicates_are_dropped(cfg):
    events = generate_events(cfg)
    path = to_file(events, cfg)
    summary = run(cfg, events_path=path, verbose=False)
    assert summary["duplicates_dropped"] > 0
    assert summary["events_accepted"] == summary["events_read"] - summary["events_dead_lettered"]


def test_dead_letter_file_has_explanations(cfg):
    events = generate_events(cfg)
    path = to_file(events, cfg)
    run(cfg, events_path=path, verbose=False)
    dlq = cfg.dead_letter_dir / "dead_letter.jsonl"
    assert dlq.exists()
    records = [json.loads(line) for line in dlq.read_text().splitlines()]
    assert records
    assert all(r["explanation"] for r in records)
