"""Micro-batch engine - the same semantics as the Spark job, without Docker.

Reads events, processes them in micro-batches, applies deduplication, the
watermark and the tumbling-window aggregation, then writes parquet per batch and
routes failures to the dead-letter log. The Spark job in spark/ implements the
identical contract with real Kafka; this exists so the logic is testable and so
a reviewer can run the pipeline in ten seconds.
"""

from __future__ import annotations

import json
import shutil
import time
from collections import Counter
from pathlib import Path

from . import sink
from .config import Config
from .events import REASONS, fingerprint, parse
from .windows import Watermark, aggregate


def run(cfg: Config, events_path: Path | None = None, verbose: bool = True) -> dict:
    events_path = events_path or cfg.raw_dir / "events.jsonl"
    if not events_path.exists():
        raise FileNotFoundError(f"no events at {events_path}; run `make produce` first")

    for path in (cfg.output_dir, cfg.checkpoint_dir):
        if path.exists():
            shutil.rmtree(path)
    if cfg.dead_letter_dir.exists():
        shutil.rmtree(cfg.dead_letter_dir)

    lines = [ln for ln in events_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    watermark = Watermark(cfg.allowed_lateness_seconds)
    seen: set[str] = set()
    dead_letters: list[dict] = []
    reason_counts: Counter = Counter()
    batch_stats: list[dict] = []
    total_windows = 0

    started = time.perf_counter()
    batch_id = 0
    for offset in range(0, len(lines), cfg.micro_batch_size):
        batch_id += 1
        chunk = lines[offset:offset + cfg.micro_batch_size]
        good = []
        batch_dlq = 0

        for line in chunk:
            event, reason = parse(line)
            if event is not None:
                key = fingerprint(event.event_id)
                if key in seen:
                    reason = "duplicate_event_id"
                elif watermark.is_late(event.event_time):
                    reason = "late_event"
                else:
                    seen.add(key)
                    watermark.observe(event.event_time)
                    good.append(event)

            if reason is not None:
                batch_dlq += 1
                reason_counts[reason] += 1
                dead_letters.append({
                    "reason": reason,
                    "explanation": REASONS[reason],
                    "batch_id": batch_id,
                    "payload": line[:400],
                })

        rows = aggregate(good, cfg.window_seconds)
        sink.write_batch(rows, cfg.output_dir, batch_id)
        total_windows += len(rows)
        batch_stats.append({"batch_id": batch_id, "events": len(chunk),
                            "accepted": len(good), "dead_lettered": batch_dlq,
                            "windows": len(rows)})
        if verbose and batch_id % 10 == 0:
            print(f"  batch {batch_id:3d} | events {len(chunk):5d} | "
                  f"accepted {len(good):5d} | dlq {batch_dlq:3d} | windows {len(rows):4d}")

    elapsed = time.perf_counter() - started
    sink.append_dead_letters(dead_letters, cfg.dead_letter_dir)

    summary = {
        "events_read": len(lines),
        "events_accepted": sum(b["accepted"] for b in batch_stats),
        "events_dead_lettered": len(dead_letters),
        "dead_letter_rate_pct": round(100 * len(dead_letters) / max(len(lines), 1), 2),
        "duplicates_dropped": reason_counts.get("duplicate_event_id", 0),
        "late_events": reason_counts.get("late_event", 0),
        "reason_counts": dict(reason_counts),
        "micro_batches": batch_id,
        "window_rows_written": total_windows,
        "elapsed_seconds": round(elapsed, 2),
        "events_per_second": round(len(lines) / max(elapsed, 0.001), 0),
        "window_seconds": cfg.window_seconds,
        "allowed_lateness_seconds": cfg.allowed_lateness_seconds,
        "watermark_max_event_time": watermark.max_event_time.isoformat()
        if watermark.max_event_time else None,
        "batches": batch_stats,
    }

    reports = cfg.reports_dir
    reports.mkdir(parents=True, exist_ok=True)
    (reports / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if verbose:
        print(f"  read {summary['events_read']:,} | accepted {summary['events_accepted']:,} | "
              f"dlq {summary['events_dead_lettered']:,} "
              f"({summary['dead_letter_rate_pct']}%) | {elapsed:.2f}s")
    return summary
