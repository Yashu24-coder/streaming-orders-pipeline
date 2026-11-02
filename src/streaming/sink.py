"""Idempotent parquet sink.

Each micro-batch writes one part file named after the batch id, and a batch that
is replayed overwrites its own file rather than appending a duplicate. That is
the same guarantee Spark gets from checkpointing plus a deterministic file name.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb


def write_batch(rows: list[dict], out_dir: Path, batch_id: int) -> Path | None:
    if not rows:
        return None
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / f"part-batch-{batch_id:05d}.parquet"
    tmp = out_dir / f".tmp-batch-{batch_id:05d}.json"
    tmp.write_text(json.dumps(rows), encoding="utf-8")
    con = duckdb.connect()
    try:
        con.execute(
            f"COPY (SELECT * FROM read_json_auto('{tmp.as_posix()}')) "
            f"TO '{target.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()
        tmp.unlink(missing_ok=True)
    return target


def append_dead_letters(records: list[dict], out_dir: Path) -> None:
    if not records:
        return
    out_dir.mkdir(parents=True, exist_ok=True)
    target = out_dir / "dead_letter.jsonl"
    with target.open("a", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record) + "\n")


def read_windows(out_dir: Path) -> "object":
    con = duckdb.connect()
    return con.execute(
        f"SELECT * FROM read_parquet('{(out_dir / '*.parquet').as_posix()}')"
    ).df()
