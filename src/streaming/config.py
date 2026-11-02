"""Pipeline configuration, from the environment with safe local defaults."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class Config:
    kafka_bootstrap: str
    topic: str
    consumer_group: str
    window_seconds: int
    allowed_lateness_seconds: int
    micro_batch_size: int
    checkpoint_dir: Path
    output_dir: Path
    dead_letter_dir: Path
    raw_dir: Path
    reports_dir: Path
    seed: int
    event_count: int

    @property
    def has_kafka(self) -> bool:
        return bool(self.kafka_bootstrap)


def load() -> Config:
    data = REPO_ROOT / "data"
    return Config(
        kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP", ""),
        topic=os.getenv("KAFKA_TOPIC", "orders.events"),
        consumer_group=os.getenv("KAFKA_GROUP", "orders-aggregator"),
        window_seconds=int(os.getenv("WINDOW_SECONDS", "300")),
        allowed_lateness_seconds=int(os.getenv("ALLOWED_LATENESS_SECONDS", "120")),
        micro_batch_size=int(os.getenv("MICRO_BATCH_SIZE", "500")),
        checkpoint_dir=Path(os.getenv("CHECKPOINT_DIR", data / "checkpoints")),
        output_dir=Path(os.getenv("OUTPUT_DIR", data / "lake" / "revenue_windows")),
        dead_letter_dir=Path(os.getenv("DEAD_LETTER_DIR", data / "lake" / "dead_letter")),
        raw_dir=Path(os.getenv("RAW_DIR", data / "raw")),
        reports_dir=Path(os.getenv("REPORTS_DIR", REPO_ROOT / "reports")),
        seed=int(os.getenv("SEED", "11")),
        event_count=int(os.getenv("EVENT_COUNT", "40000")),
    )
