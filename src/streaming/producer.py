"""Event producer.

Two modes, one contract:
  - KAFKA_BOOTSTRAP set  -> produce to Kafka with acks=all and idempotence on
  - otherwise            -> write JSONL to data/raw/events.jsonl

The synthetic stream includes the failures a real one has: duplicates from
at-least-once delivery, out-of-order events from mobile clients, negative
amounts, unknown event types and truncated JSON.
"""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .config import Config

COUNTRIES = ["IN", "IN", "IN", "US", "GB", "AE", "SG"]
TYPES = ["order_placed", "order_placed", "order_paid", "order_paid", "order_shipped",
         "order_cancelled"]


def generate_events(cfg: Config) -> list[dict]:
    rng = random.Random(cfg.seed)
    end = datetime(2026, 11, 1, 18, 0, tzinfo=timezone.utc)
    start = end - timedelta(hours=6)
    span = (end - start).total_seconds()

    events: list[dict] = []
    order_seq = 0
    step = span / cfg.event_count
    for i in range(cfg.event_count):
        # the stream is roughly ordered in time, which is what a partitioned
        # Kafka topic actually gives you - a global shuffle would make every
        # event look late and the watermark meaningless
        when = start + timedelta(seconds=i * step + rng.uniform(-1.5, 1.5))
        # mobile clients deliver out of order: 4% arrive up to 200s late, so
        # most land inside the 120s watermark and a few fall outside it
        if rng.random() < 0.04:
            when -= timedelta(seconds=rng.randint(30, 200))

        event_type = rng.choice(TYPES)
        if event_type in ("order_placed", "order_paid"):
            order_seq += 1
        amount = rng.choice([19900, 24900, 49900, 99900, 149900, 249900])
        if event_type == "order_cancelled" and rng.random() < 0.5:
            amount = -amount

        event = {
            "event_id": f"EVT-{i:07d}",
            "order_id": f"ORD-{order_seq // 2:06d}",
            "event_type": event_type,
            "event_time": when.isoformat().replace("+00:00", "Z"),
            "amount_cents": amount,
            "currency": "INR",
            "country": rng.choice(COUNTRIES),
        }

        # at-least-once delivery: 3% are re-delivered
        if rng.random() < 0.03:
            events.append(dict(event))

        # defects, each mapped to a reason code
        roll = rng.random()
        if roll < 0.008:
            event["event_type"] = "order_refunded"
        elif roll < 0.014:
            event["amount_cents"] = str(amount)
        elif roll < 0.019:
            event["event_time"] = "not-a-timestamp"
        elif roll < 0.024:
            del event["order_id"]
        elif roll < 0.028:
            events.append("{truncated-json")
            continue

        events.append(event)

    return events


def to_kafka(events: list[dict], cfg: Config) -> int:
    from kafka import KafkaProducer  # noqa: PLC0415

    producer = KafkaProducer(
        bootstrap_servers=cfg.kafka_bootstrap,
        acks="all",
        enable_idempotence=True,
        linger_ms=20,
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode() if k else None,
    )
    sent = 0
    for event in events:
        producer.send(cfg.topic, key=event.get("order_id"), value=event)
        sent += 1
    producer.flush()
    return sent


def to_file(events: list[dict], cfg: Config) -> Path:
    cfg.raw_dir.mkdir(parents=True, exist_ok=True)
    path = cfg.raw_dir / "events.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event) if isinstance(event, dict) else event)
            fh.write("\n")
    return path
