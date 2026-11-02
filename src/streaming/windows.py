"""Tumbling-window aggregation and watermark handling.

Pure functions with no I/O, which is why the tests can pin the exact behaviour
of a late event without starting Kafka.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Iterable

from .events import OrderEvent


def window_start(when: datetime, seconds: int) -> datetime:
    """Floor a timestamp to the start of its tumbling window."""
    epoch = int(when.timestamp())
    return datetime.fromtimestamp(epoch - epoch % seconds, tz=timezone.utc)


def window_end(start: datetime, seconds: int) -> datetime:
    return start + timedelta(seconds=seconds)


def aggregate(events: Iterable[OrderEvent], window_seconds: int) -> list[dict]:
    """Aggregate events into tumbling windows.

    Only `order_paid` contributes revenue; the other event types are counted so
    the funnel is visible in the same output.
    """
    buckets: dict[tuple[datetime, str], dict] = defaultdict(
        lambda: {"revenue_cents": 0, "orders": set(), "paid": 0, "placed": 0, "cancelled": 0}
    )

    for event in events:
        key = (window_start(event.event_time, window_seconds), event.country)
        bucket = buckets[key]
        bucket["orders"].add(event.order_id)
        if event.event_type == "order_placed":
            bucket["placed"] += 1
        elif event.event_type == "order_paid":
            bucket["paid"] += 1
            bucket["revenue_cents"] += event.amount_cents
        elif event.event_type == "order_cancelled":
            bucket["cancelled"] += 1

    rows = []
    for (start, country), bucket in sorted(buckets.items()):
        rows.append({
            "window_start": start.isoformat(),
            "window_end": window_end(start, window_seconds).isoformat(),
            "country": country,
            "orders": len(bucket["orders"]),
            "orders_placed": bucket["placed"],
            "orders_paid": bucket["paid"],
            "orders_cancelled": bucket["cancelled"],
            "revenue_cents": bucket["revenue_cents"],
            "revenue_inr": round(bucket["revenue_cents"] / 100, 2),
        })
    return rows


class Watermark:
    """Tracks the highest event time seen and rejects anything too far behind.

    A watermark of `max_event_time - allowed_lateness` is the standard contract:
    state for older windows can be dropped, so an event older than that is
    either late (route to the DLQ) or evidence the producer clock is wrong.
    """

    def __init__(self, allowed_lateness_seconds: int) -> None:
        self.allowed_lateness = allowed_lateness_seconds
        self.max_event_time: datetime | None = None

    def observe(self, when: datetime) -> None:
        if self.max_event_time is None or when > self.max_event_time:
            self.max_event_time = when

    def current(self) -> datetime | None:
        if self.max_event_time is None:
            return None
        return self.max_event_time - timedelta(seconds=self.allowed_lateness)

    def is_late(self, when: datetime) -> bool:
        current = self.current()
        return current is not None and when < current
