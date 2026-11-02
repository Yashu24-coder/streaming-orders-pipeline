"""Window assignment, aggregation and watermark behaviour."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from streaming.events import OrderEvent
from streaming.windows import Watermark, aggregate, window_start

T0 = datetime(2026, 11, 1, 10, 0, tzinfo=timezone.utc)


def make(minute: int, second: int = 0, event_type="order_paid", amount=10000, country="IN"):
    return OrderEvent(
        event_id=f"E-{minute}-{second}-{event_type}",
        order_id=f"O-{minute}-{second}",
        event_type=event_type,
        event_time=T0 + timedelta(minutes=minute, seconds=second),
        amount_cents=amount,
        country=country,
    )


def test_window_start_floors_to_boundary():
    assert window_start(T0 + timedelta(minutes=7, seconds=41), 300) == T0 + timedelta(minutes=5)
    assert window_start(T0 + timedelta(minutes=4, seconds=59), 300) == T0
    assert window_start(T0 + timedelta(minutes=5), 300) == T0 + timedelta(minutes=5)


def test_events_in_same_window_are_one_row():
    rows = aggregate([make(1), make(2), make(4)], 300)
    assert len(rows) == 1
    assert rows[0]["orders_paid"] == 3
    assert rows[0]["revenue_cents"] == 30000


def test_events_split_across_windows():
    rows = aggregate([make(1), make(6)], 300)
    assert len(rows) == 2
    assert [r["revenue_cents"] for r in rows] == [10000, 10000]


def test_countries_are_separate_buckets():
    rows = aggregate([make(1, country="IN"), make(2, country="US")], 300)
    assert {r["country"] for r in rows} == {"IN", "US"}


def test_only_paid_events_add_revenue():
    rows = aggregate([make(1, event_type="order_placed", amount=99999),
                      make(2, event_type="order_paid", amount=5000)], 300)
    assert rows[0]["revenue_cents"] == 5000
    assert rows[0]["orders_placed"] == 1


def test_watermark_rejects_events_beyond_lateness():
    mark = Watermark(allowed_lateness_seconds=120)
    mark.observe(T0 + timedelta(minutes=10))
    # watermark = max_event_time - lateness = T0 + 8 minutes
    assert mark.is_late(T0 + timedelta(minutes=10)) is False
    assert mark.is_late(T0 + timedelta(minutes=8)) is False   # on the boundary: on time
    assert mark.is_late(T0 + timedelta(minutes=7, seconds=59)) is True


def test_watermark_moves_only_forward():
    mark = Watermark(allowed_lateness_seconds=60)
    mark.observe(T0 + timedelta(minutes=5))
    mark.observe(T0 + timedelta(minutes=2))
    assert mark.max_event_time == T0 + timedelta(minutes=5)
