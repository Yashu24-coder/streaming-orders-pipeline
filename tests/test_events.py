"""Validation must classify every failure with the right reason code."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from streaming.events import fingerprint, parse

GOOD = {
    "event_id": "EVT-1",
    "order_id": "ORD-1",
    "event_type": "order_paid",
    "event_time": "2026-11-01T10:00:00Z",
    "amount_cents": 49900,
    "currency": "INR",
    "country": "IN",
}


def test_valid_event_parses():
    event, reason = parse(GOOD)
    assert reason is None
    assert event.event_time == datetime(2026, 11, 1, 10, 0, tzinfo=timezone.utc)
    assert event.amount_cents == 49900


@pytest.mark.parametrize("mutate,expected", [
    (lambda e: e.pop("order_id"), "missing_field"),
    (lambda e: e.update(amount_cents="49900"), "bad_type"),
    (lambda e: e.update(event_type="order_refunded"), "unknown_event_type"),
    (lambda e: e.update(event_time="yesterday"), "unparseable_time"),
    (lambda e: e.update(amount_cents=-100), "negative_amount"),
])
def test_defects_get_reason_codes(mutate, expected):
    payload = dict(GOOD)
    mutate(payload)
    event, reason = parse(payload)
    assert event is None
    assert reason == expected


def test_negative_amount_allowed_for_cancellation():
    payload = {**GOOD, "event_type": "order_cancelled", "amount_cents": -49900}
    event, reason = parse(payload)
    assert reason is None
    assert event.amount_cents == -49900


def test_malformed_json():
    event, reason = parse("{not json")
    assert event is None
    assert reason == "malformed_json"


def test_fingerprint_is_stable_and_fixed_width():
    assert fingerprint("EVT-1") == fingerprint("EVT-1")
    assert fingerprint("EVT-1") != fingerprint("EVT-2")
    assert len(fingerprint("EVT-1")) == 16
