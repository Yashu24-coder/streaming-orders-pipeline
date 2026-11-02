"""Order event schema, validation and dead-letter reason codes.

Every record that cannot be aggregated has to land somewhere explainable. Each
failure carries a reason code, so the DLQ is a diagnostic, not a bin.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

REQUIRED = ("event_id", "order_id", "event_type", "event_time", "amount_cents")

VALID_TYPES = {"order_placed", "order_paid", "order_shipped", "order_cancelled"}

# reason code -> human explanation, shown in the run summary and the README
REASONS = {
    "missing_field": "a required field is absent",
    "bad_type": "a field has the wrong type",
    "unknown_event_type": "event_type is not in the contract",
    "unparseable_time": "event_time is not ISO-8601",
    "negative_amount": "amount_cents is negative on a non-cancellation event",
    "duplicate_event_id": "the same event_id was already processed",
    "late_event": "event_time is older than the watermark",
    "malformed_json": "the payload is not valid JSON",
}


@dataclass
class OrderEvent:
    event_id: str
    order_id: str
    event_type: str
    event_time: datetime
    amount_cents: int
    currency: str = "INR"
    country: str = "IN"
    raw: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "order_id": self.order_id,
            "event_type": self.event_type,
            "event_time": self.event_time.isoformat(),
            "amount_cents": self.amount_cents,
            "currency": self.currency,
            "country": self.country,
        }


def parse(raw: bytes | str | dict) -> tuple[OrderEvent | None, str | None]:
    """Return (event, None) or (None, reason_code)."""
    if isinstance(raw, dict):
        payload = raw
    else:
        try:
            payload = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None, "malformed_json"

    if not isinstance(payload, dict):
        return None, "bad_type"

    for key in REQUIRED:
        if key not in payload or payload[key] in (None, ""):
            return None, "missing_field"

    if not isinstance(payload["amount_cents"], int) or isinstance(payload["amount_cents"], bool):
        return None, "bad_type"

    if payload["event_type"] not in VALID_TYPES:
        return None, "unknown_event_type"

    try:
        stamp = payload["event_time"]
        if isinstance(stamp, str):
            stamp = stamp.replace("Z", "+00:00")
            when = datetime.fromisoformat(stamp)
        else:
            return None, "unparseable_time"
    except (ValueError, TypeError):
        return None, "unparseable_time"

    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)

    if payload["amount_cents"] < 0 and payload["event_type"] != "order_cancelled":
        return None, "negative_amount"

    return OrderEvent(
        event_id=str(payload["event_id"]),
        order_id=str(payload["order_id"]),
        event_type=payload["event_type"],
        event_time=when,
        amount_cents=int(payload["amount_cents"]),
        currency=str(payload.get("currency", "INR")),
        country=str(payload.get("country", "IN")),
        raw=payload,
    ), None


def fingerprint(event_id: str) -> str:
    """Stable key for the dedupe set - keeps memory flat on long ids."""
    return hashlib.blake2b(event_id.encode(), digest_size=8).hexdigest()
