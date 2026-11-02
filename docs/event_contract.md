# Event contract

Every message on `orders.events` is a JSON object with these fields.

| Field | Type | Required | Notes |
|---|---|---|---|
| `event_id` | string | yes | Unique per event. The dedupe key. |
| `order_id` | string | yes | Groups events into an order. |
| `event_type` | string | yes | One of `order_placed`, `order_paid`, `order_shipped`, `order_cancelled`. |
| `event_time` | string | yes | ISO-8601 UTC, e.g. `2026-11-01T10:00:00Z`. The event time, not the ingest time. |
| `amount_cents` | integer | yes | Integer cents. Negative only for `order_cancelled`. |
| `currency` | string | no | Defaults to `INR`. |
| `country` | string | no | ISO-3166 alpha-2, defaults to `IN`. Used as a window key. |

## Example

```json
{
  "event_id": "EVT-0000123",
  "order_id": "ORD-000061",
  "event_type": "order_paid",
  "event_time": "2026-11-01T10:04:12Z",
  "amount_cents": 49900,
  "currency": "INR",
  "country": "IN"
}
```

## Why `event_time` and not ingest time

Windows are built on when the customer acted, not when the record arrived. That
is what makes a delayed event land in the right window instead of the window it
happened to be delivered in - and it is why the watermark is measured against
`event_time`.

## Adding an event type

1. Add it to `VALID_TYPES` in `src/streaming/events.py`.
2. Add it to `VALID_TYPES` in `spark/structured_streaming_job.py` - the two must
   not drift.
3. Decide whether it contributes revenue, a funnel count, or neither, in
   `windows.aggregate`.
4. Add a test in `tests/test_events.py` for the new type.
5. Note the change here.
