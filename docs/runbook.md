# Runbook

## Local, ten seconds

```bash
export PYTHONPATH=src
python -m streaming.cli produce   # writes data/raw/events.jsonl
python -m streaming.cli run       # micro-batches -> data/lake/
python -m streaming.cli report    # charts -> docs/images/
```

## Kafka, the real thing

```bash
docker compose up -d                       # broker + UI on :8080
python -m streaming.cli produce            # honours KAFKA_BOOTSTRAP
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    spark/structured_streaming_job.py
```

Check the topic:

```bash
docker exec -it streaming-kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.events --max-messages 5
```

## Reading the dead-letter queue

```bash
# counts by reason
python - <<'PY'
import json, collections, pathlib
rows = [json.loads(l) for l in pathlib.Path("data/lake/dead_letter/dead_letter.jsonl").read_text().splitlines()]
for reason, count in collections.Counter(r["reason"] for r in rows).most_common():
    print(f"{reason:22s} {count:6d}")
PY
```

Interpretation:

| Pattern | Likely cause | Action |
|---|---|---|
| `duplicate_event_id` steady at 2-4% | normal at-least-once | none |
| `duplicate_event_id` spikes | producer retrying on timeout | check producer `request.timeout.ms` and broker health |
| `late_event` rising | consumer lag or producer clock drift | check consumer lag first, then NTP on producers |
| `malformed_json` | truncated writes | check producer buffer and network |
| `unknown_event_type` | a new event type shipped upstream | add it to `VALID_TYPES` with a migration note |

## Restarting the Spark job

Stop it, fix the issue, start it again. The checkpoint directory holds the
offsets and the window state, so it resumes where it stopped. Only delete the
checkpoint if you intend to reprocess from `startingOffsets`:

```bash
rm -rf data/checkpoints/revenue   # full reprocess on next start
```

A reprocessed batch overwrites its own part file, so revenue does not double.

## Tuning the watermark

- Too small: legitimate out-of-order events become `late_event` and are lost from
  the aggregate.
- Too large: window state is held longer, so memory and checkpoint size grow.

The default 120 s is set against the 30-200 s lateness the producer injects. If
you change `ALLOWED_LATENESS_SECONDS`, re-run `make all` and check that the
`late_event` count stays small.
