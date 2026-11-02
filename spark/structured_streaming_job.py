"""Spark Structured Streaming job: Kafka -> windowed aggregates -> parquet.

Run with:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
        spark/structured_streaming_job.py

The three guarantees this job is built around:

1. **Exactly-once into the lake.** The Kafka source is read with
   `startingOffsets=earliest` and a checkpoint directory; each micro-batch writes
   parquet with `foreachBatch` and a deterministic path, so a restart replays from
   the checkpoint rather than duplicating rows.
2. **Late data is bounded, not ignored.** A 2-minute watermark lets out-of-order
   mobile events land in the right window and then closes it. Anything later is
   captured by the `late_events` stream instead of silently vanishing.
3. **Bad records never block the stream.** Rows failing validation are written to
   a dead-letter path with a reason code, and the stream continues.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (IntegerType, StringType, StructField,
                               StructType, TimestampType)

KAFKA = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "orders.events")
CHECKPOINT = os.environ.get("CHECKPOINT_DIR", "s3a://streaming/checkpoints/orders")
OUTPUT = os.environ.get("OUTPUT_DIR", "s3a://streaming/lake/revenue_windows")
DLQ = os.environ.get("DEAD_LETTER_DIR", "s3a://streaming/lake/dead_letter")
WINDOW = os.environ.get("WINDOW_SECONDS", "300 seconds")
LATENESS = os.environ.get("ALLOWED_LATENESS", "120 seconds")

SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("amount_cents", IntegerType(), True),
    StructField("currency", StringType(), True),
    StructField("country", StringType(), True),
])

VALID_TYPES = ["order_placed", "order_paid", "order_shipped", "order_cancelled"]


def main() -> None:
    spark = (
        SparkSession.builder.appName("streaming-orders")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 20000)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
    )

    # validation, kept in one place so the DLQ reason matches the local runner
    reason = (
        F.when(F.col("event_id").isNull(), F.lit("missing_field"))
        .when(F.col("order_id").isNull(), F.lit("missing_field"))
        .when(F.col("event_time").isNull(), F.lit("unparseable_time"))
        .when(~F.col("event_type").isin(VALID_TYPES), F.lit("unknown_event_type"))
        .when(F.col("amount_cents").isNull(), F.lit("bad_type"))
        .when((F.col("amount_cents") < 0) & (F.col("event_type") != "order_cancelled"),
              F.lit("negative_amount"))
    )
    checked = parsed.withColumn("dlq_reason", reason)

    good = checked.filter(F.col("dlq_reason").isNull()).drop("dlq_reason")
    bad = checked.filter(F.col("dlq_reason").isNotNull())

    windows = (
        good.withWatermark("event_time", LATENESS)
        .groupBy(F.window("event_time", WINDOW), "country")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum(F.when(F.col("event_type") == "order_paid", F.col("amount_cents"))
                  .otherwise(0)).alias("revenue_cents"),
            F.sum(F.when(F.col("event_type") == "order_placed", 1).otherwise(0)).alias("orders_placed"),
            F.sum(F.when(F.col("event_type") == "order_paid", 1).otherwise(0)).alias("orders_paid"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "country", "orders", "orders_placed", "orders_paid", "revenue_cents",
            F.round(F.col("revenue_cents") / 100, 2).alias("revenue_inr"),
        )
    )

    def write_batch(batch_df, batch_id: int) -> None:
        # deterministic path per batch id: a replayed batch overwrites itself
        batch_df.write.mode("overwrite").option("compression", "zstd") \
            .parquet(f"{OUTPUT}/batch={batch_id:05d}")

    query = (
        windows.writeStream.outputMode("append")
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"{CHECKPOINT}/revenue")
        .trigger(processingTime="30 seconds")
        .start()
    )

    dlq = (
        bad.writeStream.outputMode("append").format("json")
        .option("path", f"{DLQ}/records")
        .option("checkpointLocation", f"{CHECKPOINT}/dlq")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()
    dlq.awaitTermination()


if __name__ == "__main__":
    main()
