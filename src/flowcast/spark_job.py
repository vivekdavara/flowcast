"""The Spark Structured Streaming job: Kafka records in; dead letters, raw events and minute aggregates out, as Parquet.

DESIGN.md "Spark". The transformations are plain functions from DataFrame to DataFrame, so the stream (and day 3's
batch backfill) and the tests share one definition of "valid", "dedupe" and "window":

- `parse` turns Kafka records into one row per record: the event's fields, or the reason it's invalid. The verdict is
  `schema.decode`'s own, run in a Python UDF. Spark's `from_json` can't give the same verdicts: it keeps the last of two
  duplicate keys, ignores unknown keys and reads `NaN` as a number, where `decode` refuses all three.
- `dead_letters` and `raw_events` split the parsed rows. Neither keeps state, so nothing is ever dropped from them.
- `minute_aggregates` sets the watermark, drops duplicate event_ids, and counts one-minute windows by event type.
"""

from __future__ import annotations

import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import pyspark
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from flowcast.kafka_io import TOPIC
from flowcast.layout import Paths
from flowcast.schema import FIELDS, InvalidEvent, decode, values

WATERMARK_DELAY = "3 minutes"
WATERMARK_DELAY_MS = 180_000
# The stateful operators' state is split into this many partitions. It is fixed for the life of a checkpoint, and
# Spark's default (200) means 200 state stores per operator per batch, far too many for a laptop.
STATE_PARTITIONS = 6
# Spark keeps this many batches of its offset log; the replay check (check.py) needs every batch of a run.
BATCHES_TO_RETAIN = 100_000
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"

_INTEGER_FIELDS = {
    "schema_version": IntegerType(),
    "event_time": LongType(),
    "price_cents": LongType(),
    "quantity": IntegerType(),
}
EVENT_SCHEMA = StructType([StructField(name, _INTEGER_FIELDS.get(name, StringType())) for name in FIELDS])
PARSED_SCHEMA = StructType([StructField("error", StringType()), *EVENT_SCHEMA.fields])
# What the Kafka source gives besides the value, kept on every output row so a row can be traced to its record.
KAFKA_COLUMNS = ("topic", "partition", "offset", "kafka_timestamp")


def session(app: str = "flowcast", master: str = "local[*]", *, kafka: bool = True) -> SparkSession:
    """A local Spark session set up for this job: UTC, few state partitions, no UI, the Kafka source if asked for.

    The Python workers run this interpreter: Spark would otherwise start `python3` from PATH, which may be another
    version (it refuses a minor-version mismatch) and won't have flowcast installed.
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.master(master)
        .appName(app)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(STATE_PARTITIONS))
        .config("spark.sql.streaming.minBatchesToRetain", str(BATCHES_TO_RETAIN))
        # Parquet's standard timestamp (UTC microseconds) rather than Spark's legacy INT96 default.
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.ui.enabled", "false")
        # Local mode needs only loopback: no lookup of the machine's hostname (which can fail to resolve after a
        # network change, and then the driver can't bind), and nothing listening on other interfaces.
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
    )
    if kafka:
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    return builder.getOrCreate()


def _parse_value(value: bytes | bytearray | None) -> tuple[object, ...]:
    """(None, the event's fields) for a valid record; (the reason, then all None) for an invalid one."""
    try:
        event = decode(None if value is None else bytes(value))
    except InvalidEvent as exc:
        return (str(exc),) + (None,) * len(FIELDS)
    return (None, *values(event))


_parse_udf = F.udf(_parse_value, PARSED_SCHEMA, useArrow=True)


def parse(records: DataFrame) -> DataFrame:
    """One row per Kafka record (columns value, topic, partition, offset, timestamp): `error` (None when valid), the
    event's fields, the raw value and the record's Kafka coordinates."""
    return records.select(
        _parse_udf("value").alias("parsed"),
        "value",
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
    ).select("parsed.*", "value", *KAFKA_COLUMNS)


def dead_letters(parsed: DataFrame) -> DataFrame:
    """The invalid records: their coordinates, raw bytes and the reason."""
    return parsed.where(F.col("error").isNotNull()).select(*KAFKA_COLUMNS, "value", "error")


def raw_events(parsed: DataFrame) -> DataFrame:
    """Every valid record, duplicates and late ones included, with its coordinates and its event date (UTC)."""
    return parsed.where(F.col("error").isNull()).select(
        *FIELDS, *KAFKA_COLUMNS, F.to_date(F.timestamp_millis("event_time")).alias("event_date")
    )


def _revenue() -> Column:
    return F.when(F.col("event_type") == "purchase", F.col("price_cents") * F.col("quantity")).otherwise(F.lit(0))


def deduplicated(events: DataFrame, delay: str = WATERMARK_DELAY) -> DataFrame:
    """The events with an `event_ts` timestamp, a watermark `delay` behind the latest event time, and each event_id
    once. On a stream, a row at or before the previous batch's watermark is dropped here (DESIGN.md "Ground truth")."""
    return (
        events.withColumn("event_ts", F.timestamp_millis("event_time"))
        .withWatermark("event_ts", delay)
        .dropDuplicatesWithinWatermark(["event_id"])
    )


def window_counts(events: DataFrame) -> DataFrame:
    """Per one-minute window of `event_ts` and event type: events and purchase revenue in cents. On a stream in append
    mode, a window is written once, when the watermark passes its end."""
    return (
        events.groupBy(F.window("event_ts", "1 minute"), "event_type")
        .agg(F.count(F.lit(1)).alias("events"), F.sum(_revenue()).cast(LongType()).alias("revenue_cents"))
        .select(
            F.unix_millis(F.col("window.start")).alias("window_start"),
            "event_type",
            "events",
            "revenue_cents",
        )
    )


def minute_aggregates(events: DataFrame, delay: str = WATERMARK_DELAY) -> DataFrame:
    """The job's aggregate: `window_counts` of `deduplicated` events."""
    return window_counts(deduplicated(events, delay))


@dataclass(frozen=True)
class Queries:
    dead_letters: StreamingQuery
    raw_events: StreamingQuery
    minute_aggregates: StreamingQuery

    def all(self) -> tuple[StreamingQuery, ...]:
        return (self.dead_letters, self.raw_events, self.minute_aggregates)

    def await_termination(self) -> None:
        for query in self.all():
            query.awaitTermination()

    def stop(self) -> None:
        for query in self.all():
            query.stop()


def kafka_records(
    spark: SparkSession, bootstrap: str, topic: str = TOPIC, *, max_offsets_per_trigger: int | None = None
) -> DataFrame:
    """The topic as a stream, from the earliest offset; a gap in the offsets (data deleted under the job) fails it.
    `max_offsets_per_trigger` caps a batch's records (spread over the partitions), so a backlog is read in steps."""
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "true")
    )
    if max_offsets_per_trigger is not None:
        reader = reader.option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
    return reader.load()


def start(
    spark: SparkSession,
    records: DataFrame,
    paths: Paths,
    *,
    trigger_s: float = 5.0,
    available_now: bool = False,
    delay: str = WATERMARK_DELAY,
) -> Queries:
    """Start the three queries over `records` (a Kafka stream, or anything with its columns), writing under `paths`.

    A batch starts every `trigger_s` seconds (at once if the last one took longer). With `available_now`, the queries
    instead read what is there when they start, in as many batches as the offset cap makes, and then stop.
    """
    if not math.isfinite(trigger_s) or trigger_s < 0:
        raise ValueError("trigger_s must be a finite number of seconds, at least 0")
    interval = f"{round(trigger_s * 1000)} milliseconds"
    parsed = parse(records)

    def sink(frame: DataFrame, name: str, out: Path, partition_by: tuple[str, ...] = ()) -> StreamingQuery:
        writer = (
            frame.writeStream.queryName(name)
            .format("parquet")
            .option("path", str(out))
            .option("checkpointLocation", str(paths.checkpoint(name)))
            .outputMode("append")
        )
        writer = writer.trigger(availableNow=True) if available_now else writer.trigger(processingTime=interval)
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        return writer.start()

    return Queries(
        dead_letters=sink(dead_letters(parsed), "dead_letters", paths.dead_letters),
        raw_events=sink(raw_events(parsed), "raw_events", paths.raw_events, ("event_date",)),
        minute_aggregates=sink(
            minute_aggregates(raw_events(parsed), delay), "minute_aggregates", paths.minute_aggregates
        ),
    )
