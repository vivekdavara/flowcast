"""The batch backfill: the minute aggregates of every raw event, very late ones included (DESIGN.md "Spark").

It runs the stream's own transformations (`spark_job.minute_aggregates`: the same dedupe and the same windows) over the
raw events the stream wrote, as a batch. On a batch DataFrame the watermark does nothing, so nothing is dropped as late,
and the result must equal the truth of every event exactly.

It reads the raw sink's committed files (its `_spark_metadata` log, as the views do), with the raw events' schema given
rather than inferred, so files written before schema version 2 (no `campaign` column) read as campaign null.

The result is written to a new directory per run, `backfill/<run>/`, and published by replacing the pointer file
`backfill/CURRENT` (a rename, which is atomic): a reader sees the previous complete result or the new one, never a
half-written one, and a backfill that fails part way publishes nothing.
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, TimestampType

from flowcast.layout import Paths
from flowcast.spark_job import EVENT_SCHEMA, minute_aggregates
from flowcast.views import committed_files

RAW_SCHEMA = StructType(
    [
        *EVENT_SCHEMA.fields,
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("kafka_timestamp", TimestampType()),
    ]
)


class BackfillError(Exception):
    pass


def raw_events(spark: SparkSession, paths: Paths) -> DataFrame:
    """The committed raw events of a run, every column but the partition directory's `event_date`."""
    files = committed_files(paths.raw_events)
    if not files:
        raise BackfillError(f"{paths.raw_events}: no committed raw events")
    return spark.read.schema(RAW_SCHEMA).parquet(*(str(f) for f in files))


def run(spark: SparkSession, paths: Paths) -> Path:
    """Compute the backfill and publish it; returns the directory it was written to."""
    out = paths.backfill / f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{uuid.uuid4().hex[:8]}"
    # The stream's own aggregate, over a batch: each event_id once, one-minute windows by event type.
    minute_aggregates(raw_events(spark, paths)).coalesce(1).write.parquet(str(out))
    pointer = paths.backfill / "CURRENT"
    temporary = paths.backfill / f".CURRENT.{uuid.uuid4().hex}"
    temporary.write_text(out.name + "\n")
    os.replace(temporary, pointer)
    return out
