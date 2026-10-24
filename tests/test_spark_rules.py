"""Spark's late-row rules, confirmed on the job's own transformations before check.py relies on them.

Each test feeds a file stream one file per micro-batch (maxFilesPerTrigger = 1) with Spark's no-data batches turned
off, so batch i is exactly file i, and its watermark is known from the checkpoint. T0 is on a minute boundary, so the
first watermark, T0 - 3 minutes, is on one too and windows can end exactly on it.
"""

import json
import os
import uuid
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from typing import Any

import pytest

from flowcast.check import committed_batches

pytestmark = pytest.mark.spark

T0 = 1_767_225_600_000  # 2026-01-01T00:00:00Z
DELAY = 180_000
W1 = T0 - DELAY  # the watermark after a batch whose latest event is at T0
SCHEMA = "event_id STRING, event_type STRING, event_time BIGINT, price_cents BIGINT, quantity INT"


def event(event_time: int, event_id: str | None = None, event_type: str = "page_view") -> dict[str, Any]:
    return {"event_id": event_id or str(uuid.uuid4()), "event_type": event_type, "event_time": event_time}


@pytest.fixture
def no_data_batches_off(spark: Any) -> Iterator[None]:
    key = "spark.sql.streaming.noDataMicroBatches.enabled"
    spark.conf.set(key, "false")
    yield
    spark.conf.set(key, "true")


def run(
    spark: Any, tmp_path: Path, batches: Sequence[Sequence[dict[str, Any]]], transform: Callable[[Any], Any]
) -> tuple[list[Any], list[int]]:
    """Run `transform` over the batches as a stream to completion; the rows it wrote and each batch's watermark."""
    source = tmp_path / "in"
    source.mkdir()
    for i, rows in enumerate(batches):
        path = source / f"{i:03}.json"
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        os.utime(path, (1_000_000 + i, 1_000_000 + i))  # the file source takes files oldest first
    name = f"rules_{uuid.uuid4().hex}"
    stream = spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 1).json(str(source))
    query = (
        transform(stream)
        .writeStream.format("memory")
        .queryName(name)
        .outputMode("append")
        .option("checkpointLocation", str(tmp_path / "checkpoint"))
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
    run.last_progress = query.lastProgress  # type: ignore[attr-defined]
    rows = spark.table(name).collect()
    spark.catalog.dropTempView(name)
    watermarks = [b.watermark_ms for b in committed_batches(tmp_path / "checkpoint")]
    assert len(watermarks) == len(batches), "one batch per file"
    return rows, watermarks


@pytest.mark.usefixtures("no_data_batches_off")
def test_the_dedupe_drops_rows_at_or_before_the_previous_batchs_watermark(spark: Any, tmp_path: Path) -> None:
    from flowcast.spark_job import deduplicated

    batches = [
        [event(T0, "a")],
        # Batch 1 runs with watermark W1, but its late filter uses batch 0's (none yet): b is kept although below W1.
        [event(T0 - 200_000, "b")],
        # Batch 2's late filter is batch 1's watermark, W1: c (below) and d (exactly on it) are dropped, e kept.
        [event(T0 - 190_000, "c"), event(W1, "d"), event(W1 + 1, "e")],
    ]
    rows, watermarks = run(spark, tmp_path, batches, deduplicated)
    assert watermarks == [0, W1, W1]
    assert sorted(r.event_id for r in rows) == ["a", "b", "e"]


@pytest.mark.usefixtures("no_data_batches_off")
def test_the_window_aggregate_drops_rows_whose_window_ends_at_or_before_the_previous_watermark(
    spark: Any, tmp_path: Path
) -> None:
    from flowcast.spark_job import window_counts

    def windowed_without_dedupe(stream: Any) -> Any:
        from pyspark.sql import functions as F

        timed = stream.withColumn("event_ts", F.timestamp_millis("event_time")).withWatermark("event_ts", "3 minutes")
        return window_counts(timed)

    batches = [
        [event(T0)],
        # x's window [T0 - 4 min, W1) ends on batch 1's watermark, but its late filter is batch 0's: kept, and written
        # in this batch, since the window's end is at or before the batch's (eviction) watermark W1.
        [event(T0 - 190_000, "x")],
        # Batch 2's late filter is W1: y's window ends on it (dropped); z's window starts on it (kept).
        [event(T0 - 181_000, "y"), event(W1, "z")],
        [event(T0 + 60_000)],
        # Batch 4 runs with watermark T0 + 1 min - 3 min = W1 + 1 min, the end of z's window, so it's written.
        [event(T0 + 60_000)],
    ]
    rows, watermarks = run(spark, tmp_path, batches, windowed_without_dedupe)
    assert watermarks == [0, W1, W1, W1, W1 + 60_000]
    assert sorted((r.window_start, r.events) for r in rows) == [(W1 - 60_000, 1), (W1, 1)]


@pytest.mark.usefixtures("no_data_batches_off")
def test_in_the_job_the_dedupe_is_the_rule_that_drops(spark: Any, tmp_path: Path) -> None:
    """Chained, the window aggregate's late filter is the same previous-batch watermark as the dedupe's: a row the
    dedupe keeps is never dropped at the window, even when its window ends at or before the batch's own watermark."""
    from flowcast.spark_job import minute_aggregates

    batches = [
        [event(T0, "a")],
        # x: kept, its window ends on W1; v: a page view in the same window, which brings no revenue.
        [event(T0 - 190_000, "x", "purchase") | {"price_cents": 250, "quantity": 3}, event(T0 - 185_000, "v")],
        [event(T0, "a"), event(W1, "y")],  # a again: a duplicate; y: at the dedupe's late watermark, dropped
        [event(T0 + 60_000)],
        [event(T0 + 60_000)],
    ]
    rows, watermarks = run(spark, tmp_path, batches, minute_aggregates)
    assert watermarks == [0, W1, W1, W1, W1 + 60_000]
    # x's window is written with its revenue; y's window (W1 to W1 + 1 min) has nothing in it, so it isn't written.
    assert sorted((r.window_start, r.event_type, r.events, r.revenue_cents) for r in rows) == [
        (W1 - 60_000, "page_view", 1, 0),
        (W1 - 60_000, "purchase", 1, 750),
    ]


@pytest.mark.usefixtures("no_data_batches_off")
def test_the_dedupe_forgets_an_event_id_once_the_watermark_passes_its_time_plus_the_delay(
    spark: Any, tmp_path: Path
) -> None:
    """dropDuplicatesWithinWatermark keeps an event_id only until the watermark passes its event time plus the delay,
    so its state is bounded; dropDuplicates on event_id alone would keep every id forever (the review's surviving
    mutant: the output is the same, only the state differs)."""
    from flowcast.spark_job import deduplicated

    batches = [[event(T0, "a")], [event(T0 + 400_000, "f")], [event(T0 + 400_001, "g")]]
    rows, watermarks = run(spark, tmp_path, batches, deduplicated)
    assert watermarks == [0, W1, T0 + 220_000]  # batch 2's watermark passes a's T0 + 3 min
    assert sorted(r.event_id for r in rows) == ["a", "f", "g"]
    assert run.last_progress["stateOperators"][0]["numRowsTotal"] == 2  # type: ignore[attr-defined]  # f and g
