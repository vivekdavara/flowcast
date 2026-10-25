"""The batch backfill: the stream's own transformations over every raw event must give the truth of every event."""

import dataclasses
import json
import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from flowcast import backfill
from flowcast.check import check_run
from flowcast.generator import Config, sessions
from flowcast.kafka_io import delete_topic, ensure_topic, produce, wait_for_broker
from flowcast.layout import LayoutError, Paths, current_backfill
from flowcast.schema import V1_FIELDS, Event, values
from flowcast.truth import Row, minute_aggregates
from flowcast.views import connect

BOOTSTRAP = os.environ.get("FLOWCAST_TEST_BOOTSTRAP", "")
CONFIG = Config(seed=31, sessions_per_second=0.2, late_fraction=0.2, very_late_fraction=0.05, duplicate_fraction=0.05)


def written(spark: Any, directory: Path) -> list[Row]:
    return sorted(
        Row(r.window_start, r.event_type, r.events, r.revenue_cents)
        for r in spark.read.parquet(str(directory)).collect()
    )


def log_raw_files(paths: Paths, batch_id: int, directory: Path) -> None:
    """List a directory's Parquet files in the raw sink's log as one batch, as Spark's file sink does."""
    log = paths.raw_events / "_spark_metadata"
    log.mkdir(parents=True, exist_ok=True)
    entries = [
        json.dumps({"path": f.as_uri(), "size": f.stat().st_size, "isDir": False, "action": "add"})
        for f in sorted(directory.glob("*.parquet"))
    ]
    (log / str(batch_id)).write_text("\n".join(["v1", *entries]))


@pytest.mark.spark
def test_raw_files_from_before_and_after_schema_version_2_backfill_to_the_truth(spark: Any, tmp_path: Path) -> None:
    from pyspark.sql.types import StructType

    from flowcast.backfill import RAW_SCHEMA

    v1 = [r.event for r in sessions(60, CONFIG)]
    v2 = [r.event for r in sessions(60, dataclasses.replace(CONFIG, seed=32, schema_version=2))]
    assert any(e.campaign for e in v2)
    paths = Paths(tmp_path)
    coordinates = ("clickstream", 0, 0, None)
    # Written by a job from before version 2: the files have no campaign column at all.
    old_schema = StructType(
        [RAW_SCHEMA[name] for name in (*V1_FIELDS, "topic", "partition", "offset", "kafka_timestamp")]
    )
    old = paths.raw_events / "event_date=2026-01-01" / "old"
    spark.createDataFrame([(*values(e)[: len(V1_FIELDS)], *coordinates) for e in v1], old_schema).write.parquet(
        str(old)
    )
    new = paths.raw_events / "event_date=2026-01-01" / "new"
    spark.createDataFrame([(*values(e), *coordinates) for e in v2], RAW_SCHEMA).write.parquet(str(new))
    log_raw_files(paths, 0, old)
    log_raw_files(paths, 1, new)

    events = backfill.raw_events(spark, paths)
    campaigns = {r.event_id: r.campaign for r in events.select("event_id", "campaign").collect()}
    assert all(campaigns[e.event_id] is None for e in v1)
    assert all(campaigns[e.event_id] == e.campaign for e in v2)
    out = backfill.run(spark, paths)
    assert current_backfill(paths) == out
    assert written(spark, out) == minute_aggregates([*v1, *v2])


def test_a_run_with_no_raw_events_isnt_backfilled(tmp_path: Path) -> None:
    with pytest.raises(backfill.BackfillError, match="no committed raw events"):
        backfill.raw_events(None, Paths(tmp_path))  # type: ignore[arg-type]
    assert current_backfill(Paths(tmp_path)) is None


@pytest.mark.parametrize("name", ["", "../elsewhere", ".hidden"])
def test_a_pointer_that_names_no_backfill_directory_is_refused(tmp_path: Path, name: str) -> None:
    paths = Paths(tmp_path)
    paths.backfill.mkdir()
    (paths.backfill / "CURRENT").write_text(name)
    with pytest.raises(LayoutError, match="not a backfill directory name"):
        current_backfill(paths)


@pytest.fixture
def topic() -> Iterator[str]:
    wait_for_broker(BOOTSTRAP, timeout_s=60)
    name = f"test-{uuid.uuid4()}"
    ensure_topic(BOOTSTRAP, name, partitions=6)
    yield name
    delete_topic(BOOTSTRAP, name)


@pytest.mark.kafka
@pytest.mark.spark
@pytest.mark.skipif(not BOOTSTRAP, reason="FLOWCAST_TEST_BOOTSTRAP not set (scripts/kafka.sh start)")
def test_the_backfill_of_a_streamed_run_is_the_truth_very_late_events_included(
    spark: Any, topic: str, tmp_path: Path
) -> None:
    from tests.test_spark_stream import run

    # The producers upgrade to schema version 2 part way: the job reads both, and the backfill counts both.
    v1: list[Event] = [r.event for r in sessions(120, CONFIG)]
    v2: list[Event] = [r.event for r in sessions(120, dataclasses.replace(CONFIG, seed=33, schema_version=2))]
    events = v1 + v2
    produce(BOOTSTRAP, topic, v1)
    produce(BOOTSTRAP, topic, v2)
    run(spark, topic, tmp_path)
    paths = Paths(tmp_path)
    report = check_run(BOOTSTRAP, topic, tmp_path)
    assert report.ok and report.replay.invalid == 0, "\n".join(report.lines())
    con = connect(tmp_path)
    try:
        campaigns = dict(con.execute("SELECT event_id, campaign FROM raw_events").fetchall())
    finally:
        con.close()
    assert {e.event_id: e.campaign for e in events} == campaigns and any(campaigns.values())

    first = backfill.run(spark, paths)
    truth = minute_aggregates(events)
    assert written(spark, first) == truth

    # The stream dropped the late records the backfill counts: never more than the backfill, in some windows less.
    stream = {(r.window_start, r.event_type): r.events for r in written(spark, paths.minute_aggregates)}
    full = {(r.window_start, r.event_type): r.events for r in truth}
    assert all(n <= full[key] for key, n in stream.items())
    assert any(n < full[key] for key, n in stream.items())

    # A second backfill is a new directory, published by the pointer; the first stays whole until it's replaced.
    second = backfill.run(spark, paths)
    assert second != first and current_backfill(paths) == second
    assert written(spark, second) == written(spark, first)
    assert not list(paths.backfill.glob(".CURRENT.*"))
