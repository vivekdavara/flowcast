"""The whole job through a real broker, checked exactly: the minute aggregates it wrote must be the truth of the records
its own batches had to keep (check.py), and every record must come out as a raw event or a dead letter."""

import json
import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import pytest
from confluent_kafka import Producer

from flowcast.check import committed_batches, expected_rows, replay
from flowcast.generator import Config, sessions
from flowcast.kafka_io import delete_topic, ensure_topic, produce, read_topic, wait_for_broker
from flowcast.truth import Row, minute_aggregates, minute_aggregates_sql
from flowcast.views import connect
from tests.test_schema import make, raw

BOOTSTRAP = os.environ.get("FLOWCAST_TEST_BOOTSTRAP", "")

pytestmark = [
    pytest.mark.kafka,
    pytest.mark.spark,
    pytest.mark.skipif(not BOOTSTRAP, reason="FLOWCAST_TEST_BOOTSTRAP not set (scripts/kafka.sh start)"),
]

# Sessions spread thin (one every 5 s) so 300 of them span about 25 minutes of event time and the watermark passes
# most windows; many late, very late and duplicate records, so batch boundaries matter.
CONFIG = Config(seed=11, sessions_per_second=0.2, late_fraction=0.2, very_late_fraction=0.05, duplicate_fraction=0.05)
BAD = [b"{", b"null", raw(make(), extra=1), raw(make(), quantity=2.0)]


@pytest.fixture
def topic() -> Iterator[str]:
    wait_for_broker(BOOTSTRAP, timeout_s=60)
    name = f"test-{uuid.uuid4()}"
    ensure_topic(BOOTSTRAP, name, partitions=6)
    yield name
    delete_topic(BOOTSTRAP, name)


def produce_raw(topic: str, values: list[bytes]) -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    for value in values:
        producer.produce(topic, value=value, key=b"u1")
    assert producer.flush(30) == 0


def run(spark: Any, topic: str, out: Path, *, duckdb: bool = False) -> None:
    """One run of the job over what's in the topic now, 150 records a batch, resuming from its checkpoints."""
    from flowcast.layout import Paths
    from flowcast.spark_job import kafka_records, start

    records = kafka_records(spark, BOOTSTRAP, topic, max_offsets_per_trigger=150)
    start(spark, records, Paths(out), available_now=True, duckdb=duckdb).await_termination()


def test_the_job_writes_exactly_what_the_replay_of_its_batches_says(spark: Any, topic: str, tmp_path: Path) -> None:
    from flowcast.layout import Paths

    events = [r.event for r in sessions(300, CONFIG)]
    half = len(events) // 2
    produce(BOOTSTRAP, topic, events[:half])
    produce_raw(topic, BAD)
    run(spark, topic, tmp_path)
    produce(BOOTSTRAP, topic, events[half:])
    run(spark, topic, tmp_path)  # a second run, resuming from the first one's checkpoints

    paths = Paths(tmp_path)
    in_kafka = read_topic(BOOTSTRAP, topic)
    batches = committed_batches(paths.checkpoint("minute_aggregates"))
    result = replay(in_kafka, batches, topic)
    assert len(batches) >= len(in_kafka) // 150
    assert result.unread == 0 and result.invalid == len(BAD)
    assert result.watermark_errors == []  # the job ran with the specified 3-minute delay
    assert len(result.dropped) > 10, "the late rule was exercised"

    written = sorted(
        Row(r.window_start, r.event_type, r.events, r.revenue_cents)
        for r in spark.read.parquet(str(paths.minute_aggregates)).collect()
    )
    assert written == expected_rows(result)
    assert len(written) > 50
    # And the bound that holds whatever the batches were: never more than the truth of every event.
    truth = {(r.window_start, r.event_type): r for r in minute_aggregates(events)}
    assert all(r.events <= truth[r.window_start, r.event_type].events for r in written)
    assert any(r.events < truth[r.window_start, r.event_type].events for r in written)

    raw_events = spark.read.parquet(str(paths.raw_events))
    assert raw_events.count() == len(events)  # every valid record, duplicates and very late ones included
    assert raw_events.select("partition", "offset").distinct().count() == len(events)
    letters = spark.read.parquet(str(paths.dead_letters)).collect()
    assert sorted(bytes(r.value) for r in letters) == sorted(BAD)

    # DuckDB's views (built from the sinks' logs) see the same rows; and the truth's SQL over the raw events gives the
    # truth of every event, very late ones included: the raw output lost nothing.
    con = connect(tmp_path)
    try:
        aggregates = con.execute("SELECT window_start, event_type, events, revenue_cents FROM minute_aggregates")
        assert sorted(Row(*r) for r in aggregates.fetchall()) == written
        counts = con.execute('SELECT count(*), count(DISTINCT (partition, "offset")) FROM raw_events').fetchone()
        assert counts == (len(events), len(events))
        mismatched = (
            "SELECT count(*) FROM raw_events WHERE event_date <> CAST(make_timestamp(event_time * 1000) AS DATE)"
        )
        assert con.execute(mismatched).fetchone() == (0,)
        assert sorted(bytes(v) for (v,) in con.execute("SELECT value FROM dead_letters").fetchall()) == sorted(BAD)
        input_order = 'row_number() OVER (ORDER BY partition, "offset") AS input_order'
        con.execute(f"CREATE VIEW events AS SELECT {input_order}, * FROM raw_events")
        truth_sql = [
            Row(int(a), str(b), int(c), int(d)) for a, b, c, d in con.execute(minute_aggregates_sql()).fetchall()
        ]
        assert truth_sql == minute_aggregates(events)
    finally:
        con.close()


def test_the_cli_runs_the_job_checks_it_and_queries_it(topic: str, tmp_path: Path) -> None:
    events = [r.event for r in sessions(120, CONFIG)]
    produce(BOOTSTRAP, topic, events)
    out = tmp_path / "run"
    cli = [sys.executable, "-m", "flowcast.cli"]
    kafka = ["--bootstrap", BOOTSTRAP, "--topic", topic]

    stream = [*cli, "stream", *kafka, "--out", str(out), "--available-now", "--max-offsets", "200"]
    done = subprocess.run(stream, capture_output=True, text=True, timeout=300)
    assert done.returncode == 0, done.stderr[-8000:]
    check = subprocess.run([*cli, "check", *kafka, "--out", str(out)], capture_output=True, text=True, timeout=120)
    assert check.returncode == 0, check.stdout + check.stderr
    assert check.stdout.splitlines()[-1] == "ok"
    assert f"raw_events: {len(events)} written, {len(events)} valid records read" in check.stdout
    # The backfill: published, then held to the truth of every raw record by the same check.
    backfill = subprocess.run([*cli, "backfill", "--out", str(out)], capture_output=True, text=True, timeout=300)
    assert backfill.returncode == 0, backfill.stderr[-8000:]
    assert f"backfill: {len(minute_aggregates(events))} rows, {len(set(e.event_id for e in events))} events" in (
        backfill.stdout
    )
    check = subprocess.run([*cli, "check", *kafka, "--out", str(out)], capture_output=True, text=True, timeout=120)
    assert check.returncode == 0, check.stdout + check.stderr
    assert "in the truth of every raw record: equal" in check.stdout
    query = [*cli, "query", "--out", str(out), "SELECT count(*) AS n, sum(events) > 0 AS any FROM raw_events, ("]
    assert subprocess.run(query, capture_output=True, text=True).returncode == 1  # bad SQL: exit 1
    query[-1] = "SELECT count(*) AS n FROM raw_events"
    assert subprocess.run(query, capture_output=True, text=True).stdout == f"n\n{len(events)}\n"

    # An aggregate row the replay doesn't explain, committed in the sink's log as Spark would: the check must fail.
    sink = out / "minute_aggregates"
    forged = sink / "part-forged.parquet"
    row = "SELECT 0::BIGINT AS window_start, 'page_view' AS event_type, 1::BIGINT AS events, 0::BIGINT AS revenue_cents"
    with duckdb.connect() as con:
        con.execute(f"COPY ({row}) TO '{forged}'")
    last = max((p for p in (sink / "_spark_metadata").iterdir() if p.name.isdigit()), key=lambda p: int(p.name))
    entry = {"path": forged.as_uri(), "size": forged.stat().st_size, "isDir": False, "action": "add"}
    last.write_text(last.read_text().rstrip("\n") + "\n" + json.dumps(entry))
    check = subprocess.run([*cli, "check", *kafka, "--out", str(out)], capture_output=True, text=True, timeout=120)
    assert check.returncode == 1
    assert "1 unexpected" in check.stdout and "MISMATCH" in check.stdout
