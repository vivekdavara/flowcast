"""Review of day 2: `flowcast stream` through a real broker, as a user runs it."""

import os
import subprocess
import sys
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

from flowcast.kafka_io import delete_topic, ensure_topic, wait_for_broker
from tests.test_schema import make, raw

BOOTSTRAP = os.environ.get("FLOWCAST_TEST_BOOTSTRAP", "")
pytestmark = [
    pytest.mark.kafka,
    pytest.mark.spark,
    pytest.mark.skipif(not BOOTSTRAP, reason="FLOWCAST_TEST_BOOTSTRAP not set"),
]
CLI = [sys.executable, "-m", "flowcast.cli"]


@pytest.fixture
def topic() -> Iterator[str]:
    wait_for_broker(BOOTSTRAP, timeout_s=60)
    name = f"review-{uuid.uuid4()}"
    ensure_topic(BOOTSTRAP, name, partitions=3)
    yield name
    delete_topic(BOOTSTRAP, name)


def produce_raw(topic: str, values: list[bytes]) -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    for value in values:
        producer.produce(topic, value=value, key=b"u1")
    assert producer.flush(30) == 0


def test_a_failing_query_is_reported_as_flowcast_stream_failed_not_a_traceback(topic: str, tmp_path: Path) -> None:
    # The reviewer's trigger was a record that failed the parse UDF, which 29332c3 fixed; this one is a failure the
    # job is meant to have: records deleted from Kafka before the job read them (retention, or by hand), which
    # failOnDataLoss turns into a failed query in all three queries.
    args = [*CLI, "stream", "--bootstrap", BOOTSTRAP, "--topic", topic, "--out", str(tmp_path / "run")]
    produce_raw(topic, [raw(make())] * 5)
    first = subprocess.run([*args, "--available-now"], capture_output=True, text=True, timeout=300)
    assert first.returncode == 0, first.stderr[-1500:]
    produce_raw(topic, [raw(make())] * 5)
    reader = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"review-{uuid.uuid4()}"})
    ends = [
        TopicPartition(topic, p, reader.get_watermark_offsets(TopicPartition(topic, p), timeout=10)[1])
        for p in range(3)
    ]
    reader.close()
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})  # kept: its futures resolve only while it lives
    for future in admin.delete_records(ends, request_timeout=30).values():
        future.result(timeout=30)
    done = subprocess.run([*args, "--available-now"], capture_output=True, text=True, timeout=300)
    assert done.returncode == 1
    # The CLI's own failure line, not an uncaught StreamingQueryException out of cli.py (Spark's log of the error
    # above it is fine).
    assert 'flowcast/cli.py", line' not in done.stderr, done.stderr[-1500:]
    assert "flowcast stream: queries failed: " in done.stderr
