"""batching.py against the real job: Spark cuts the backlog where the model says, and drops the records it predicts,
on time though every one of them is."""

import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from flowcast.batching import offsets_of, plan, simulate
from flowcast.check import committed_batches, kafka_end_offsets, replay
from flowcast.generator import Config, sessions
from flowcast.kafka_io import delete_topic, ensure_topic, produce, read_topic, wait_for_broker

BOOTSTRAP = os.environ.get("FLOWCAST_TEST_BOOTSTRAP", "")

pytestmark = [
    pytest.mark.kafka,
    pytest.mark.spark,
    pytest.mark.skipif(not BOOTSTRAP, reason="FLOWCAST_TEST_BOOTSTRAP not set (scripts/kafka.sh start)"),
]

# Sparse (a session every 5 s) and orderly: nothing late, nothing duplicated, so every drop is the split's doing.
CONFIG = Config(seed=4, sessions_per_second=0.2, late_fraction=0, very_late_fraction=0, duplicate_fraction=0)
SIZE = 60


@pytest.fixture
def topic() -> Iterator[str]:
    wait_for_broker(BOOTSTRAP, timeout_s=60)
    name = f"test-{uuid.uuid4()}"
    ensure_topic(BOOTSTRAP, name, partitions=6)
    yield name
    delete_topic(BOOTSTRAP, name)


def test_the_model_predicts_the_jobs_batches_and_late_drops(spark: Any, topic: str, tmp_path: Path) -> None:
    from flowcast.layout import Paths
    from flowcast.spark_job import kafka_records, start

    produce(BOOTSTRAP, topic, (r.event for r in sessions(120, CONFIG)))
    queries = start(
        spark, kafka_records(spark, BOOTSTRAP, topic, max_offsets_per_trigger=SIZE), Paths(tmp_path), available_now=True
    )
    queries.await_termination()

    records = read_topic(BOOTSTRAP, topic)
    batches = committed_batches(Paths(tmp_path).checkpoint("minute_aggregates"))
    start_offsets, end_offsets = offsets_of(records)
    planned = plan(SIZE, start_offsets, end_offsets)
    actual = [kafka_end_offsets(b, topic) for b in batches]
    assert actual[: len(planned)] == planned
    assert all(ends == end_offsets for ends in actual[len(planned) :])  # then no-data batches, for the watermark

    job = replay(records, batches, topic)
    model = simulate(records, SIZE, topic)
    assert job.watermark_errors == [] and job.unread == 0
    assert sorted(e.event_id for e in job.dropped) == sorted(e.event_id for e in model.dropped)
    assert job.dropped, "on-time records dropped by the split: the case this test is about"
