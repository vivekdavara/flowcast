"""Review of 943596b: kafka_io.py against a real broker at FLOWCAST_TEST_BOOTSTRAP (topics review-<uuid>, deleted)."""

import os
import threading
import uuid

import pytest

from flowcast.kafka_io import delete_topic, ensure_topic, wait_for_broker

BOOTSTRAP = os.environ.get("FLOWCAST_TEST_BOOTSTRAP", "")

pytestmark = [
    pytest.mark.kafka,
    pytest.mark.skipif(not BOOTSTRAP, reason="FLOWCAST_TEST_BOOTSTRAP not set (scripts/kafka.sh start)"),
]


def test_ensure_topic_racing_another_creator_checks_the_topic_it_lost_to() -> None:
    # The loser of a create race gets TOPIC_ALREADY_EXISTS and re-reads metadata, but the broker's metadata cache can
    # lag the controller, so _partition_count returns None and ensure_topic raises "topic ... has None partitions,
    # not 6" (seen in 1 of 5 races of 4 threads). Expected: wait for the topic to appear, then check its count.
    wait_for_broker(BOOTSTRAP, timeout_s=30)
    errors: list[str] = []
    for _ in range(15):
        name = f"review-{uuid.uuid4()}"

        def create(name: str = name) -> None:
            try:
                ensure_topic(BOOTSTRAP, name, partitions=6)
            except Exception as exc:
                errors.append(repr(exc))

        threads = [threading.Thread(target=create) for _ in range(6)]
        try:
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
        finally:
            delete_topic(BOOTSTRAP, name)
    assert errors == []


def test_a_topic_ensure_topic_just_created_can_be_read_and_counted_at_once() -> None:
    # ensure_topic returns once the controller has the topic, before the broker serves its partitions, and
    # _watermarks treats the transient NOT_LEADER_FOR_PARTITION as final: "no offsets for ... partition 0: Failed to
    # get watermark offsets: Broker: Not leader for partition" (7 of 60 reads/counts of 30 new topics). So
    # `flowcast topic` then `flowcast count`, and test_reading_an_empty_topic_returns_nothing, are flaky. Expected:
    # ensure_topic waits until every partition has a leader, or _watermarks retries transient errors within timeout_s.
    from flowcast.kafka_io import KafkaError, read_topic, topic_counts

    wait_for_broker(BOOTSTRAP, timeout_s=30)
    errors: list[str] = []
    for _ in range(20):
        name = f"review-{uuid.uuid4()}"
        try:
            ensure_topic(BOOTSTRAP, name, partitions=6)
            try:
                assert topic_counts(BOOTSTRAP, name, timeout_s=10) == dict.fromkeys(range(6), 0)
                assert read_topic(BOOTSTRAP, name, timeout_s=10) == []
            except KafkaError as exc:
                errors.append(str(exc))
        finally:
            delete_topic(BOOTSTRAP, name)
    assert errors == []
