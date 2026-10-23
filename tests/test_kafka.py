"""Against a real broker at FLOWCAST_TEST_BOOTSTRAP (scripts/kafka.sh start locally; a service container in CI).

Each test uses topics of its own, named test-<uuid>, and deletes them.
"""

import os
import uuid
from collections import Counter, defaultdict
from collections.abc import Iterator

import pytest

from flowcast.generator import sessions
from flowcast.kafka_io import KafkaError, delete_topic, ensure_topic, produce, read_topic, wait_for_broker
from flowcast.schema import decode, encode

BOOTSTRAP = os.environ.get("FLOWCAST_TEST_BOOTSTRAP", "")

pytestmark = [
    pytest.mark.kafka,
    pytest.mark.skipif(not BOOTSTRAP, reason="FLOWCAST_TEST_BOOTSTRAP not set (scripts/kafka.sh start)"),
]


@pytest.fixture(scope="module", autouse=True)
def broker() -> None:
    wait_for_broker(BOOTSTRAP, timeout_s=60)


@pytest.fixture
def topic() -> Iterator[str]:
    name = f"test-{uuid.uuid4()}"
    yield name
    delete_topic(BOOTSTRAP, name)


def test_a_generated_stream_round_trips_through_kafka(topic: str) -> None:
    assert ensure_topic(BOOTSTRAP, topic, partitions=6)
    records = sessions(300)
    events = [record.event for record in records]
    produced = produce(BOOTSTRAP, topic, events)
    assert produced.records == len(events)

    read = read_topic(BOOTSTRAP, topic)
    # Every record arrived once (the generator's duplicates are separate sends and stay), bytes unchanged.
    assert Counter(r.value for r in read) == Counter(encode(e) for e in events)
    assert all(r.key is not None and decode(r.value or b"").user_id == r.key.decode() for r in read)
    # One user's records are in one partition, in the order they were sent.
    partitions: dict[bytes, set[int]] = defaultdict(set)
    in_kafka: dict[bytes, list[bytes]] = defaultdict(list)
    for r in read:
        assert r.key is not None and r.value is not None
        partitions[r.key].add(r.partition)
        in_kafka[r.key].append(r.value)
    assert all(len(p) == 1 for p in partitions.values())
    sent: dict[bytes, list[bytes]] = defaultdict(list)
    for event in events:
        sent[event.user_id.encode()].append(encode(event))
    assert in_kafka == sent
    # Six partitions, and more than one used: ordering across users is not something Kafka gives.
    assert len({r.partition for r in read}) > 1


def test_ensure_topic_is_idempotent_and_checks_the_partition_count(topic: str) -> None:
    assert ensure_topic(BOOTSTRAP, topic, partitions=3)
    assert not ensure_topic(BOOTSTRAP, topic, partitions=3)
    with pytest.raises(KafkaError, match="has 3 partitions, not 6"):
        ensure_topic(BOOTSTRAP, topic, partitions=6)


def test_reading_an_empty_topic_returns_nothing(topic: str) -> None:
    ensure_topic(BOOTSTRAP, topic, partitions=2)
    assert read_topic(BOOTSTRAP, topic) == []


def test_reading_a_missing_topic_is_an_error() -> None:
    with pytest.raises(KafkaError, match="no topic"):
        read_topic(BOOTSTRAP, f"test-missing-{uuid.uuid4()}", timeout_s=10)


def test_producing_to_a_missing_topic_fails_rather_than_creating_it() -> None:
    # The broker refuses auto-creation (auto.create.topics.enable=false), so the records can't be delivered.
    name = f"test-missing-{uuid.uuid4()}"
    events = [record.event for record in sessions(1)]
    with pytest.raises(KafkaError):
        produce(BOOTSTRAP, name, events, flush_timeout_s=5)
    with pytest.raises(KafkaError, match="no topic"):
        read_topic(BOOTSTRAP, name, timeout_s=10)


def test_deleting_a_missing_topic_is_not_an_error() -> None:
    delete_topic(BOOTSTRAP, f"test-missing-{uuid.uuid4()}")


def test_no_broker_is_reported_after_the_timeout() -> None:
    with pytest.raises(KafkaError, match=r"no Kafka broker at 127\.0\.0\.1:1 "):
        wait_for_broker("127.0.0.1:1", timeout_s=1)
