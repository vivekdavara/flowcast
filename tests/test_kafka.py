"""Against a real broker at FLOWCAST_TEST_BOOTSTRAP (scripts/kafka.sh start locally; a service container in CI).

Each test uses topics of its own, named test-<uuid>, and deletes them.
"""

import os
import uuid
from collections import Counter, defaultdict
from collections.abc import Iterator

import pytest
from confluent_kafka import TopicPartition
from confluent_kafka.admin import AdminClient, ConfigResource

from flowcast.generator import sessions
from flowcast.kafka_io import (
    KafkaError,
    ProducerSettings,
    delete_topic,
    ensure_topic,
    produce,
    read_topic,
    topic_counts,
    wait_for_broker,
)
from flowcast.schema import Event, decode, encode

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


def test_records_the_broker_rejects_make_produce_fail(topic: str) -> None:
    # Every event is over 200 bytes, so a topic that takes at most 100 refuses each record: produce must not succeed.
    ensure_topic(BOOTSTRAP, topic, partitions=1, config={"max.message.bytes": "100"})
    events = [record.event for record in sessions(3)]
    with pytest.raises(KafkaError, match=rf"{len(events)} of {len(events)} records failed; first: .*too large"):
        produce(BOOTSTRAP, topic, events)


def test_a_topic_the_broker_refuses_to_create_is_an_error(topic: str) -> None:
    with pytest.raises(KafkaError, match=f"creating topic {topic} failed: .*retention.ms"):
        ensure_topic(BOOTSTRAP, topic, partitions=1, config={"retention.ms": "not-a-number"})


def test_a_failure_part_way_leaves_exactly_the_records_sent_before_it(topic: str) -> None:
    # From the day-1 review's suspicion: an error from the events while producing must not leave a partial,
    # unflushed queue; what was sent before it is delivered, nothing after.
    ensure_topic(BOOTSTRAP, topic, partitions=2)
    events = [record.event for record in sessions(20)][:10]

    def failing() -> Iterator[Event]:
        yield from events
        raise RuntimeError("the source broke")

    with pytest.raises(RuntimeError, match="the source broke"):
        produce(BOOTSTRAP, topic, failing())
    assert sorted(r.value for r in read_topic(BOOTSTRAP, topic)) == sorted(encode(e) for e in events)


def test_reading_and_counting_start_at_the_low_watermark(topic: str) -> None:
    # The day-1 review's surviving mutants read from offset 0 and counted `high` alone; neither shows until records
    # are deleted from the front of a partition.
    ensure_topic(BOOTSTRAP, topic, partitions=1)
    events = [record.event for record in sessions(10)]
    produce(BOOTSTRAP, topic, events)
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    admin.delete_records([TopicPartition(topic, 0, 5)])[TopicPartition(topic, 0, 5)].result()
    read = read_topic(BOOTSTRAP, topic)
    assert [r.offset for r in read] == list(range(5, len(events)))
    assert [r.value for r in read] == [encode(e) for e in events[5:]]
    assert topic_counts(BOOTSTRAP, topic) == {0: len(events) - 5}


def test_an_empty_config_means_the_brokers_defaults(topic: str) -> None:
    ensure_topic(BOOTSTRAP, topic, partitions=1, config={})
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    resource = ConfigResource("topic", topic)
    configs = admin.describe_configs([resource])[resource].result()
    assert configs["retention.bytes"].value == "-1"  # TOPIC_CONFIG would have set 1 GiB


def test_the_delivery_accounting_matches_the_topic(topic: str) -> None:
    assert ensure_topic(BOOTSTRAP, topic, partitions=6)
    events = [record.event for record in sessions(200)]
    produced = produce(BOOTSTRAP, topic, events, settings=ProducerSettings(linger_ms=20, compression="lz4"))
    assert produced.partitions == topic_counts(BOOTSTRAP, topic)
    read = read_topic(BOOTSTRAP, topic)
    assert produced.bytes == sum(len(r.key or b"") + len(r.value or b"") for r in read)
    assert produced.seconds > 0 and produced.records_per_s == produced.records / produced.seconds
