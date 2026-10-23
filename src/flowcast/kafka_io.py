"""Kafka: the topic, an idempotent producer for generated events, and a reader for a snapshot of a topic.

DESIGN.md "Kafka". Records are keyed by user_id, so one user's events stay in one partition, in the order sent.
"""

from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass

from confluent_kafka import Consumer, KafkaException, Message, Producer, TopicPartition
from confluent_kafka import KafkaError as RdKafkaError
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic  # confluent_kafka.admin re-exports it without declaring it public

from flowcast.schema import Event, encode

DEFAULT_BOOTSTRAP = "127.0.0.1:9192"
TOPIC = "clickstream"
PARTITIONS = 6
# Bounded by time and by size per partition, so a long benchmark can't fill a laptop disk.
TOPIC_CONFIG = {"retention.ms": str(7 * 24 * 3600 * 1000), "retention.bytes": str(1 << 30)}

# librdkafka reports every refused connection itself; route that to logging rather than stderr.
_log = logging.getLogger("flowcast.kafka")


class KafkaError(Exception):
    """A Kafka operation that didn't succeed, with what to do about it where that's known."""


def _client_config(bootstrap: str) -> dict[str, object]:
    return {"bootstrap.servers": bootstrap, "logger": _log}


def _admin(bootstrap: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap}, logger=_log)


def wait_for_broker(bootstrap: str, timeout_s: float = 60.0) -> None:
    """Return once the broker answers a metadata request; raise KafkaError after `timeout_s`."""
    admin = _admin(bootstrap)
    deadline = time.monotonic() + timeout_s
    while True:
        remaining = deadline - time.monotonic()
        try:
            admin.list_topics(timeout=max(0.1, min(5.0, remaining)))
            return
        except KafkaException as exc:
            if time.monotonic() >= deadline:
                raise KafkaError(f"no Kafka broker at {bootstrap} after {timeout_s:g}s ({exc})") from exc
            time.sleep(min(0.5, max(0.0, deadline - time.monotonic())))


def ensure_topic(
    bootstrap: str,
    name: str = TOPIC,
    partitions: int = PARTITIONS,
    config: dict[str, str] | None = None,
    timeout_s: float = 30.0,
) -> bool:
    """Create the topic if it doesn't exist; True if this call created it. An existing topic with a different
    partition count is an error: key-to-partition mapping depends on it, so it isn't silently accepted."""
    admin = _admin(bootstrap)
    existing = _partition_count(admin, name, timeout_s)
    if existing is None:
        topic = NewTopic(name, num_partitions=partitions, replication_factor=1, config=config or TOPIC_CONFIG)
        try:
            admin.create_topics([topic], operation_timeout=timeout_s, request_timeout=timeout_s)[name].result()
            return True
        except KafkaException as exc:
            if exc.args[0].code() != RdKafkaError.TOPIC_ALREADY_EXISTS:
                raise KafkaError(f"creating topic {name} failed: {exc.args[0].str()}") from exc
            # Someone else created it between our check and our create: check it like any existing topic.
            existing = _partition_count(admin, name, timeout_s)
    if existing != partitions:
        raise KafkaError(f"topic {name} has {existing} partitions, not {partitions}")
    return False


def delete_topic(bootstrap: str, name: str, timeout_s: float = 30.0) -> None:
    admin = _admin(bootstrap)
    try:
        admin.delete_topics([name], operation_timeout=timeout_s, request_timeout=timeout_s)[name].result()
    except KafkaException as exc:
        if exc.args[0].code() != RdKafkaError.UNKNOWN_TOPIC_OR_PART:
            raise KafkaError(f"deleting topic {name} failed: {exc.args[0].str()}") from exc


def _partition_count(admin: AdminClient, name: str, timeout_s: float) -> int | None:
    try:
        metadata = admin.list_topics(topic=name, timeout=timeout_s)
    except KafkaException as exc:
        raise KafkaError(f"no answer from Kafka: {exc.args[0].str()}") from exc
    topic = metadata.topics.get(name)
    if topic is None or (topic.error is not None and topic.error.code() == RdKafkaError.UNKNOWN_TOPIC_OR_PART):
        return None
    if topic.error is not None:
        raise KafkaError(f"topic {name}: {topic.error.str()}")
    return len(topic.partitions)


@dataclass(frozen=True)
class Produced:
    records: int
    seconds: float


def produce(bootstrap: str, topic: str, events: Iterable[Event], flush_timeout_s: float = 60.0) -> Produced:
    """Send each event as one record (key user_id, value its wire form), in order, and wait until every one is
    acknowledged. Raises KafkaError if any record wasn't delivered."""
    producer = Producer(
        {
            **_client_config(bootstrap),
            # Idempotence implies acks=all and at most 5 requests in flight per partition, with sequence numbers, so a
            # retried batch can't duplicate or reorder records within a partition.
            "enable.idempotence": True,
            "linger.ms": 5,
        }
    )
    delivered = 0
    failures: list[str] = []

    def on_delivery(err: RdKafkaError | None, _msg: Message) -> None:
        nonlocal delivered
        if err is not None:
            failures.append(err.str())
        else:
            delivered += 1

    sent = 0
    started = time.perf_counter()
    for event in events:
        value = encode(event)
        while True:
            try:
                producer.produce(topic, value=value, key=event.user_id.encode(), on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.05)  # the local queue is full: serve delivery reports until there is room
        sent += 1
        producer.poll(0)
    left = producer.flush(flush_timeout_s)
    seconds = time.perf_counter() - started
    if failures:
        raise KafkaError(f"{len(failures)} of {sent} records failed; first: {failures[0]}")
    if left or delivered != sent:
        raise KafkaError(f"only {delivered} of {sent} records acknowledged within {flush_timeout_s:g}s")
    return Produced(sent, seconds)


@dataclass(frozen=True, slots=True)
class Record:
    partition: int
    offset: int
    key: bytes | None
    value: bytes | None
    timestamp_ms: int


def read_topic(bootstrap: str, topic: str, timeout_s: float = 60.0) -> list[Record]:
    """Every record in the topic up to the high-water marks at the time of the call, ordered by partition and offset.

    No consumer group is joined and no offsets are committed: partitions are assigned directly and read to the end
    offsets taken at the start, so the read always ends and leaves nothing behind.
    """
    consumer = Consumer(
        {
            **_client_config(bootstrap),
            "group.id": f"flowcast-reader-{uuid.uuid4()}",  # required by the client; never joined or committed to
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "enable.partition.eof": False,
        }
    )
    try:
        metadata = consumer.list_topics(topic=topic, timeout=timeout_s)
        info = metadata.topics.get(topic)
        if info is None or info.error is not None:
            raise KafkaError(f"no topic {topic}")
        ends: dict[int, int] = {}
        starts: dict[int, int] = {}
        for partition in sorted(info.partitions):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=timeout_s)
            if high > low:
                starts[partition], ends[partition] = low, high
        consumer.assign([TopicPartition(topic, p, starts[p]) for p in ends])
        records: list[Record] = []
        position = dict(starts)
        deadline = time.monotonic() + timeout_s
        while any(position[p] < ends[p] for p in ends):
            if time.monotonic() >= deadline:
                missing = sum(ends[p] - position[p] for p in ends)
                raise KafkaError(f"read of {topic} timed out with {missing} records to go")
            for message in consumer.consume(num_messages=1000, timeout=0.5):
                error = message.error()
                if error is not None:
                    raise KafkaError(f"reading {topic}: {error.str()}")
                partition, offset = message.partition(), message.offset()
                assert partition is not None and offset is not None
                if offset >= ends[partition]:
                    continue  # written after the snapshot was taken
                _, timestamp = message.timestamp()
                records.append(Record(partition, offset, message.key(), message.value(), timestamp))
                position[partition] = offset + 1
    finally:
        consumer.close()
    records.sort(key=lambda r: (r.partition, r.offset))
    return records
