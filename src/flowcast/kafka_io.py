"""Kafka: the topic, an idempotent producer for generated events, and a reader for a snapshot of a topic.

DESIGN.md "Kafka". Records are keyed by user_id, so one user's events stay in one partition, in the order sent.
"""

from __future__ import annotations

import logging
import math
import time
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field

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

# Kafka's partition count is a 32-bit signed integer.
MAX_PARTITIONS = 2**31 - 1
# Errors a partition gives for a moment after its topic is created, while the broker takes up leadership.
_SETTLING = frozenset({RdKafkaError.NOT_LEADER_FOR_PARTITION, RdKafkaError.LEADER_NOT_AVAILABLE})
_POLL_S = 0.1

# librdkafka reports every refused connection itself; route that to logging rather than stderr.
_log = logging.getLogger("flowcast.kafka")


class KafkaError(Exception):
    """A Kafka operation that didn't succeed, with what to do about it where that's known."""


def _client_config(bootstrap: str) -> dict[str, object]:
    return {"bootstrap.servers": bootstrap, "logger": _log}


def _admin(bootstrap: str) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap}, logger=_log)


def _check_timeout(timeout_s: float) -> None:
    # NaN compares false with everything, so a NaN deadline never passes; infinity never does either.
    if not math.isfinite(timeout_s) or timeout_s < 0:
        raise ValueError("timeout_s must be a finite number of seconds, at least 0")


def _left(deadline: float) -> float:
    """Seconds to the deadline, at least 0.1 (a client call with a timeout of 0 doesn't wait for an answer)."""
    return max(0.1, deadline - time.monotonic())


def wait_for_broker(bootstrap: str, timeout_s: float = 60.0) -> None:
    """Return once the broker answers a metadata request; raise KafkaError after `timeout_s`."""
    _check_timeout(timeout_s)
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
    """Create the topic if it doesn't exist; True if this call created it. Returns once the broker serves every
    partition (a leader for each), so the topic can be written and read at once. An existing topic with a different
    partition count is an error: key-to-partition mapping depends on it, so it isn't silently accepted. `config`
    None means TOPIC_CONFIG; pass {} for the broker's defaults."""
    if not 1 <= partitions <= MAX_PARTITIONS:
        raise ValueError(f"partitions must be between 1 and {MAX_PARTITIONS}")
    _check_timeout(timeout_s)
    admin = _admin(bootstrap)
    deadline = time.monotonic() + timeout_s
    created = False
    if _served_partitions(admin, name, _left(deadline)) is None:
        topic = NewTopic(
            name,
            num_partitions=partitions,
            replication_factor=1,
            config=TOPIC_CONFIG if config is None else config,
        )
        try:
            left = _left(deadline)
            admin.create_topics([topic], operation_timeout=left, request_timeout=left)[name].result()
            created = True
        except KafkaException as exc:
            if exc.args[0].code() != RdKafkaError.TOPIC_ALREADY_EXISTS:
                raise KafkaError(f"creating topic {name} failed: {exc.args[0].str()}") from exc
            # Someone else created it between our check and our create: check it like any existing topic.
    # The controller has the topic once the create returns, but the broker's metadata (and its leadership of each
    # partition) can lag it: a racing creator's re-read, or a reader right after, could find no topic yet.
    while (existing := _served_partitions(admin, name, _left(deadline))) is None:
        if time.monotonic() >= deadline:
            raise KafkaError(f"topic {name} isn't served by the broker after {timeout_s:g}s")
        time.sleep(_POLL_S)
    if existing != partitions:
        raise KafkaError(f"topic {name} has {existing} partitions, not {partitions}")
    return created


def delete_topic(bootstrap: str, name: str, timeout_s: float = 30.0) -> None:
    _check_timeout(timeout_s)
    admin = _admin(bootstrap)
    try:
        admin.delete_topics([name], operation_timeout=timeout_s, request_timeout=timeout_s)[name].result()
    except KafkaException as exc:
        if exc.args[0].code() != RdKafkaError.UNKNOWN_TOPIC_OR_PART:
            raise KafkaError(f"deleting topic {name} failed: {exc.args[0].str()}") from exc


def _served_partitions(admin: AdminClient, name: str, timeout_s: float) -> int | None:
    """The topic's partition count once every partition has a leader; None while it doesn't exist (or isn't served
    yet)."""
    try:
        metadata = admin.list_topics(topic=name, timeout=timeout_s)
    except KafkaException as exc:
        raise KafkaError(f"no answer from Kafka: {exc.args[0].str()}") from exc
    topic = metadata.topics.get(name)
    if topic is None or (topic.error is not None and topic.error.code() == RdKafkaError.UNKNOWN_TOPIC_OR_PART):
        return None
    if topic.error is not None and topic.error.code() not in _SETTLING:
        raise KafkaError(f"topic {name}: {topic.error.str()}")
    if topic.error is not None or not topic.partitions or any(p.leader < 0 for p in topic.partitions.values()):
        return None
    return len(topic.partitions)


COMPRESSION = ("none", "gzip", "snappy", "lz4", "zstd")
# librdkafka's limits for linger.ms and batch.size.
MAX_LINGER_MS = 900_000
MAX_BATCH_BYTES = 2_147_483_647


@dataclass(frozen=True)
class ProducerSettings:
    """How the producer batches: wait up to `linger_ms` for a batch to fill, up to `batch_bytes` per partition batch,
    compressed with `compression`. The defaults are librdkafka's own."""

    linger_ms: float = 5.0
    batch_bytes: int = 1_000_000
    compression: str = "none"

    def __post_init__(self) -> None:
        if not (math.isfinite(self.linger_ms) and 0 <= self.linger_ms <= MAX_LINGER_MS):
            raise ValueError(f"linger_ms must be between 0 and {MAX_LINGER_MS}")
        if not 1 <= self.batch_bytes <= MAX_BATCH_BYTES:
            raise ValueError(f"batch_bytes must be between 1 and {MAX_BATCH_BYTES}")
        if self.compression not in COMPRESSION:
            raise ValueError(f"compression must be one of {', '.join(COMPRESSION)}")

    def config(self) -> dict[str, object]:
        return {"linger.ms": self.linger_ms, "batch.size": self.batch_bytes, "compression.type": self.compression}


@dataclass(frozen=True)
class Produced:
    """What a produce call sent: every record was acknowledged, or it raised."""

    records: int
    seconds: float  # from the first send to the last acknowledgement
    bytes: int = 0  # keys and values, before compression
    partitions: dict[int, int] = field(default_factory=dict)  # acknowledged records by partition

    @property
    def records_per_s(self) -> float:
        return self.records / self.seconds if self.seconds > 0 else 0.0


def produce(
    bootstrap: str,
    topic: str,
    events: Iterable[Event],
    flush_timeout_s: float = 60.0,
    settings: ProducerSettings | None = None,
) -> Produced:
    """Send each event as one record (key user_id, value its wire form), in order, and wait until every one is
    acknowledged. Raises KafkaError if any record wasn't delivered. `events` may pace itself (pacing.Pacer)."""
    _check_timeout(flush_timeout_s)
    producer = Producer(
        {
            **_client_config(bootstrap),
            # Idempotence implies acks=all and at most 5 requests in flight per partition, with sequence numbers, so a
            # retried batch can't duplicate or reorder records within a partition.
            "enable.idempotence": True,
            **(settings or ProducerSettings()).config(),
        }
    )
    delivered = 0
    failures: list[str] = []
    partitions: dict[int, int] = {}

    def on_delivery(err: RdKafkaError | None, msg: Message) -> None:
        nonlocal delivered
        if err is not None:
            failures.append(err.str())
        else:
            delivered += 1
            partition = msg.partition()
            assert partition is not None
            partitions[partition] = partitions.get(partition, 0) + 1

    sent = sent_bytes = 0
    started = time.perf_counter()
    try:
        for event in events:
            value = encode(event)
            key = event.user_id.encode()
            while True:
                try:
                    producer.produce(topic, value=value, key=key, on_delivery=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.05)  # the local queue is full: serve delivery reports until there is room
            sent += 1
            sent_bytes += len(value) + len(key)
            producer.poll(0)
    except BaseException as exc:
        # The events failed part way (a bad event, an interrupt): deliver what was already sent before raising, so
        # the topic holds a prefix of the events rather than whatever happened to leave the queue. The flush is in
        # short slices, so a second Ctrl-C ends it; and if some of the prefix wasn't delivered, the error says so.
        left = _flush(producer, flush_timeout_s)
        if failures or left:
            exc.add_note(f"flowcast: of the {sent} records sent before this, {delivered} were acknowledged")
        raise
    left = _flush(producer, flush_timeout_s)
    seconds = time.perf_counter() - started
    if failures:
        raise KafkaError(f"{len(failures)} of {sent} records failed; first: {failures[0]}")
    if left or delivered != sent:
        raise KafkaError(f"only {delivered} of {sent} records acknowledged within {flush_timeout_s:g}s")
    return Produced(sent, seconds, sent_bytes, dict(sorted(partitions.items())))


def _flush(producer: Producer, timeout_s: float) -> int:
    """Producer.flush in slices of 0.1 s: one long call into librdkafka holds off Python's signal handling, so Ctrl-C
    couldn't end it. Returns the records still not delivered."""
    deadline = time.monotonic() + timeout_s
    while True:
        left = producer.flush(min(0.1, max(0.0, deadline - time.monotonic())))
        if left == 0 or time.monotonic() >= deadline:
            return left


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
    _check_timeout(timeout_s)
    consumer = _reader(bootstrap)
    try:
        starts: dict[int, int] = {}
        ends: dict[int, int] = {}
        for p, (low, high) in _watermarks(consumer, topic, timeout_s).items():
            if high > low:
                starts[p], ends[p] = low, high
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


def topic_counts(bootstrap: str, topic: str, timeout_s: float = 30.0) -> dict[int, int]:
    """Records in each partition now: high-water mark minus low (exact for this project's non-transactional writes,
    which put no control records in the log)."""
    _check_timeout(timeout_s)
    consumer = _reader(bootstrap)
    try:
        return {p: high - low for p, (low, high) in _watermarks(consumer, topic, timeout_s).items()}
    finally:
        consumer.close()


def _reader(bootstrap: str) -> Consumer:
    return Consumer(
        {
            **_client_config(bootstrap),
            "group.id": f"flowcast-reader-{uuid.uuid4()}",  # required by the client; never joined or committed to
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "enable.partition.eof": False,
        }
    )


def _watermarks(consumer: Consumer, topic: str, timeout_s: float) -> dict[int, tuple[int, int]]:
    """(low, high) offsets of each partition, by partition number."""
    try:
        metadata = consumer.list_topics(topic=topic, timeout=timeout_s)
    except KafkaException as exc:
        raise KafkaError(f"no answer from Kafka: {exc.args[0].str()}") from exc
    info = metadata.topics.get(topic)
    if info is None or info.error is not None:
        raise KafkaError(f"no topic {topic}")
    marks: dict[int, tuple[int, int]] = {}
    deadline = time.monotonic() + timeout_s
    for partition in sorted(info.partitions):
        while partition not in marks:
            try:
                marks[partition] = consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=max(0.1, deadline - time.monotonic())
                )
            except KafkaException as exc:
                # A partition of a topic created a moment ago may not have taken up its leader yet: wait for it.
                if exc.args[0].code() not in _SETTLING or time.monotonic() >= deadline:
                    raise KafkaError(f"no offsets for {topic} partition {partition}: {exc.args[0].str()}") from exc
                time.sleep(_POLL_S)
    return marks
