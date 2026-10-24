"""The producer's settings, checked without a broker (the day-1 review's mutant turned idempotence off unnoticed)."""

from collections.abc import Iterator
from typing import Any, ClassVar

import pytest

import flowcast.kafka_io as kafka_io
from flowcast.generator import sessions
from flowcast.schema import Event, encode


class FakeMessage:
    def __init__(self, partition: int) -> None:
        self._partition = partition

    def partition(self) -> int:
        return self._partition


class FakeProducer:
    configs: ClassVar[list[dict[str, Any]]] = []

    def __init__(self, config: dict[str, Any]) -> None:
        self.configs.append(config)
        self.callbacks: list[Any] = []

    def produce(self, topic: str, value: bytes, key: bytes, on_delivery: Any) -> None:
        self.callbacks.append(on_delivery)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        for i, callback in enumerate(self.callbacks):
            callback(None, FakeMessage(i % 2))
        self.callbacks.clear()
        return 0


def test_the_producer_is_idempotent_and_waits_for_all_replicas(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(kafka_io, "Producer", FakeProducer)
    events = [record.event for record in sessions(3)]
    assert kafka_io.produce("127.0.0.1:1", "t", events).records == len(events)
    config = FakeProducer.configs[-1]
    assert config["enable.idempotence"] is True
    # Idempotence implies acks=all; nothing may weaken it.
    assert config.get("acks", "all") in ("all", -1)
    assert "max.in.flight.requests.per.connection" not in config


@pytest.mark.parametrize("timeout", [float("nan"), float("inf"), -1.0])
def test_every_kafka_call_refuses_a_deadline_that_can_never_pass(timeout: float) -> None:
    # Checked before any connection, so no broker is needed (the second review found only two calls checked).
    calls = [
        lambda: kafka_io.wait_for_broker("127.0.0.1:1", timeout_s=timeout),
        lambda: kafka_io.ensure_topic("127.0.0.1:1", "t", timeout_s=timeout),
        lambda: kafka_io.delete_topic("127.0.0.1:1", "t", timeout_s=timeout),
        lambda: kafka_io.produce("127.0.0.1:1", "t", [], flush_timeout_s=timeout),
        lambda: kafka_io.read_topic("127.0.0.1:1", "t", timeout_s=timeout),
        lambda: kafka_io.topic_counts("127.0.0.1:1", "t", timeout_s=timeout),
    ]
    for call in calls:
        with pytest.raises(ValueError, match="finite"):
            call()


def test_an_error_part_way_says_when_records_sent_before_it_were_not_delivered() -> None:
    # Nothing listens on 127.0.0.1:1, so the records sent before the error can't be delivered: the original error is
    # raised, with a note saying how many of them were acknowledged.
    events = [record.event for record in sessions(2)]

    def failing() -> Iterator[Event]:
        yield from events
        raise RuntimeError("the source broke")

    with pytest.raises(RuntimeError, match="the source broke") as raised:
        kafka_io.produce("127.0.0.1:1", "t", failing(), flush_timeout_s=0.5)
    assert raised.value.__notes__ == [f"flowcast: of the {len(events)} records sent before this, 0 were acknowledged"]


def test_batching_settings_reach_the_producer_and_deliveries_are_counted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(kafka_io, "Producer", FakeProducer)
    events = [record.event for record in sessions(3)]
    settings = kafka_io.ProducerSettings(linger_ms=0, batch_bytes=65_536, compression="zstd")
    produced = kafka_io.produce("127.0.0.1:1", "t", events, settings=settings)
    config = FakeProducer.configs[-1]
    assert (config["linger.ms"], config["batch.size"], config["compression.type"]) == (0, 65_536, "zstd")
    assert config["enable.idempotence"] is True  # settings can't turn it off
    assert produced.partitions == {0: (len(events) + 1) // 2, 1: len(events) // 2}
    assert produced.bytes == sum(len(encode(e)) + len(e.user_id) for e in events)


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"linger_ms": -1}, "linger_ms"),
        ({"linger_ms": float("nan")}, "linger_ms"),
        ({"linger_ms": 900_001}, "linger_ms"),
        ({"batch_bytes": 0}, "batch_bytes"),
        ({"batch_bytes": 2**31}, "batch_bytes"),
        ({"compression": "brotli"}, "compression"),
    ],
)
def test_producer_settings_refuse_what_librdkafka_would(changes: dict[str, Any], reason: str) -> None:
    with pytest.raises(ValueError, match=reason):
        kafka_io.ProducerSettings(**changes)
