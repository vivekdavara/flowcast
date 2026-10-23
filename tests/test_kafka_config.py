"""The producer's settings, checked without a broker (the day-1 review's mutant turned idempotence off unnoticed)."""

from typing import Any, ClassVar

import pytest

import flowcast.kafka_io as kafka_io
from flowcast.generator import sessions


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
        for callback in self.callbacks:
            callback(None, None)
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


def test_wait_for_broker_refuses_a_deadline_that_can_never_pass() -> None:
    for timeout in (float("nan"), float("inf"), -1.0):
        with pytest.raises(ValueError, match="finite"):
            kafka_io.wait_for_broker("127.0.0.1:1", timeout_s=timeout)
