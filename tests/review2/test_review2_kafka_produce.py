"""Second review (943596b..a403362): produce's new flush on an error part way. No broker needed: 127.0.0.1:1 refuses
connections, so nothing can be delivered and a flush waits its whole timeout."""

import signal
import threading
import time
from collections.abc import Iterator
from types import FrameType

import pytest

from flowcast.generator import sessions
from flowcast.kafka_io import produce
from flowcast.schema import Event

DEAD = "127.0.0.1:1"


def interrupted_stream(events: list[Event]) -> Iterator[Event]:
    yield from events
    raise KeyboardInterrupt  # Ctrl-C part way through `flowcast produce`


def test_a_second_ctrl_c_ends_the_flush_after_an_interrupt() -> None:
    # Ctrl-C now makes produce flush for up to flush_timeout_s (the CLI's default: 60 s) before the interrupt reaches
    # the caller, and the flush can't be interrupted: a second Ctrl-C is only handled once flush returns, so a user
    # aborting a produce to a broker that went away waits the whole minute whatever they press. At 943596b the first
    # Ctrl-C ended it at once. Expected: an interrupt during that flush ends it (or the flush after an interrupt is
    # short).
    events = [record.event for record in sessions(2)]
    previous = signal.getsignal(signal.SIGINT)

    def on_sigint(_signum: int, _frame: FrameType | None) -> None:
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, on_sigint)
    second = threading.Timer(1.0, signal.raise_signal, (signal.SIGINT,))  # the second Ctrl-C, during the flush
    try:
        started = time.monotonic()
        second.start()
        with pytest.raises(KeyboardInterrupt):
            produce(DEAD, "review2-never-created", interrupted_stream(events), flush_timeout_s=8)
        elapsed = time.monotonic() - started
    finally:
        second.cancel()
        signal.signal(signal.SIGINT, previous)
    assert elapsed < 4, f"produce took {elapsed:.1f}s to give up after two interrupts"
