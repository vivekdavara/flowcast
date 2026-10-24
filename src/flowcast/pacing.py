"""Sending generated records at the pace they arrived: the generator's arrival times replayed against the wall clock.

The generator makes a stream far faster than real time (DESIGN.md "The generator"). A producer that sends as fast as
it can is a backlog test; one paced by arrival time is a live stream, which is what end-to-end latency (day 4) means.
`speed` scales the clock: 1 is real time, 10 is ten times faster.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Iterable, Iterator

from flowcast.generator import Emitted
from flowcast.schema import Event

# Sleeping for less than this costs more than it's worth; a record due sooner is sent at once.
MIN_SLEEP_S = 0.001


class Pacer:
    """Yields each record's event no earlier than its arrival time (relative to the first record's), divided by
    `speed`. If the consumer falls behind, records go out at once and `max_lag_s` says by how much it fell behind."""

    def __init__(
        self,
        records: Iterable[Emitted],
        speed: float,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not math.isfinite(speed) or speed <= 0:
            raise ValueError("speed must be a positive, finite number")
        self._records = records
        self._speed = speed
        self._clock = clock
        self._sleep = sleep
        self.max_lag_s = 0.0
        self.slept_s = 0.0

    def __iter__(self) -> Iterator[Event]:
        start_wall = start_arrival = None
        for record in self._records:
            if start_wall is None or start_arrival is None:
                start_wall, start_arrival = self._clock(), record.arrival_ms
            due = start_wall + (record.arrival_ms - start_arrival) / 1000 / self._speed
            wait = due - self._clock()
            if wait >= MIN_SLEEP_S:
                self._sleep(wait)
                self.slept_s += wait
            elif wait < 0:
                self.max_lag_s = max(self.max_lag_s, -wait)
            yield record.event
