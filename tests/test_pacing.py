import pytest

from flowcast.generator import Emitted, sessions
from flowcast.pacing import Pacer
from tests.test_schema import make


class FakeClock:
    """A clock that moves only when slept on, or when the test moves it (to play a slow consumer)."""

    def __init__(self) -> None:
        self.now = 100.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


def at(*arrivals_ms: int) -> list[Emitted]:
    return [Emitted(t, make(event_time=1_767_225_600_000 + i), "on_time", False) for i, t in enumerate(arrivals_ms)]


def test_records_go_out_at_their_arrival_times_scaled_by_speed() -> None:
    clock = FakeClock()
    sent_at = [clock.now for _ in Pacer(at(5_000, 5_000, 6_000, 9_000), 2.0, clock, clock.sleep)]
    # Relative to the first record: 0, 0, 1 s and 4 s of arrival time, at twice real time.
    assert sent_at == pytest.approx([100.0, 100.0, 100.5, 102.0])


def test_a_slow_consumer_gets_records_at_once_and_the_lag_is_reported() -> None:
    clock = FakeClock()
    pacer = Pacer(at(0, 1_000, 2_000, 3_000), 1.0, clock, clock.sleep)
    for i, _ in enumerate(pacer):
        if i == 1:
            clock.now += 2.5  # sending the second record took 2.5 s: the third is 1.5 s late, the fourth 0.5 s
    assert pacer.max_lag_s == pytest.approx(1.5)
    assert clock.sleeps == pytest.approx([1.0])


def test_tiny_waits_are_not_slept() -> None:
    clock = FakeClock()
    pacer = Pacer(at(0, 1, 2), 4.0, clock, clock.sleep)  # due 0.25 ms and 0.5 ms in: under MIN_SLEEP_S
    assert len(list(pacer)) == 3
    assert clock.sleeps == []
    assert pacer.max_lag_s == 0.0


def test_the_events_are_the_records_events_in_order() -> None:
    records = sessions(20)
    clock = FakeClock()
    assert list(Pacer(records, 1000.0, clock, clock.sleep)) == [r.event for r in records]


def test_nothing_in_is_nothing_out() -> None:
    assert list(Pacer([], 1.0)) == []


@pytest.mark.parametrize("speed", [0.0, -1.0, float("nan"), float("inf")])
def test_speed_must_be_positive_and_finite(speed: float) -> None:
    with pytest.raises(ValueError, match="speed"):
        Pacer([], speed)


def test_an_anchor_sends_records_at_their_own_times_on_the_wall_clock() -> None:
    clock = FakeClock()  # monotonic 100.0 is wall-clock epoch 1,000.0 s
    wall = lambda: clock.now + 900.0  # noqa: E731
    # The stream starts at epoch 1,000,000 ms; its first record arrives 300 ms after that, the second 1 s after.
    pacer = Pacer(at(1_000_300, 1_001_000), 1.0, clock, clock.sleep, anchor_ms=1_000_000, wall=wall)
    sent_at = [wall() for _ in pacer]
    assert sent_at == pytest.approx([1_000.3, 1_001.0])  # each at its arrival time, the first one included


def test_a_late_start_against_the_anchor_is_lag_not_a_shift() -> None:
    clock = FakeClock()
    wall = lambda: clock.now + 900.0 + 3.0  # noqa: E731  # the producer starts 3 s after the anchor
    pacer = Pacer(at(1_000_300, 1_001_000, 1_005_000), 1.0, clock, clock.sleep, anchor_ms=1_000_000, wall=wall)
    sent_at = [wall() for _ in pacer]
    # The first two are overdue and go at once; the third still goes at its own time, not 3 s late.
    assert sent_at == pytest.approx([1_003.0, 1_003.0, 1_005.0])
    assert pacer.max_lag_s == pytest.approx(2.7)
