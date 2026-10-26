"""batching.py: Spark's split of a batch across partitions, and the late drops it causes on a skewed backlog."""

import dataclasses

import pytest

from flowcast.batching import batches_with_watermarks, offsets_of, plan, rate_limit, simulate, skew
from flowcast.check import Batch, replay
from flowcast.generator import take
from flowcast.kafka_io import Record
from flowcast.schema import encode

T0 = 1_767_225_600_000
TOPIC = "t"


def test_the_split_is_proportional_to_what_is_left() -> None:
    # 600 left in partition 0, 300 in 1, 100 in 2: a batch of 100 takes 60, 30, 10.
    assert rate_limit(100, {0: 0, 1: 0, 2: 0}, {0: 600, 1: 300, 2: 100}) == {0: 60, 1: 30, 2: 10}
    # Shares round down: 100 * 1/3 = 33.3.
    assert rate_limit(100, {0: 0, 1: 0, 2: 0}, {0: 10, 1: 10, 2: 10}) == {0: 10, 1: 10, 2: 10}
    assert rate_limit(10, {0: 0, 1: 0, 2: 0}, {0: 100, 1: 100, 2: 100}) == {0: 3, 1: 3, 2: 3}
    # A share below 1 rounds up, so a small partition isn't starved; never past the end.
    assert rate_limit(10, {0: 0, 1: 0}, {0: 1000, 1: 5}) == {0: 9, 1: 1}
    # A partition with nothing left stays at its end; nothing left at all is the end.
    assert rate_limit(10, {0: 4, 1: 7}, {0: 4, 1: 20}) == {0: 4, 1: 17}
    assert rate_limit(10, {0: 4}, {0: 4}) == {0: 4}


def test_a_plan_reads_everything_in_steps() -> None:
    batches = plan(100, {0: 0, 1: 50}, {0: 180, 1: 110})
    assert batches[-1] == {0: 180, 1: 110}
    # 180 and 60 left: 75 and 25; then 105 and 35: 75 and 25 again; then the 30 and 10 left.
    assert batches == [{0: 75, 1: 75}, {0: 150, 1: 100}, {0: 180, 1: 110}]
    assert plan(100, {0: 5}, {0: 5}) == []
    with pytest.raises(ValueError, match="at least 1"):
        plan(0, {0: 0}, {0: 1})


def records(times_by_partition: dict[int, list[int]]) -> list[Record]:
    """Valid records whose event times are given, per partition, in offset order; every one distinct."""
    events = take(sum(len(ts) for ts in times_by_partition.values()))
    out = []
    n = 0
    for partition, times in times_by_partition.items():
        for offset, time in enumerate(times):
            event = dataclasses.replace(events[n].event, event_time=time)
            out.append(Record(partition, offset, None, encode(event), time))
            n += 1
    return out


# Partition 0 is sparse (100 s of event time a record), partition 1 dense (1 s): both in order, nothing late.
SKEWED = records({0: [T0 + 100_000 * i for i in range(40)], 1: [T0 + 1_000 * i for i in range(40)]})


def test_small_batches_drop_on_time_records_of_the_slow_partition() -> None:
    small = simulate(SKEWED, 4, TOPIC)  # two records from each partition a batch
    assert small.dropped and {e.event_time for e in small.dropped} <= {T0 + 1_000 * i for i in range(40)}
    assert len(small.kept) + len(small.dropped) == 80 and small.watermark_errors == []
    # One batch for everything has no earlier watermark to drop by.
    assert simulate(SKEWED, 80, TOPIC).dropped == []
    # Larger batches drop fewer: the watermark trails the records by more event time.
    counts = [len(simulate(SKEWED, size, TOPIC).dropped) for size in (4, 8, 16, 40)]
    assert counts == sorted(counts, reverse=True) and counts[0] > counts[-1]


def test_simulate_is_the_replay_of_its_planned_batches() -> None:
    start, end = offsets_of(SKEWED)
    assert (start, end) == ({0: 0, 1: 0}, {0: 40, 1: 40})
    batches = batches_with_watermarks(SKEWED, plan(4, start, end), TOPIC)
    assert batches[0].watermark_ms == 0
    # Batch 1's watermark: the latest event time of batch 0 (partition 0's second record) minus 3 minutes.
    assert batches[1].watermark_ms == T0 + 100_000 - 180_000
    assert replay(SKEWED, batches, TOPIC).dropped == simulate(SKEWED, 4, TOPIC).dropped
    assert isinstance(batches[0], Batch) and batches[0].sources == ({TOPIC: {"0": 2, "1": 2}},)


def test_invalid_records_dont_move_the_watermark() -> None:
    bad = [Record(0, 0, None, b"{", T0 + 10**9)]
    batches = batches_with_watermarks(
        bad + records({1: [T0]}), [{0: 1, 1: 0}, {0: 1, 1: 1}, {0: 1, 1: 1}], TOPIC, delay_ms=0
    )
    assert [b.watermark_ms for b in batches] == [0, 0, T0]


def test_the_lead_is_about_the_same_at_any_size_the_step_is_not() -> None:
    start, end = offsets_of(SKEWED)
    small = skew(SKEWED, batches_with_watermarks(SKEWED, plan(4, start, end), TOPIC), TOPIC)
    large = skew(SKEWED, batches_with_watermarks(SKEWED, plan(16, start, end), TOPIC), TOPIC)
    # After k records from each, partition 0 is at 100(k-1) s and partition 1 at k-1 s: the lead is 99(k-1) s.
    assert (small.batches, small.lead_max_s, small.step_median_s) == (20, 3_861, 200)
    assert (large.batches, large.lead_max_s, large.step_median_s) == (5, 3_861, 800)
    assert skew([], [], TOPIC).batches == 0
