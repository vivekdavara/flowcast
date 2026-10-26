"""How the job cuts a backlog into batches, and what that does to late data (DESIGN.md "Batch size and late data").

With `maxOffsetsPerTrigger`, Spark's Kafka source splits each batch's records across partitions in proportion to what
each has left to read (`KafkaMicroBatchStream.rateLimit`): it evens out counts, not event times. The watermark is
global, the latest event time over every partition minus the delay. When a partition's records cover more event time
per record than another's (fewer sessions keyed to it, at that moment), the first runs ahead in event time within the
same batch, and the watermark it sets can pass records the slower partition hasn't delivered yet: they are dropped as
late although they arrived on time. Small batches over a sparse stream make it worse: few records per partition per
batch, each spanning a lot of event time.

`plan` reproduces Spark's split for an available-now run (the end offsets fixed when the run starts), and `simulate`
replays the records through those batches with the job's own rules (check.replay), so a batch size's late drops can be
predicted without running Spark. `skew` measures the cause: per batch, how far apart the partitions' latest event
times are.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from flowcast.check import Batch, Replay, replay
from flowcast.kafka_io import Record
from flowcast.schema import InvalidEvent, decode
from flowcast.truth import WATERMARK_DELAY_MS


def rate_limit(limit: int, start: Mapping[int, int], end: Mapping[int, int]) -> dict[int, int]:
    """The next batch's end offsets: Spark's rateLimit. Each partition with records left gets `limit` times its share
    of all that's left, rounded down (up when below 1, so a small partition isn't starved), never past its end."""
    sizes = {p: end[p] - start[p] for p in end if end[p] - start[p] > 0}
    total = float(sum(sizes.values()))
    if total < 1:
        return dict(end)
    out = {}
    for p, last in end.items():
        if p not in sizes:
            out[p] = last
            continue
        prorate = limit * (sizes[p] / total)
        out[p] = min(last, start[p] + (math.ceil(prorate) if prorate < 1 else math.floor(prorate)))
    return out


def plan(limit: int, start: Mapping[int, int], end: Mapping[int, int]) -> list[dict[int, int]]:
    """Every data batch's end offsets for an available-now run from `start` to `end` at `limit` records a batch."""
    if limit < 1:
        raise ValueError("limit must be at least 1")
    batches = []
    position = dict(start)
    while any(position[p] < end[p] for p in end):
        position = rate_limit(limit, position, end)
        batches.append(position)
    return batches


def _event_time(record: Record) -> int | None:
    try:
        return decode(record.value).event_time
    except InvalidEvent:
        return None


def batches_with_watermarks(
    records: Sequence[Record], ends: Sequence[Mapping[int, int]], topic: str, delay_ms: int = WATERMARK_DELAY_MS
) -> list[Batch]:
    """`ends` as committed batches: each with the watermark the records of the batches before it give (the latest
    event time minus the delay, 0 before any), as Spark records it in the offset log."""
    by_partition: dict[int, list[tuple[int, int | None]]] = {}
    for record in sorted(records, key=lambda r: (r.partition, r.offset)):
        by_partition.setdefault(record.partition, []).append((record.offset, _event_time(record)))
    position = dict.fromkeys(by_partition, 0)
    latest: int | None = None
    out = []
    for batch_id, batch_end in enumerate(ends):
        watermark = 0 if latest is None else latest - delay_ms
        out.append(Batch(batch_id, watermark, ({topic: {str(p): o for p, o in batch_end.items()}},)))
        for p, rows in by_partition.items():
            i = position[p]
            while i < len(rows) and rows[i][0] < batch_end.get(p, 0):
                t = rows[i][1]
                if t is not None:
                    latest = t if latest is None else max(latest, t)
                i += 1
            position[p] = i
    return out


def offsets_of(records: Sequence[Record]) -> tuple[dict[int, int], dict[int, int]]:
    """Each partition's first offset and the offset after its last, over `records`."""
    start: dict[int, int] = {}
    end: dict[int, int] = {}
    for r in records:
        start[r.partition] = min(start.get(r.partition, r.offset), r.offset)
        end[r.partition] = max(end.get(r.partition, 0), r.offset + 1)
    return start, end


def simulate(records: Sequence[Record], limit: int, topic: str, delay_ms: int = WATERMARK_DELAY_MS) -> Replay:
    """The replay of an available-now run over all of `records` at `limit` records a batch."""
    start, end = offsets_of(records)
    batches = batches_with_watermarks(records, plan(limit, start, end), topic, delay_ms)
    return replay(records, batches, topic, delay_ms)


@dataclass(frozen=True)
class Skew:
    """How far apart the partitions are in event time, at the end of each batch."""

    batches: int
    lead_median_s: float  # the leading partition's latest event time minus the slowest one's, median over batches
    lead_max_s: float
    step_median_s: float  # how far the latest event time (and so the watermark) moves in one batch, median


def skew(records: Sequence[Record], batches: Sequence[Batch], topic: str) -> Skew:
    """At the end of each batch, every partition's latest event time so far: the lead is the spread between them. The
    split keeps partitions level in records, so the lead is about the same at any batch size; what a batch size changes
    is the step. A batch drops by the watermark of the batch before it, which trails the records it reads by about a
    step or two plus the delay: on-time records are dropped once the lead outgrows that."""
    by_partition: dict[int, list[tuple[int, int | None]]] = {}
    for record in sorted(records, key=lambda r: (r.partition, r.offset)):
        by_partition.setdefault(record.partition, []).append((record.offset, _event_time(record)))
    position = dict.fromkeys(by_partition, 0)
    latest: dict[int, int] = {}
    leads: list[float] = []
    steps: list[float] = []
    previous: int | None = None
    for batch in batches:
        ends = batch.sources[0][topic] if batch.sources and batch.sources[0] else {}
        for p, rows in by_partition.items():
            i = position[p]
            while i < len(rows) and rows[i][0] < int(ends.get(str(p), 0)):
                t = rows[i][1]
                if t is not None:
                    latest[p] = max(latest.get(p, t), t)
                i += 1
            position[p] = i
        if not latest:
            continue
        top = max(latest.values())
        if len(latest) > 1:
            leads.append((top - min(latest.values())) / 1000)
        if previous is not None and top > previous:
            steps.append((top - previous) / 1000)
        previous = top

    def median(values: list[float]) -> float:
        return sorted(values)[(len(values) - 1) // 2] if values else 0.0

    return Skew(len(leads), median(leads), max(leads, default=0.0), median(steps))
