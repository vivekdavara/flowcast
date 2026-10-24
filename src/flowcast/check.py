"""The exact check of the streaming aggregate against the truth, by replaying the job's own batches.

DESIGN.md "Ground truth". Which late records the stream drops depends on where its micro-batch boundaries fell, so
the truth of all events only bounds Spark's output. A query's checkpoint records, for every batch, the Kafka offsets it
read up to and the watermark it ran with. Replaying that over the records in Kafka gives exactly the records the job
had to keep: a record read in batch i is dropped when its event time is at or before batch i-1's watermark (the
late-events watermark of every stateful operator, Spark 4's `PropagateWatermarkSimulator`; confirmed by
tests/test_spark_rules.py). The truth of the kept records must then equal the job's output, row for row, in every
window the job has written: those whose end is at or before the last batch's watermark.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from flowcast.kafka_io import Record
from flowcast.schema import Event, InvalidEvent, decode
from flowcast.truth import WINDOW_MS, Row, minute_aggregates


@dataclass(frozen=True)
class Batch:
    """One committed micro-batch of a query, from its checkpoint's offset log."""

    batch_id: int
    watermark_ms: int
    # Each source's end offset as Spark wrote it (for Kafka: {topic: {partition: next offset}}); None for no source.
    sources: tuple[Any, ...]


class CheckpointError(Exception):
    pass


def committed_batches(checkpoint: Path) -> list[Batch]:
    """Every batch the query committed, in order. Spark writes `offsets/<id>` when it plans a batch and
    `commits/<id>` when the batch's output is done; a planned batch without a commit (a crash) isn't included."""
    offsets, commits = checkpoint / "offsets", checkpoint / "commits"
    committed = {int(p.name) for p in commits.iterdir() if p.name.isdigit()} if commits.is_dir() else set()
    batches = []
    for batch_id in sorted(committed):
        lines = (offsets / str(batch_id)).read_text().splitlines()
        if len(lines) < 2 or lines[0] != "v1":
            raise CheckpointError(f"{offsets / str(batch_id)}: not a version-1 offset log entry")
        metadata = json.loads(lines[1])
        sources = tuple(None if line == "-" else json.loads(line) for line in lines[2:])
        batches.append(Batch(batch_id, int(metadata["batchWatermarkMs"]), sources))
    ids = [b.batch_id for b in batches]
    if ids != list(range(len(ids))):
        # Spark deletes the oldest entries beyond spark.sql.streaming.minBatchesToRetain; the replay needs them all.
        raise CheckpointError(f"{checkpoint}: batches {ids[:3]}... aren't a complete run from batch 0")
    return batches


def kafka_end_offsets(batch: Batch, topic: str) -> dict[int, int]:
    """A batch's end offsets for `topic`, by partition (its one source must be that Kafka topic)."""
    if len(batch.sources) != 1 or not isinstance(batch.sources[0], dict) or topic not in batch.sources[0]:
        raise CheckpointError(f"batch {batch.batch_id} doesn't read the Kafka topic {topic}")
    return {int(p): int(o) for p, o in batch.sources[0][topic].items()}


@dataclass(frozen=True)
class Replay:
    kept: list[Event]  # valid records the stream had to keep, in batch order then partition and offset
    dropped: list[Event]  # valid records at or before their batch's late-events watermark
    invalid: int  # records `decode` refuses (the job's dead letters)
    unread: int  # records past the last committed batch's end offsets
    watermark_ms: int  # the last committed batch's watermark: windows ending at or before it are written


def replay(records: Iterable[Record], batches: Sequence[Batch], topic: str) -> Replay:
    """Assign each record to the batch that read it, and keep it or drop it as the stream did."""
    by_partition: dict[int, list[Record]] = {}
    for record in sorted(records, key=lambda r: (r.partition, r.offset)):
        by_partition.setdefault(record.partition, []).append(record)
    position = {p: 0 for p in by_partition}  # index of the next unassigned record in each partition
    kept: list[Event] = []
    dropped: list[Event] = []
    invalid = 0
    late_watermark = 0  # batch 0 runs with no earlier watermark
    for batch in batches:
        ends = kafka_end_offsets(batch, topic)
        for partition, partition_records in sorted(by_partition.items()):
            end = ends.get(partition, 0)
            i = position[partition]
            while i < len(partition_records) and partition_records[i].offset < end:
                try:
                    event = decode(partition_records[i].value)
                except InvalidEvent:
                    invalid += 1
                else:
                    (dropped if event.event_time <= late_watermark else kept).append(event)
                i += 1
            position[partition] = i
        late_watermark = batch.watermark_ms
    unread = sum(len(rs) - position[p] for p, rs in by_partition.items())
    return Replay(kept, dropped, invalid, unread, batches[-1].watermark_ms if batches else 0)


def expected_rows(result: Replay) -> list[Row]:
    """The rows the stream must have written: the truth of the kept records, in windows the watermark has passed."""
    return [row for row in minute_aggregates(result.kept) if row.window_start + WINDOW_MS <= result.watermark_ms]


@dataclass(frozen=True)
class Report:
    """The check of one stopped run of the job (`check_run`)."""

    batches: int
    records: int  # in the topic when the check read it
    replay: Replay
    expected: list[Row]
    written: list[Row]
    raw_events: tuple[int, int]  # (written, valid records the raw query's batches read)
    dead_letters: tuple[int, int]  # (written, invalid records the dead-letter query's batches read)

    @property
    def ok(self) -> bool:
        return (
            self.written == self.expected
            and self.raw_events[0] == self.raw_events[1]
            and self.dead_letters[0] == self.dead_letters[1]
        )

    def lines(self) -> list[str]:
        r = self.replay
        missing = sorted(set(self.expected) - set(self.written))
        extra = sorted(set(self.written) - set(self.expected))
        return [
            f"minute_aggregates: {self.batches} batches, watermark {r.watermark_ms}; {self.records} records in the "
            f"topic: {len(r.kept)} kept, {len(r.dropped)} dropped as late, {r.invalid} invalid, {r.unread} not read",
            f"  {len(self.written)} rows written, {len(self.expected)} expected: "
            + ("equal" if self.written == self.expected else f"{len(missing)} missing, {len(extra)} unexpected"),
            *(f"  missing    {row}" for row in missing[:10]),
            *(f"  unexpected {row}" for row in extra[:10]),
            f"raw_events: {self.raw_events[0]} written, {self.raw_events[1]} valid records read",
            f"dead_letters: {self.dead_letters[0]} written, {self.dead_letters[1]} invalid records read",
            "ok" if self.ok else "MISMATCH",
        ]


def check_run(bootstrap: str, topic: str, root: Path, timeout_s: float = 60.0) -> Report:
    """Check a stopped run of the job under `root` against the records in `topic`. Each output is checked against
    its own query's checkpoint, since the three queries read Kafka independently."""
    from flowcast.kafka_io import read_topic
    from flowcast.layout import Paths
    from flowcast.views import connect, latest_batch

    paths = Paths(root)
    per_query: dict[str, list[Batch]] = {}
    for name, sink in (
        ("minute_aggregates", paths.minute_aggregates),
        ("raw_events", paths.raw_events),
        ("dead_letters", paths.dead_letters),
    ):
        batches = committed_batches(paths.checkpoint(name))
        if not batches:
            raise CheckpointError(f"{paths.checkpoint(name)}: no committed batches")
        # A sink logs a batch just before the checkpoint commits it: if they differ, the job is mid-batch (or died
        # there), and its output and checkpoint don't describe the same batches yet.
        if latest_batch(sink) != batches[-1].batch_id:
            raise CheckpointError(f"{name}: the sink and its checkpoint disagree on the last batch; stop the job first")
        per_query[name] = batches
    records = read_topic(bootstrap, topic, timeout_s)
    result = replay(records, per_query["minute_aggregates"], topic)
    raw_read = replay(records, per_query["raw_events"], topic)
    dead_read = replay(records, per_query["dead_letters"], topic)
    con = connect(root)
    try:
        rows = con.execute("SELECT window_start, event_type, events, revenue_cents FROM minute_aggregates").fetchall()
        raw_count = con.execute("SELECT count(*) FROM raw_events").fetchone()
        dead_count = con.execute("SELECT count(*) FROM dead_letters").fetchone()
    finally:
        con.close()
    assert raw_count is not None and dead_count is not None
    return Report(
        batches=len(per_query["minute_aggregates"]),
        records=len(records),
        replay=result,
        expected=expected_rows(result),
        written=sorted(Row(int(a), str(b), int(c), int(d)) for a, b, c, d in rows),
        raw_events=(int(raw_count[0]), len(raw_read.kept) + len(raw_read.dropped)),
        dead_letters=(int(dead_count[0]), dead_read.invalid),
    )
