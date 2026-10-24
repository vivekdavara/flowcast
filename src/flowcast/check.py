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
from dataclasses import dataclass
from pathlib import Path
from typing import Any


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
