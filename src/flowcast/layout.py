"""Where one run of the job writes: an output directory per stream, each with its own checkpoint.

Kept apart from spark_job so the readers of the outputs (views, check) don't need Spark.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    """Where one run of the job writes. Each output has its own checkpoint (one streaming query per output)."""

    root: Path

    @property
    def dead_letters(self) -> Path:
        return self.root / "dead_letters"

    @property
    def raw_events(self) -> Path:
        return self.root / "raw_events"

    @property
    def minute_aggregates(self) -> Path:
        return self.root / "minute_aggregates"

    def checkpoint(self, output: str) -> Path:
        return self.root / "_checkpoints" / output
