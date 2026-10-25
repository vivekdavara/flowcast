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

    @property
    def backfill(self) -> Path:
        """The batch backfill's results: a directory per run and the `CURRENT` pointer to the published one."""
        return self.root / "backfill"

    @property
    def duckdb(self) -> Path:
        """The idempotent DuckDB sink's database (written only when the job runs with it)."""
        return self.root / "aggregates.duckdb"

    def progress(self, output: str) -> Path:
        """Spark's progress reports for one output's query, a JSON line per batch, saved when a run of the job ends."""
        return self.root / "_progress" / f"{output}.jsonl"

    def checkpoint(self, output: str) -> Path:
        return self.root / "_checkpoints" / output


class LayoutError(Exception):
    pass


def current_backfill(paths: Paths) -> Path | None:
    """The published backfill's directory (named by `backfill/CURRENT`), or None if there is none."""
    pointer = paths.backfill / "CURRENT"
    if not pointer.exists():
        return None
    name = pointer.read_text().strip()
    if not name or "/" in name or name.startswith("."):
        raise LayoutError(f"{pointer}: not a backfill directory name: {name!r}")
    return paths.backfill / name
