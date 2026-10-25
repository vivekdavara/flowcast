"""The idempotent DuckDB sink without Spark: a batch id is written once, in one transaction, for one query only."""

import json
from pathlib import Path

import duckdb
import pytest

from flowcast.duckdb_sink import DuckDBSink, SinkError, query_id, read
from flowcast.truth import Row

T0 = 1_767_225_600_000
QUERY = "b6df5fb3-e11e-4f3b-b554-1b1ba4de55cb"


def make_sink(tmp_path: Path, query: str = QUERY) -> DuckDBSink:
    checkpoint = tmp_path / "checkpoint"
    checkpoint.mkdir(exist_ok=True)
    (checkpoint / "metadata").write_text(json.dumps({"id": query}))
    return DuckDBSink(tmp_path / "aggregates.duckdb", checkpoint)


def rows(*starts: int, events: int = 1) -> list[Row]:
    return [Row(T0 + 60_000 * n, "page_view", events, 0) for n in starts]


def test_the_query_id_is_the_checkpoints(tmp_path: Path) -> None:
    sink = make_sink(tmp_path)
    assert query_id(sink.checkpoint) == QUERY


def test_a_batch_runs_once_however_often_spark_hands_it_over(tmp_path: Path) -> None:
    sink = make_sink(tmp_path)
    assert read(sink.database) == ([], -1)
    assert sink.write(0, rows(0, 1)) is True
    # Spark running batch 0 again after a crash: skipped, even if (wrongly) its rows had changed.
    assert sink.write(0, rows(0, 1)) is False
    assert sink.write(0, rows(5, events=9)) is False
    assert sink.write(1, []) is True  # a batch that wrote nothing is still a batch
    assert sink.write(2, rows(2)) is True
    assert sink.write(1, rows(7)) is False
    assert read(sink.database) == (rows(0, 1, 2), 2)


def test_a_gap_in_the_batch_ids_is_refused(tmp_path: Path) -> None:
    sink = make_sink(tmp_path)
    with pytest.raises(SinkError, match="batch 1 after batch -1"):
        sink.write(1, rows(0))
    sink.write(0, rows(0))
    with pytest.raises(SinkError, match="missing batches"):
        sink.write(2, rows(1))
    assert read(sink.database) == (rows(0), 0)


def test_another_querys_batches_are_refused(tmp_path: Path) -> None:
    # A new checkpoint starts again at batch 0: without the query id, its batches would be skipped as written.
    make_sink(tmp_path).write(0, rows(0))
    other = make_sink(tmp_path, query="00000000-0000-4000-8000-000000000000")
    with pytest.raises(SinkError, match=f"holds batches of query {QUERY}"):
        other.write(0, rows(1))
    with pytest.raises(SinkError, match=f"holds batches of query {QUERY}"):
        other.write(1, rows(1))
    assert read(other.database) == (rows(0), 0)


@pytest.mark.parametrize("earlier", [False, True], ids=["within-the-batch", "in-an-earlier-batch"])
def test_a_window_written_twice_fails_the_batch_and_writes_none_of_it(tmp_path: Path, earlier: bool) -> None:
    # Append mode writes a window once; a second row for it would double its counts, so the key refuses it, and the
    # transaction takes the batch's other rows and its id with it.
    sink = make_sink(tmp_path)
    sink.write(0, rows(0))
    with pytest.raises(duckdb.ConstraintException):
        sink.write(1, rows(1, 0) if earlier else rows(1, 2, 1))
    assert read(sink.database) == (rows(0), 0)
    assert sink.write(1, rows(1, 2)) is True
    assert read(sink.database) == (rows(0, 1, 2), 1)


def test_reading_doesnt_create_a_database(tmp_path: Path) -> None:
    assert read(tmp_path / "missing.duckdb") == ([], -1)
    assert not (tmp_path / "missing.duckdb").exists()
