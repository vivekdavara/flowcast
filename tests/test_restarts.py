"""The restart log: each start's query states, and the batches a restart re-ran that the file sink skipped."""

from pathlib import Path

from flowcast.layout import Paths
from flowcast.restarts import QueryState, read_starts, record_start, restarts_file, skipped_reruns


def logs(paths: Paths, name: str, offsets: int, commits: int, sink: int) -> None:
    """A query whose offset log, commit log and sink log run to these batch ids (-1: none)."""
    for directory, last in (
        (paths.checkpoint(name) / "offsets", offsets),
        (paths.checkpoint(name) / "commits", commits),
        (getattr(paths, name) / "_spark_metadata", sink),
    ):
        directory.mkdir(parents=True, exist_ok=True)
        for batch in range(last + 1):
            (directory / str(batch)).write_text("v1\n")


def test_a_start_records_each_querys_state_and_reads_back(tmp_path: Path) -> None:
    paths = Paths(tmp_path)
    logs(paths, "minute_aggregates", offsets=3, commits=2, sink=3)
    first = record_start(paths, ["minute_aggregates", "raw_events"])
    assert first == {"minute_aggregates": QueryState(3, 3, 2), "raw_events": QueryState(-1, -1, -1)}
    logs(paths, "minute_aggregates", offsets=5, commits=5, sink=5)
    record_start(paths, ["minute_aggregates"])
    assert read_starts(paths) == [first, {"minute_aggregates": QueryState(5, 5, 5)}]


def test_a_torn_last_line_is_skipped_and_the_next_start_stands_alone(tmp_path: Path) -> None:
    # A start killed while writing its line started no query, so it has nothing to say.
    paths = Paths(tmp_path)
    record_start(paths, ["raw_events"])
    with restarts_file(paths).open("a") as log:
        log.write('{"started_at": 1, "queries": {"raw_ev')
    record_start(paths, ["raw_events"])
    assert read_starts(paths) == [{"raw_events": QueryState(-1, -1, -1)}] * 2
    assert read_starts(Paths(tmp_path / "none")) == []


def test_only_a_file_sinks_logged_but_uncommitted_batch_is_a_skipped_rerun() -> None:
    starts = [
        {"minute_aggregates": QueryState(-1, -1, -1), "duckdb_aggregates": QueryState(-1, -1, -1)},
        {"minute_aggregates": QueryState(4, 4, 3), "duckdb_aggregates": QueryState(4, 4, 3)},  # sinks ahead
        {"minute_aggregates": QueryState(7, 6, 6), "duckdb_aggregates": QueryState(7, 6, 6)},  # mid-batch
        {"minute_aggregates": QueryState(9, 9, 9)},
    ]
    assert skipped_reruns(starts, "minute_aggregates") == {4}
    # The DuckDB sink is called for a re-run batch (foreachBatch executes it), so nothing of it is skipped.
    assert skipped_reruns(starts, "duckdb_aggregates") == set()
    assert skipped_reruns(starts, "raw_events") == set()
