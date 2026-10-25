"""Exactly once across a crash. A batch is planned (`offsets/<id>`), its output written and logged by the sink, and only
then committed (`commits/<id>`); a crash can cut it off anywhere in between. On restart Spark runs the uncommitted batch
again with the offsets and watermark it planned, and each sink must make the second run harmless. These tests leave a
finished run in the two states a crash can leave the last batch in, restart it, and require every output to reconcile
exactly (check.py) with no record counted twice."""

import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import duckdb
import pytest

from flowcast.check import CheckpointError, check_run, committed_batches
from flowcast.generator import Config, sessions
from flowcast.kafka_io import delete_topic, ensure_topic, produce, wait_for_broker
from flowcast.layout import Paths
from flowcast.views import committed_files, connect, orphan_files
from tests.test_spark_stream import BAD, produce_raw, run

BOOTSTRAP = os.environ.get("FLOWCAST_TEST_BOOTSTRAP", "")

pytestmark = [
    pytest.mark.kafka,
    pytest.mark.spark,
    pytest.mark.skipif(not BOOTSTRAP, reason="FLOWCAST_TEST_BOOTSTRAP not set (scripts/kafka.sh start)"),
]

CONFIG = Config(seed=23, sessions_per_second=0.2, late_fraction=0.2, very_late_fraction=0.05, duplicate_fraction=0.05)
QUERIES = ("dead_letters", "raw_events", "minute_aggregates", "duckdb_aggregates")


@pytest.fixture
def topic() -> Iterator[str]:
    wait_for_broker(BOOTSTRAP, timeout_s=60)
    name = f"test-{uuid.uuid4()}"
    ensure_topic(BOOTSTRAP, name, partitions=6)
    yield name
    delete_topic(BOOTSTRAP, name)


def sink_of(paths: Paths, name: str) -> Path:
    path = getattr(paths, name)
    assert isinstance(path, Path)
    return path


def log_entry(sink: Path, batch_id: int) -> Path:
    """The sink log's file for a batch: `<id>`, or `<id>.compact` every tenth batch."""
    log = sink / "_spark_metadata"
    return next(p for p in (log / str(batch_id), log / f"{batch_id}.compact") if p.exists())


def unwrite(path: Path) -> None:
    """Remove a log file as if it had never been written: with Hadoop's checksum file beside it (`.<name>.crc`), which
    is renamed into place with it; a stale one makes Spark's next write of that name fail."""
    path.unlink()
    path.with_name(f".{path.name}.crc").unlink()


def cut_off_last_batch(paths: Paths, name: str, *, logged: bool) -> tuple[int, str]:
    """Put a query's last batch back in the state a crash leaves it in: planned and written but not committed, and,
    unless `logged`, not in the sink's log either (its data files stay, as orphans). Returns the batch id and its
    offset-log entry."""
    checkpoint = paths.checkpoint(name)
    last = committed_batches(checkpoint)[-1].batch_id
    unwrite(checkpoint / "commits" / str(last))
    if not logged and name == "duckdb_aggregates":
        # A crash before the sink's transaction committed leaves neither the batch's rows nor its id.
        with duckdb.connect(str(paths.duckdb)) as con:
            con.execute("DELETE FROM minute_aggregates WHERE batch_id = ?", [last])
            con.execute("DELETE FROM sink_batches WHERE batch_id = ?", [last])
    elif not logged:
        unwrite(log_entry(sink_of(paths, name), last))
    return last, (checkpoint / "offsets" / str(last)).read_text()


@pytest.mark.parametrize("logged", [True, False], ids=["after-the-sink-logged", "before-the-sink-logged"])
def test_a_batch_cut_off_before_its_commit_runs_again_exactly_once(
    spark: Any, topic: str, tmp_path: Path, logged: bool
) -> None:
    events = [r.event for r in sessions(200, CONFIG)]
    half = len(events) // 2
    produce(BOOTSTRAP, topic, events[:half])
    produce_raw(topic, BAD)
    run(spark, topic, tmp_path, duckdb=True)
    paths = Paths(tmp_path)
    cut = {name: cut_off_last_batch(paths, name, logged=logged) for name in QUERIES}

    if logged:
        # The sink is a batch ahead of its checkpoint: the check won't read a run in that state.
        with pytest.raises(CheckpointError, match="disagree on the last batch"):
            check_run(BOOTSTRAP, topic, tmp_path)
    else:
        # Sink and checkpoint agree on the batch before: a consistent, shorter run, plus the files it never logged.
        early = check_run(BOOTSTRAP, topic, tmp_path)
        assert early.ok, early.lines()
        assert early.orphans["raw_events"] > 0 and early.orphans["dead_letters"] > 0

    produce(BOOTSTRAP, topic, events[half:])
    run(spark, topic, tmp_path, duckdb=True)  # the restart: the cut-off batch first, then the new records

    report = check_run(BOOTSTRAP, topic, tmp_path)
    assert report.ok, "\n".join(report.lines())
    assert report.replay.unread == 0 and report.replay.invalid == len(BAD)
    # The second start recorded where each query stood; a batch the file sink had logged was skipped on its re-run.
    assert report.restarts == 2
    assert report.skipped_reruns == ([cut["minute_aggregates"][0]] if logged else [])
    # The DuckDB sink: each batch once (its rows equal the replay of its own query, so nothing doubled).
    assert report.duckdb is not None and report.duckdb[0] == report.duckdb[1] and len(report.duckdb[0]) > 20
    for name, (batch_id, offsets) in cut.items():
        # The batch ran again as it was planned, not re-planned over what had arrived since.
        assert (paths.checkpoint(name) / "offsets" / str(batch_id)).read_text() == offsets
        assert batch_id in {b.batch_id for b in committed_batches(paths.checkpoint(name))}

    con = connect(tmp_path)
    try:
        counts = con.execute('SELECT count(*), count(DISTINCT (partition, "offset")) FROM raw_events').fetchone()
        assert counts == (len(events), len(events))
        letters = con.execute('SELECT count(*), count(DISTINCT "offset") FROM dead_letters').fetchone()
        assert letters == (len(BAD), len(BAD))
    finally:
        con.close()

    orphans = orphan_files(paths.raw_events)
    if logged:
        # The file sink saw the batch in its log and skipped it: nothing written twice.
        assert orphans == [] and report.orphans == {name: 0 for name in QUERIES[:3]}
    else:
        # The batch was written again under new names. Its first files are still there, and a glob of the directory
        # (what the views never do) counts their records twice.
        assert orphans and report.orphans["raw_events"] == len(orphans)
        with duckdb.connect() as con:
            globbed = con.execute(f"SELECT count(*) FROM read_parquet('{paths.raw_events}/**/*.parquet')").fetchone()
        assert globbed is not None and globbed[0] > len(events)
        assert set(orphans).isdisjoint(committed_files(paths.raw_events))
