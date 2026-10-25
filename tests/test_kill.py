"""Exactly once under real kills: the job SIGKILLed at random moments mid-batch and restarted, then checked exactly
(flowcast.recovery; scripts/kill_restart.py runs the same at a larger size)."""

import os
import random
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from flowcast.check import check_run
from flowcast.generator import Config, sessions
from flowcast.kafka_io import delete_topic, ensure_topic, produce, wait_for_broker
from flowcast.layout import Paths
from flowcast.recovery import DUCKDB_QUERY, QUERIES, QueryState, kill_and_restart, query_state
from flowcast.views import connect

BOOTSTRAP = os.environ.get("FLOWCAST_TEST_BOOTSTRAP", "")
CONFIG = Config(seed=29, sessions_per_second=0.2, late_fraction=0.2, very_late_fraction=0.05, duplicate_fraction=0.05)


@pytest.mark.parametrize(
    ("planned", "logged", "committed", "phase"),
    [
        (-1, -1, -1, "between batches"),
        (4, 4, 4, "between batches"),
        (5, 4, 4, "planned, not logged"),
        (5, 5, 4, "logged, not committed"),
    ],
)
def test_a_query_state_names_its_phase(planned: int, logged: int, committed: int, phase: str) -> None:
    assert QueryState(planned, logged, committed).phase == phase


def test_query_state_reads_the_three_logs(tmp_path: Path) -> None:
    paths = Paths(tmp_path)
    checkpoint = paths.checkpoint("raw_events")
    for directory, names in (
        (checkpoint / "offsets", ["0", "1", "2", ".2.crc"]),
        (checkpoint / "commits", ["0", "1"]),
        (paths.raw_events / "_spark_metadata", ["0", "1", "2"]),
    ):
        directory.mkdir(parents=True)
        for name in names:
            (directory / name).write_text("v1\n")
    assert query_state(paths, "raw_events") == QueryState(planned=2, logged=2, committed=1)
    assert query_state(paths, "dead_letters") == QueryState(-1, -1, -1)


@pytest.fixture
def topic() -> Iterator[str]:
    wait_for_broker(BOOTSTRAP, timeout_s=60)
    name = f"test-{uuid.uuid4()}"
    ensure_topic(BOOTSTRAP, name, partitions=6)
    yield name
    delete_topic(BOOTSTRAP, name)


@pytest.mark.kafka
@pytest.mark.spark
@pytest.mark.skipif(not BOOTSTRAP, reason="FLOWCAST_TEST_BOOTSTRAP not set (scripts/kafka.sh start)")
def test_killed_and_restarted_the_job_is_still_exact(topic: str, tmp_path: Path) -> None:
    events = [r.event for r in sessions(150, CONFIG)]
    produce(BOOTSTRAP, topic, events)
    out = tmp_path / "run"
    outcome = kill_and_restart(
        BOOTSTRAP,
        topic,
        out,
        kills=3,
        max_offsets=40,
        rng=random.Random(3),
        log=tmp_path / "stream.log",
        duckdb=True,
    )
    assert len(outcome.kills) + outcome.finished_early == 3
    assert outcome.kills, "at least one kill landed while the job ran"
    assert all(set(kill.states) == {*QUERIES, DUCKDB_QUERY} for kill in outcome.kills)

    report = check_run(BOOTSTRAP, topic, out)
    assert report.ok, "\n".join(report.lines())
    assert report.replay.unread == 0 and len(report.replay.dropped) > 0
    assert report.duckdb is not None and report.duckdb[0] == report.duckdb[1]
    con = connect(out)
    try:
        counts = con.execute('SELECT count(*), count(DISTINCT (partition, "offset")) FROM raw_events').fetchone()
    finally:
        con.close()
    assert counts == (len(events), len(events))
