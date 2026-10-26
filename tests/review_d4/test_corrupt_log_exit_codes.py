"""A sink log line that isn't JSON: `quality` turns json's ValueError into a usage error (exit 2, cli.py _quality's
`except ValueError`), and `latency` doesn't catch it at all (traceback). Both should exit 1 (bad input files)."""

from flowcast.cli import main


def _corrupt(tmp_path):
    log = tmp_path / "raw_events" / "_spark_metadata"
    log.mkdir(parents=True)
    (log / "0").write_text("v1\n{torn")


def test_quality_exits_1(tmp_path):
    _corrupt(tmp_path)
    assert main(["quality", "--out", str(tmp_path)]) == 1


def test_latency_exits_1(tmp_path):
    _corrupt(tmp_path)
    assert main(["latency", "--out", str(tmp_path)]) == 1


def test_a_torn_progress_line_is_skipped(tmp_path):
    from flowcast.latency import throughput
    from flowcast.layout import Paths

    path = Paths(tmp_path).progress("raw_events")
    path.parent.mkdir(parents=True)
    good = '{"numInputRows": 5, "timestamp": "2026-10-26T19:00:00.000Z", "durationMs": {"triggerExecution": 1000}}'
    path.write_text(good + '\n{"numInputRows": 3, "timest')
    result = throughput(tmp_path, "raw_events")
    assert result is not None and result.rows == 5
