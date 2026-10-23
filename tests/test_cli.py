import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from flowcast import __version__
from flowcast.cli import main
from flowcast.generator import Config, sessions, take
from flowcast.kafka_io import delete_topic, read_topic
from flowcast.schema import decode, encode


def test_version_prints_the_package_version(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["version"]) == 0
    assert capsys.readouterr().out == f"flowcast {__version__}\n"


def test_a_missing_command_is_a_usage_error() -> None:
    with pytest.raises(SystemExit) as exc:
        main([])
    assert exc.value.code == 2


def test_generate_writes_the_wire_form_of_whole_sessions(tmp_path: Path) -> None:
    out = tmp_path / "events.jsonl"
    assert main(["generate", "--sessions", "50", "--seed", "3", "--out", str(out)]) == 0
    expected = b"".join(encode(r.event) + b"\n" for r in sessions(50, Config(seed=3)))
    assert out.read_bytes() == expected


def test_generate_records_is_a_prefix_of_the_endless_stream(capfdbinary: pytest.CaptureFixture[bytes]) -> None:
    assert main(["generate", "--records", "20"]) == 0
    assert capfdbinary.readouterr().out == b"".join(encode(r.event) + b"\n" for r in take(20))


def test_generate_passes_the_disorder_settings_on(tmp_path: Path) -> None:
    out = tmp_path / "events.jsonl"
    args = ["--duplicate-fraction", "0", "--late-fraction", "0.5", "--very-late-fraction", "0"]
    assert main(["generate", "--records", "2000", "--out", str(out), *args]) == 0
    config = Config(duplicate_fraction=0, late_fraction=0.5, very_late_fraction=0)
    assert out.read_bytes() == b"".join(encode(r.event) + b"\n" for r in take(2000, config))
    events = [decode(line) for line in out.read_bytes().splitlines()]
    assert len({e.event_id for e in events}) == 2000  # no duplicates


def test_truth_is_the_same_from_either_engine_and_from_a_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["truth", "--sessions", "300", "--seed", "5"]) == 0
    python = capsys.readouterr().out
    assert main(["truth", "--sessions", "300", "--seed", "5", "--engine", "duckdb"]) == 0
    assert capsys.readouterr().out == python
    out = tmp_path / "events.jsonl"
    assert main(["generate", "--sessions", "300", "--seed", "5", "--out", str(out)]) == 0
    assert main(["truth", "--input", str(out)]) == 0
    assert capsys.readouterr().out == python
    lines = python.splitlines()
    assert lines[0] == "window_start,window_start_ms,event_type,events,revenue_cents"
    assert lines[1].startswith("2026-01-01T00:00:00Z,1767225600000,")


def test_truth_names_the_bad_line_of_an_input_file(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    good = encode(take(1)[0].event)
    path = tmp_path / "events.jsonl"
    path.write_bytes(good + b"\n" + good.replace(b'"country":', b'"countryy":') + b"\n")
    assert main(["truth", "--input", str(path)]) == 1
    assert capsys.readouterr().err == f"flowcast truth: {path}:2: missing field country\n"


def test_a_missing_input_file_is_a_failure_not_a_traceback(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["truth", "--input", str(tmp_path / "none.jsonl")]) == 1
    assert "No such file" in capsys.readouterr().err


@pytest.mark.parametrize(
    "args",
    [
        ["generate", "--sessions", "-1"],
        ["generate", "--sessions", "x"],
        ["generate", "--sessions", "1", "--records", "1"],
        ["generate"],
        ["truth", "--engine", "spark", "--sessions", "1"],
    ],
)
def test_bad_arguments_are_usage_errors(args: list[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(args)
    assert exc.value.code == 2


def test_bad_generator_settings_are_usage_errors(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["generate", "--records", "1", "--late-fraction", "2"]) == 2
    assert capsys.readouterr().err == "flowcast generate: late_fraction must be between 0 and 1\n"


def test_no_broker_is_a_failure_after_the_wait(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["count", "--bootstrap", "127.0.0.1:1", "--wait", "0.5"]) == 1
    assert "no Kafka broker at 127.0.0.1:1" in capsys.readouterr().err


BOOTSTRAP = os.environ.get("FLOWCAST_TEST_BOOTSTRAP", "")


@pytest.mark.kafka
@pytest.mark.skipif(not BOOTSTRAP, reason="FLOWCAST_TEST_BOOTSTRAP not set (scripts/kafka.sh start)")
def test_topic_produce_and_count(capsys: pytest.CaptureFixture[str]) -> None:
    topic = f"test-{uuid.uuid4()}"
    kafka = ["--bootstrap", BOOTSTRAP, "--topic", topic]
    try:
        assert main(["topic", *kafka, "--partitions", "3"]) == 0
        assert capsys.readouterr().out == f"topic {topic}: created, 3 partitions\n"
        assert main(["topic", *kafka, "--partitions", "3"]) == 0
        assert capsys.readouterr().out == f"topic {topic}: exists, 3 partitions\n"
        assert main(["topic", *kafka]) == 1
        assert "has 3 partitions, not 6" in capsys.readouterr().err

        assert main(["produce", *kafka, "--sessions", "40", "--seed", "9"]) == 0
        records = sessions(40, Config(seed=9))
        assert capsys.readouterr().out.startswith(f"produced {len(records)} records to {topic} in ")
        assert sorted(r.value for r in read_topic(BOOTSTRAP, topic)) == sorted(encode(r.event) for r in records)

        assert main(["count", *kafka]) == 0
        out = capsys.readouterr().out.splitlines()
        assert out[-1] == f"total: {len(records)}"
        assert [line.split(":")[0] for line in out[:-1]] == ["partition 0", "partition 1", "partition 2"]
    finally:
        delete_topic(BOOTSTRAP, topic)


def test_a_topic_needs_at_least_one_partition(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["topic", "--partitions", "0", "--bootstrap", "127.0.0.1:1"]) == 2
    assert capsys.readouterr().err == "flowcast topic: --partitions must be between 1 and 2147483647\n"


def test_a_closed_pipe_ends_generate_quietly() -> None:
    process = subprocess.Popen(
        [sys.executable, "-m", "flowcast.cli", "generate", "--records", "500000"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdout is not None and process.stderr is not None
    assert process.stdout.readline().startswith(b'{"schema_version":1,')
    process.stdout.close()  # like `head -1` exiting
    stderr = process.stderr.read()
    assert process.wait(timeout=60) == 1
    assert stderr == b""  # no traceback, no "Exception ignored ... BrokenPipeError"
