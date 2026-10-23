"""scripts/kafka.sh against a real Kafka distribution: a broker of its own in a temporary directory, on ports of its
own. Starts a JVM, so it runs only with FLOWCAST_TEST_KAFKA_SCRIPT=1 (CI's kafka-script job; locally on demand)."""

import os
import socket
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest

from flowcast.generator import sessions
from flowcast.kafka_io import ensure_topic, produce, read_topic

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "kafka.sh"
PORT, CONTROLLER_PORT = 19192, 19193

pytestmark = pytest.mark.skipif(
    os.environ.get("FLOWCAST_TEST_KAFKA_SCRIPT") != "1", reason="FLOWCAST_TEST_KAFKA_SCRIPT=1 runs it"
)


def run(directory: Path, *args: str) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        "FLOWCAST_KAFKA_DIR": str(directory),
        "FLOWCAST_KAFKA_PORT": str(PORT),
        "FLOWCAST_KAFKA_CONTROLLER_PORT": str(CONTROLLER_PORT),
    }
    return subprocess.run(["bash", str(SCRIPT), *args], env=env, capture_output=True, text=True, timeout=120)


@pytest.fixture
def directory(tmp_path: Path) -> Iterator[Path]:
    path = tmp_path / "missing-parent" / "kafka"  # start must make the parents too
    yield path
    run(path, "stop")


def test_start_status_round_trip_stop_and_reset(directory: Path) -> None:
    started = run(directory, "start")
    assert started.returncode == 0, started.stderr
    assert f"on 127.0.0.1:{PORT}" in started.stdout
    again = run(directory, "start")
    assert again.returncode == 0 and "already running" in again.stdout
    assert run(directory, "status").returncode == 0
    assert run(directory, "bootstrap").stdout == f"127.0.0.1:{PORT}\n"

    bootstrap = f"127.0.0.1:{PORT}"
    ensure_topic(bootstrap, "clickstream")
    events = [record.event for record in sessions(50)]
    produce(bootstrap, "clickstream", events)
    assert len(read_topic(bootstrap, "clickstream")) == len(events)

    assert run(directory, "stop").returncode == 0
    assert run(directory, "status").returncode == 3
    # The data survives a restart.
    assert run(directory, "start").returncode == 0, run(directory, "logs").stdout
    assert len(read_topic(bootstrap, "clickstream")) == len(events)
    assert run(directory, "stop").returncode == 0

    reset = run(directory, "reset")
    assert reset.returncode == 0 and not directory.exists()


def test_start_refuses_a_port_in_use(directory: Path) -> None:
    with socket.create_server(("127.0.0.1", CONTROLLER_PORT)):
        result = run(directory, "start")
    assert result.returncode == 1
    assert f"listening on port {CONTROLLER_PORT}" in result.stderr


def test_start_and_reset_refuse_a_directory_the_script_did_not_make(tmp_path: Path) -> None:
    (tmp_path / "precious.txt").write_text("keep me")
    assert "wasn't made by this script" in run(tmp_path, "start").stderr
    assert "not deleting it" in run(tmp_path, "reset").stderr
    assert (tmp_path / "precious.txt").read_text() == "keep me"


def test_an_unknown_command_is_a_usage_error(tmp_path: Path) -> None:
    assert run(tmp_path, "restart").returncode == 2
