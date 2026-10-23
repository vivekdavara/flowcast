"""Review of 943596b: scripts/kafka.sh's pid handling. No JVM: fake Kafka tools and a fake java stand in, on ports of
their own (19492/19493), in a temporary directory."""

import contextlib
import os
import signal
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "kafka.sh"
PORT, CONTROLLER_PORT = 19492, 19493


def fake_tools(root: Path) -> tuple[Path, Path]:
    """A KAFKA_BIN whose server prints the readiness line and sleeps, and a JAVA_HOME with an executable bin/java."""
    bin_dir, java_home = root / "kafka-bin", root / "java"
    bin_dir.mkdir()
    (java_home / "bin").mkdir(parents=True)
    tools = {
        "kafka-storage": '#!/bin/bash\nif [ "$1" = random-uuid ]; then echo fake-cluster-id; exit 0; fi\n'
        'dir=$(sed -n "s/^log.dirs=//p" "$5"); mkdir -p "$dir"; echo id > "$dir/meta.properties"\n',
        # Keeps the config path on its command line, as the real server does, for a fixed script's pid check.
        "kafka-server-start": '#!/bin/bash\necho "[KafkaRaftServer nodeId=1] Kafka Server started"\n'
        'exec -a "java kafka.Kafka $1" sleep 60\n',
        "../java/bin/java": "#!/bin/bash\n",
    }
    for name, text in tools.items():
        path = bin_dir / name
        path.write_text(text)
        path.chmod(0o755)
    return bin_dir, java_home


def run(directory: Path, tools: tuple[Path, Path], *args: str) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        "FLOWCAST_KAFKA_DIR": str(directory),
        "FLOWCAST_KAFKA_PORT": str(PORT),
        "FLOWCAST_KAFKA_CONTROLLER_PORT": str(CONTROLLER_PORT),
        "KAFKA_BIN": str(tools[0]),
        "FLOWCAST_JAVA_HOME": str(tools[1]),
    }
    return subprocess.run(["bash", str(SCRIPT), *args], env=env, capture_output=True, text=True, timeout=60)


@pytest.fixture
def bystander() -> Iterator[subprocess.Popen[bytes]]:
    """A process of ours that isn't Kafka: what a stale kafka.pid points at once its pid is reused (after a reboot or a
    crash that left the pid file)."""
    process = subprocess.Popen(["sleep", "120"], start_new_session=True)
    yield process
    process.kill()
    process.wait()


@pytest.fixture
def stale(tmp_path: Path, bystander: subprocess.Popen[bytes]) -> Iterator[tuple[Path, tuple[Path, Path]]]:
    directory = tmp_path / "kafka"
    directory.mkdir()
    (directory / ".flowcast-kafka").touch()  # a directory the script made, from an earlier run
    (directory / "kafka.pid").write_text(f"{bystander.pid}\n")
    tools = fake_tools(tmp_path)
    yield directory, tools
    # Whatever the script started here (the fake server) must not outlive the test.
    pid_file = directory / "kafka.pid"
    if pid_file.exists():
        pid = int(pid_file.read_text())
        if pid != bystander.pid:
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)


def alive(process: subprocess.Popen[bytes]) -> bool:
    time.sleep(0.3)
    return process.poll() is None


def test_stop_does_not_kill_another_process_that_has_the_stale_pid(
    stale: tuple[Path, tuple[Path, Path]], bystander: subprocess.Popen[bytes]
) -> None:
    # running_pid only checks `kill -0 $pid`, so any live process of ours with that pid counts as "kafka": stop sends it
    # SIGTERM (and SIGKILL 30 s later). Expected: check the pid is this directory's Kafka (its command line names
    # $DIR/server.properties) before signalling it, and treat the pid file as stale otherwise.
    directory, tools = stale
    run(directory, tools, "stop")
    assert alive(bystander), "kafka.sh stop killed a process that wasn't its Kafka"


def test_status_and_start_do_not_take_a_reused_pid_for_kafka(
    stale: tuple[Path, tuple[Path, Path]], bystander: subprocess.Popen[bytes]
) -> None:
    # The same check makes `status` say "running" and `start` say "already running" and start nothing, so a script or
    # test that runs `start` then connects fails with no broker.
    directory, tools = stale
    status = run(directory, tools, "status")
    assert status.returncode == 3, status.stdout
    started = run(directory, tools, "start")
    assert "already running" not in started.stdout
    assert started.returncode == 0, started.stderr
    run(directory, tools, "stop")
