"""Second review (943596b..a403362): kafka.sh's new running_pid, which accepts a pid only if its command line holds
the exact string " $DIR/server.properties". No JVM: fake Kafka tools stand in (as in tests/review), ports
19596/19597.
"""

import contextlib
import os
import signal
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "kafka.sh"
PORT, CONTROLLER_PORT = 19596, 19597


def fake_tools(root: Path) -> tuple[Path, Path]:
    bin_dir, java_home = root / "kafka-bin", root / "java"
    bin_dir.mkdir()
    (java_home / "bin").mkdir(parents=True)
    tools = {
        "kafka-storage": '#!/bin/bash\nif [ "$1" = random-uuid ]; then echo fake-cluster-id; exit 0; fi\n'
        'dir=$(sed -n "s/^log.dirs=//p" "$5"); mkdir -p "$dir"; echo id > "$dir/meta.properties"\n',
        # Like the real server: the config path, as passed, stays on the process's command line.
        "kafka-server-start": '#!/bin/bash\necho "[KafkaRaftServer nodeId=1] Kafka Server started"\n'
        'exec -a "java kafka.Kafka $1" sleep 60\n',
        "../java/bin/java": "#!/bin/bash\n",
    }
    for name, text in tools.items():
        path = bin_dir / name
        path.write_text(text)
        path.chmod(0o755)
    return bin_dir, java_home


def run(
    directory: str, tools: tuple[Path, Path], *args: str, cwd: Path | None = None
) -> subprocess.CompletedProcess[str]:
    env = {
        **os.environ,
        "FLOWCAST_KAFKA_DIR": directory,
        "FLOWCAST_KAFKA_PORT": str(PORT),
        "FLOWCAST_KAFKA_CONTROLLER_PORT": str(CONTROLLER_PORT),
        "KAFKA_BIN": str(tools[0]),
        "FLOWCAST_JAVA_HOME": str(tools[1]),
    }
    if cwd is not None:
        env["PWD"] = str(cwd)
    return subprocess.run(["bash", str(SCRIPT), *args], env=env, capture_output=True, text=True, timeout=60, cwd=cwd)


def alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


@pytest.fixture
def setup(tmp_path: Path) -> Iterator[tuple[Path, Path, tuple[Path, Path], list[int]]]:
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    started: list[int] = []
    yield real, link, fake_tools(tmp_path), started
    for pid in started:
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGKILL)


def test_stop_by_another_spelling_of_the_same_directory_stops_the_broker(
    setup: tuple[Path, Path, tuple[Path, Path], list[int]],
) -> None:
    # The broker was started through a symlink (on macOS: /tmp vs /private/tmp, /var vs /private/var, or any symlinked
    # checkout); `stop` is run with the directory's real path. Same directory, same pid file, same broker. running_pid
    # compares strings, so stop says "kafka not running" -- and then deletes the pid file, so the live broker is
    # orphaned: no later stop/status can find it, start dies on "something else is listening", and `reset` rm -rf's
    # the data directory under the running broker. At 943596b (kill -0 only) this stop worked.
    real, link, tools, started = setup
    directory = str(link / "kafka")
    assert run(directory, tools, "start").returncode == 0
    pid = int((real / "kafka" / "kafka.pid").read_text())
    started.append(pid)

    stopped = run(str(real / "kafka"), tools, "stop")
    time.sleep(0.3)
    assert not alive(pid), f"stop left the broker running: {stopped.stdout!r}"


def test_a_relative_dir_spelled_differently_still_finds_the_broker(
    setup: tuple[Path, Path, tuple[Path, Path], list[int]],
) -> None:
    # FLOWCAST_KAFKA_DIR=kafka and FLOWCAST_KAFKA_DIR=./kafka from the same working directory name the same files, but
    # DIR becomes "$PWD/kafka" vs "$PWD/./kafka", so `status` reports a running broker as stopped (and `stop` would
    # delete its pid file and leave it running).
    real, _link, tools, started = setup
    assert run("kafka", tools, "start", cwd=real).returncode == 0
    pid = int((real / "kafka" / "kafka.pid").read_text())
    started.append(pid)

    status = run("./kafka", tools, "status", cwd=real)
    assert status.returncode == 0, f"status of a running broker: {status.stdout!r}"


def test_stop_keeps_the_pid_file_of_a_live_process_it_cannot_verify(
    setup: tuple[Path, Path, tuple[Path, Path], list[int]],
) -> None:
    # Whatever the spelling, deleting the pid file of a pid that is alive but unverified throws away the only record
    # of the broker. Expected: keep the file (or say so) rather than print "kafka not running".
    real, link, tools, started = setup
    assert run(str(link / "kafka"), tools, "start").returncode == 0
    pid_file = real / "kafka" / "kafka.pid"
    started.append(int(pid_file.read_text()))
    run(str(real / "kafka"), tools, "stop")
    assert pid_file.exists() or not alive(started[0]), "the pid file of a live broker was deleted"
