"""Review of 943596b: cli.py's exit codes (0 done, 1 the work failed, 2 a usage error), not tracebacks or hangs."""

import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from flowcast.cli import main
from flowcast.kafka_io import delete_topic

SRC = Path(__file__).resolve().parents[2] / "src"
BOOTSTRAP = os.environ.get("FLOWCAST_TEST_BOOTSTRAP", "")


def test_a_wait_of_nan_seconds_is_a_usage_error_not_a_hang() -> None:
    # `--wait nan` passes type=float; in wait_for_broker every comparison with the NaN deadline is False, so with no
    # broker it never times out (sleep(max(0.0, nan)) is sleep(0): a busy retry loop, forever).
    env = {**os.environ, "PYTHONPATH": str(SRC)}
    command = [sys.executable, "-m", "flowcast.cli", "count", "--bootstrap", "127.0.0.1:1", "--wait", "nan"]
    try:
        result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=20)
    except subprocess.TimeoutExpired:
        pytest.fail("flowcast count --wait nan was still waiting after 20 s")
    assert result.returncode == 2, result.stderr


@pytest.mark.kafka
@pytest.mark.skipif(not BOOTSTRAP, reason="FLOWCAST_TEST_BOOTSTRAP not set (scripts/kafka.sh start)")
def test_a_partition_count_too_big_for_kafka_is_an_error_not_a_traceback(capsys: pytest.CaptureFixture[str]) -> None:
    # _positive has no upper bound; NewTopic raises OverflowError ("signed integer is greater than maximum") for
    # 99999999999, which main() doesn't catch.
    topic = f"review-{uuid.uuid4()}"
    try:
        code = main(["topic", "--bootstrap", BOOTSTRAP, "--topic", topic, "--partitions", "99999999999"])
        assert code in (1, 2)
        assert capsys.readouterr().err.startswith("flowcast topic: ")
    finally:
        delete_topic(BOOTSTRAP, topic)
