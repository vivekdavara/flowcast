"""Kill the streaming job at random moments and restart it: exactly once under real SIGKILLs (DESIGN.md "Exactly once").

Each round starts `flowcast stream --available-now` in its own process group, waits until the job plans a batch beyond
what the round started with (so every round moves the run forward), lets it run a random while longer, and kills the
whole group (the Python driver, Spark's JVM and its Python workers) with SIGKILL: no shutdown hook, no flush. What
each query's checkpoint and sink held at that moment is recorded. A last round runs to the end, and `check_run` then
has to find the run exact.
"""

from __future__ import annotations

import contextlib
import os
import random
import signal
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from flowcast.layout import Paths
from flowcast.views import latest_batch

QUERIES = ("dead_letters", "raw_events", "minute_aggregates")


def _last(directory: Path) -> int:
    if not directory.is_dir():
        return -1
    return max((int(p.name) for p in directory.iterdir() if p.name.isdigit()), default=-1)


@dataclass(frozen=True)
class QueryState:
    """Where a query's last batch had got to: planned (offset log), logged (sink log), committed (commit log)."""

    planned: int
    logged: int
    committed: int

    @property
    def phase(self) -> str:
        if self.planned == self.committed:
            return "between batches"
        if self.logged > self.committed:
            return "logged, not committed"  # the sink's log is ahead: on restart the sink must skip the batch
        return "planned, not logged"  # mid-batch: its files (if any) become orphans, and the batch runs again


def query_state(paths: Paths, name: str) -> QueryState:
    checkpoint = paths.checkpoint(name)
    sink = getattr(paths, name)
    assert isinstance(sink, Path)
    return QueryState(_last(checkpoint / "offsets"), latest_batch(sink), _last(checkpoint / "commits"))


@dataclass(frozen=True)
class Kill:
    round: int
    seconds: float  # from starting the job to the kill
    states: dict[str, QueryState]


@dataclass(frozen=True)
class Outcome:
    kills: list[Kill]
    finished_early: int  # rounds in which the job read everything and stopped before its kill
    final_seconds: float  # the last, uninterrupted round

    def phases(self) -> Counter[str]:
        return Counter(state.phase for kill in self.kills for state in kill.states.values())


def stream_command(bootstrap: str, topic: str, out: Path, max_offsets: int) -> list[str]:
    return [
        sys.executable,
        "-m",
        "flowcast.cli",
        "stream",
        "--bootstrap",
        bootstrap,
        "--topic",
        topic,
        "--out",
        str(out),
        "--available-now",
        "--max-offsets",
        str(max_offsets),
    ]


def _kill_group(process: subprocess.Popen[bytes], timeout_s: float) -> None:
    """SIGKILL every process in the job's group, then wait until none is left."""
    with contextlib.suppress(ProcessLookupError):  # the whole group already gone
        os.killpg(process.pid, signal.SIGKILL)
    process.wait(timeout_s)
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            os.killpg(process.pid, 0)  # anything (a JVM, a Python worker) still in the group?
        except ProcessLookupError:
            return
        except PermissionError:
            # macOS: a group whose only members are still exiting (`ps` state E, already reparented to launchd)
            # answers EPERM, not ESRCH. Seen with Spark's JVM a few milliseconds after the kill; it clears.
            pass
        if time.monotonic() > deadline:
            raise TimeoutError(f"processes of group {process.pid} outlived SIGKILL for {timeout_s} s")
        time.sleep(0.05)


def kill_and_restart(
    bootstrap: str,
    topic: str,
    out: Path,
    *,
    kills: int,
    max_offsets: int,
    rng: random.Random,
    max_wait_s: float = 2.0,
    log: Path | None = None,
    timeout_s: float = 300.0,
) -> Outcome:
    """`kills` rounds that each end in a SIGKILL at a random moment up to `max_wait_s` after the job plans a new
    batch, then one round that runs to the end. Raises if the job fails on its own or the last round doesn't finish."""
    paths = Paths(out)
    command = stream_command(bootstrap, topic, out, max_offsets)
    stderr = log.open("ab") if log is not None else subprocess.DEVNULL
    results: list[Kill] = []
    finished_early = 0
    try:
        for n in range(kills):
            planned_before = _last(paths.checkpoint("minute_aggregates") / "offsets")
            started = time.monotonic()
            process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=stderr, start_new_session=True)
            try:
                while _last(paths.checkpoint("minute_aggregates") / "offsets") <= planned_before:
                    if process.poll() is not None or time.monotonic() - started > timeout_s:
                        break
                    time.sleep(0.02)
                if process.poll() is None:
                    time.sleep(rng.uniform(0, max_wait_s))
            finally:
                exited = process.poll()
                _kill_group(process, timeout_s=30)
            if exited is not None:
                if exited != 0:
                    raise RuntimeError(f"round {n}: the job exited with {exited} before it was killed")
                finished_early += 1
                continue
            states = {name: query_state(paths, name) for name in QUERIES}
            results.append(Kill(n, time.monotonic() - started, states))
        started = time.monotonic()
        done = subprocess.run(command, stdout=subprocess.DEVNULL, stderr=stderr, timeout=timeout_s, check=False)
        if done.returncode != 0:
            raise RuntimeError(f"the last round exited with {done.returncode}")
        return Outcome(results, finished_early, time.monotonic() - started)
    finally:
        if log is not None and not isinstance(stderr, int):
            stderr.close()
