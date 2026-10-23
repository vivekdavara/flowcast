"""How fast the generator makes records, with and without encoding, and how much it holds in memory.

    .venv/bin/python scripts/measure_generator.py [--sessions N] [--runs R]

Prints the median of R runs of each step and the conditions (machine, Python, power). Synthetic data; the numbers
are one laptop's, for sizing day 2's producer runs, not a benchmark of anything else.
"""

from __future__ import annotations

import argparse
import platform
import statistics
import subprocess
import sys
import time

from flowcast.generator import Config, Generator
from flowcast.schema import encode


def power() -> str:
    try:
        out = subprocess.run(["pmset", "-g", "batt"], capture_output=True, text=True, timeout=5).stdout
    except (OSError, subprocess.SubprocessError):
        return "unknown"
    first, _, rest = out.partition("\n")
    source = first.split("'")[1] if "'" in first else first.strip()
    percent = next((part.strip() for part in rest.split(";") if part.strip().endswith("%")), "")
    return f"{source} {percent.split()[-1] if percent else ''}".strip()


def cpu() -> str:
    try:
        out = subprocess.run(["sysctl", "-n", "machdep.cpu.brand_string"], capture_output=True, text=True, timeout=5)
    except (OSError, subprocess.SubprocessError):
        return platform.machine()
    return out.stdout.strip() or platform.machine()


def run(sessions: int, encode_too: bool) -> tuple[int, float, int]:
    generator = Generator(Config(), sessions=sessions)
    records = peak = 0
    started = time.perf_counter()
    for record in generator:
        if encode_too:
            encode(record.event)
        records += 1
        if records % 1024 == 0:  # sampled: reading it on every record would slow what is being measured
            peak = max(peak, generator.pending)
    return records, time.perf_counter() - started, peak


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sessions", type=int, default=50_000)
    parser.add_argument("--runs", type=int, default=5)
    args = parser.parse_args()
    print(f"machine: {cpu()}, {platform.system()} {platform.mac_ver()[0] or platform.release()}")
    print(f"python: {sys.version.split()[0]}")
    print(f"power: {power()}")
    print(f"sessions: {args.sessions:,}, runs: {args.runs} (median shown)")
    for label, encode_too in (("generate", False), ("generate + encode", True)):
        results = [run(args.sessions, encode_too) for _ in range(args.runs)]
        records = results[0][0]
        seconds = statistics.median(r[1] for r in results)
        peak = max(r[2] for r in results)
        rate = records / seconds
        print(f"{label:>18}: {records:,} records in {seconds:.3f} s = {rate:,.0f} records/s; peak pending {peak:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
