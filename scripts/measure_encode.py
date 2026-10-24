"""Where encoding an event spends its time, and the old encode (dataclasses.astuple) against the current one.

    .venv/bin/python scripts/measure_encode.py [--sessions N] [--rounds R]

Generates the events once, then times each step over all of them, alternating the steps round by round so a change in
the machine's load hits each one alike, and prints each step's best round (events/s). Synthetic data.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Callable
from dataclasses import astuple

from flowcast.generator import Config, Generator
from flowcast.schema import FIELDS, Event, encode, validate, values


def old_encode(event: Event) -> bytes:
    """encode as it was at e2f0a30: the same checks and bytes, the values taken by dataclasses.astuple."""
    validate(event)
    return json.dumps(dict(zip(FIELDS, astuple(event), strict=True)), separators=(",", ":")).encode()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sessions", type=int, default=20_000)
    parser.add_argument("--rounds", type=int, default=7)
    args = parser.parse_args()
    events = [record.event for record in Generator(Config(), sessions=args.sessions)]
    assert all(old_encode(e) == encode(e) for e in events), "the two encoders must give the same bytes"
    steps: dict[str, Callable[[Event], object]] = {
        "encode at e2f0a30 (astuple)": old_encode,
        "encode now": encode,
        "  validate": validate,
        "  dataclasses.astuple": astuple,
        "  schema.values": values,
    }
    best = dict.fromkeys(steps, float("inf"))
    for _ in range(args.rounds):
        for name, step in steps.items():
            started = time.perf_counter()
            for event in events:
                step(event)
            best[name] = min(best[name], time.perf_counter() - started)
    print(f"{len(events):,} events, best of {args.rounds} rounds, load average {os.getloadavg()[0]:.2f} at the end")
    for name, seconds in best.items():
        print(f"{name:30} {len(events) / seconds:>12,.0f} events/s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
