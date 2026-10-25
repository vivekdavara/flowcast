"""Kill the streaming job with SIGKILL at random moments, restart it each time, then check the run exactly.

    .venv/bin/python scripts/kill_restart.py --topic kill-demo --sessions 3000 --kills 10 --out data/kill-demo

Produces the sessions to a new topic, runs `flowcast stream --available-now --duckdb` in rounds that each end in a
SIGKILL of the whole process group (flowcast.recovery), lets a last round finish, and runs `check_run`: every output,
the DuckDB sink's included, must reconcile exactly with the replay of its own query's batches, with no record counted
twice. Prints where each
query's last batch was when each kill landed. Needs a broker (`make kafka-start`).
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from pathlib import Path

from confluent_kafka import Producer

from flowcast.check import check_run
from flowcast.generator import Config, sessions
from flowcast.kafka_io import DEFAULT_BOOTSTRAP, ensure_topic, produce, topic_counts, wait_for_broker
from flowcast.recovery import QUERIES, kill_and_restart
from flowcast.schema import encode
from flowcast.views import connect


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", required=True, help="a new topic (refused if it holds records)")
    parser.add_argument("--out", type=Path, required=True, help="a new run directory (refused if it exists)")
    parser.add_argument("--sessions", type=int, default=3000)
    parser.add_argument(
        "--sessions-per-second",
        type=float,
        default=0.5,
        help="event-time rate; spread out so the watermark closes windows (default %(default)s)",
    )
    parser.add_argument("--kills", type=int, default=10)
    parser.add_argument("--max-offsets", type=int, default=300, help="records per batch (default %(default)s)")
    parser.add_argument("--max-wait", type=float, default=2.0, help="a kill lands up to this many seconds into a batch")
    parser.add_argument("--seed", type=int, default=1, help="seeds the generator and the kill times")
    args = parser.parse_args()
    if args.out.exists():
        parser.error(f"{args.out} exists; a kill run starts from nothing")

    wait_for_broker(args.bootstrap, timeout_s=30)
    ensure_topic(args.bootstrap, args.topic)
    if sum(topic_counts(args.bootstrap, args.topic).values()):
        parser.error(f"topic {args.topic} already holds records")
    config = Config(seed=args.seed, sessions_per_second=args.sessions_per_second)
    events = [r.event for r in sessions(args.sessions, config)]
    half = len(events) // 2
    # A few records decode refuses, in the middle of the stream, so the dead-letter query has work too.
    bad = [b"{", b"null", encode(events[0])[:-1] + b',"extra":1}']
    produced = produce(args.bootstrap, args.topic, events[:half])
    producer = Producer({"bootstrap.servers": args.bootstrap})
    for value in bad:
        producer.produce(args.topic, value=value, key=b"u0")
    if producer.flush(30):
        raise RuntimeError("the invalid records weren't all delivered")
    produced_rest = produce(args.bootstrap, args.topic, events[half:])
    print(
        f"produced {produced.records + produced_rest.records} events and {len(bad)} invalid records ({args.sessions} "
        f"sessions, {args.sessions_per_second:g} a second of event time, seed {args.seed}) to {args.topic}"
    )

    args.out.mkdir(parents=True)
    started = time.monotonic()
    outcome = kill_and_restart(
        args.bootstrap,
        args.topic,
        args.out,
        kills=args.kills,
        max_offsets=args.max_offsets,
        rng=random.Random(args.seed),
        max_wait_s=args.max_wait,
        log=args.out / "stream.log",
        duckdb=True,
    )
    for kill in outcome.kills:
        where = "; ".join(
            f"{name} {s.phase} (planned {s.planned}, logged {s.logged}, committed {s.committed})"
            for name, s in kill.states.items()
        )
        print(f"kill {kill.round} at {kill.seconds:.1f} s: {where}")
    phases = outcome.phases()
    print(
        f"{len(outcome.kills)} kills ({outcome.finished_early} rounds finished before their kill), "
        f"{len(QUERIES) + 1} queries each: " + ", ".join(f"{n} {phase}" for phase, n in sorted(phases.items()))
    )
    print(f"last round {outcome.final_seconds:.1f} s; all rounds {time.monotonic() - started:.1f} s")

    report = check_run(args.bootstrap, args.topic, args.out)
    print("\n".join(report.lines()))
    con = connect(args.out)
    try:
        row = con.execute('SELECT count(*), count(DISTINCT (partition, "offset")) FROM raw_events').fetchone()
    finally:
        con.close()
    assert row is not None
    print(f"raw_events: {row[0]} rows, {row[1]} distinct Kafka records")
    return 0 if report.ok and row[0] == row[1] else 1


if __name__ == "__main__":
    sys.exit(main())
