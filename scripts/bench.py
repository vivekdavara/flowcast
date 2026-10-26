"""Day 4's benchmarks: the job over a backlog at several batch sizes, and over a live stream at several triggers.

    .venv/bin/python scripts/bench.py sizes --topic bench-backlog --records 200000 --sizes 2000,10000,50000
    .venv/bin/python scripts/bench.py triggers --topic bench-live --seconds 300 --triggers 1,5,10

`sizes` produces the records once (unpaced) if the topic is empty, then for each size runs `flowcast stream
--available-now --max-offsets SIZE` from nothing in a fresh process and reports the wall time, events per second
(records over the job's whole run, JVM start included, and over the span of its batches), batches, and what the
check's replay says was dropped as late, next to what `batching.simulate` predicts from the records alone. `--topic`
may name an existing topic (day 3's `kill-d3` is kept); it is then read as it is.

`triggers` runs, for each trigger interval, a fresh topic and run: the job streaming with `--trigger T`, and the
producer sending in real time (`--speed 1 --start-ms now`) for about `--seconds`, after which the job gets a few
batches more and is stopped with SIGINT (so it saves its progress). It reports the latencies from `flowcast latency`.

Everything goes under `--out` (default data/bench, gitignored): a run directory per case, the job's log, and
`results.json` with every number printed. Needs a broker (`make kafka-start`) and Java 21 for Spark.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path

from flowcast.batching import simulate
from flowcast.check import check_run
from flowcast.generator import Config, take
from flowcast.kafka_io import DEFAULT_BOOTSTRAP, ensure_topic, produce, read_topic, topic_counts, wait_for_broker
from flowcast.latency import latencies, throughput

CLI = [sys.executable, "-m", "flowcast.cli"]


def clock_gap() -> float:
    """Wall-clock seconds minus monotonic seconds. On macOS the monotonic clock stops while the machine sleeps and the
    wall clock doesn't, so this grows by however long it slept: a case whose gap grew measured a sleep, not the job."""
    return time.time() - time.monotonic()


def records_in(bootstrap: str, topic: str) -> int:
    return sum(topic_counts(bootstrap, topic).values())


def stream_backlog(bootstrap: str, topic: str, out: Path, size: int) -> float:
    """Run the job over everything in the topic, `size` records a batch; the wall seconds it took."""
    command = [*CLI, "stream", "--bootstrap", bootstrap, "--topic", topic, "--out", str(out), "--available-now"]
    started = time.monotonic()
    with (out.parent / f"{out.name}.log").open("w") as log:
        subprocess.run([*command, "--max-offsets", str(size)], check=True, stdout=log, stderr=subprocess.STDOUT)
    return time.monotonic() - started


def sizes(args: argparse.Namespace) -> list[dict[str, object]]:
    ensure_topic(args.bootstrap, args.topic)
    if records_in(args.bootstrap, args.topic) == 0:
        events = (r.event for r in take(args.records, Config(seed=args.seed)))
        produced = produce(args.bootstrap, args.topic, events)
        print(f"produced {produced.records:,} records to {args.topic} at {produced.records_per_s:,.0f} records/s")
    records = read_topic(args.bootstrap, args.topic)
    total = len(records)
    print(f"{args.topic}: {total:,} records")
    rows = []
    for size in args.sizes:
        out = args.out / f"{args.topic}-size-{size}"
        if out.exists():
            raise SystemExit(f"{out} exists: remove it to measure again")
        gap = clock_gap()
        wall = stream_backlog(args.bootstrap, args.topic, out, size)
        slept = clock_gap() - gap
        raw = throughput(out, "raw_events")
        report = check_run(args.bootstrap, args.topic, out)
        row: dict[str, object] = {
            "size": size,
            "records": total,
            "wall_s": round(wall, 1),
            "slept_s": round(slept, 1),
            "records_per_wall_s": round(total / wall),
            "raw_events_batches": raw.batches if raw else 0,
            "raw_events_per_s": round(raw.rows_per_s) if raw else 0,
            "kept": len(report.replay.kept),
            "dropped_late": len(report.replay.dropped),
            "dropped_late_pct": round(100 * len(report.replay.dropped) / total, 3),
            # batching.simulate's prediction of the same, from the records alone (no Spark)
            "predicted_dropped": len(simulate(records, size, args.topic).dropped),
            "check_ok": report.ok,
        }
        print(json.dumps(row))
        rows.append(row)
    return rows


def rate(config: Config, sample: int = 100_000) -> float:
    """The generator's records per second of arrival time, from a prefix of its stream. Sessions overlap more once the
    stream is under way, so a short prefix underestimates it (20,000 records gave 302 a second where 375 arrived)."""
    records = take(sample, config)
    return (sample - 1) / ((records[-1].arrival_ms - records[0].arrival_ms) / 1000)


def triggers(args: argparse.Namespace) -> list[dict[str, object]]:
    config = Config(seed=args.seed, sessions_per_second=args.sessions_per_second)
    per_s = rate(config)
    count = int(per_s * args.seconds)
    print(f"{per_s:,.1f} records/s at {args.sessions_per_second:g} sessions/s: {count:,} records for {args.seconds} s")
    rows = []
    for trigger in args.triggers:
        topic = f"{args.topic}-{trigger:g}s"
        ensure_topic(args.bootstrap, topic)
        if records_in(args.bootstrap, topic):
            raise SystemExit(f"topic {topic} already holds records")
        out = args.out / topic
        if out.exists():
            raise SystemExit(f"{out} exists: remove it to measure again")
        out.parent.mkdir(parents=True, exist_ok=True)
        kafka = ["--bootstrap", args.bootstrap, "--topic", topic]
        with (args.out / f"{topic}.log").open("w") as log:
            job = subprocess.Popen(
                [*CLI, "stream", *kafka, "--out", str(out), "--trigger", str(trigger)],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            gap = clock_gap()
            try:
                time.sleep(args.warmup)  # Spark's start and the first (empty) batches, before anything is sent
                generator = ["--records", str(count), "--seed", str(args.seed)]
                generator += ["--sessions-per-second", str(args.sessions_per_second), "--start-ms", "now"]
                sending = time.monotonic()
                subprocess.run([*CLI, "produce", *kafka, *generator, "--speed", "1"], check=True, stdout=log)
                sent_s = time.monotonic() - sending
                time.sleep(max(3 * trigger, 10))  # the last records' batches
            finally:
                job.send_signal(signal.SIGINT)  # the job's Ctrl-C: stop the queries, save their progress
                job.wait(timeout=120)
            slept = clock_gap() - gap
        result = latencies(out)
        raw = throughput(out, "raw_events")
        summaries = {"produced": result.produced, "event": result.event, "window": result.window}
        row: dict[str, object] = {
            "trigger_s": trigger,
            "sent_s": round(sent_s, 1),
            "slept_s": round(slept, 1),
            "records": records_in(args.bootstrap, topic),
            "raw_events_batches": raw.batches if raw else 0,
            **{f"{name}_ms": asdict(s) if s else None for name, s in summaries.items()},
        }
        print(f"trigger {trigger:g} s: sent for {sent_s:.1f} s, slept {slept:.1f} s")
        for line in result.lines():
            print(f"  {line}")
        rows.append(row)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--out", type=Path, default=Path("data/bench"))
    parser.add_argument("--seed", type=int, default=1)
    sub = parser.add_subparsers(dest="bench", required=True)
    s = sub.add_parser("sizes", help="a backlog at several batch sizes")
    s.add_argument("--topic", required=True)
    s.add_argument("--records", type=int, default=200_000, help="produced if the topic is empty")
    s.add_argument("--sizes", type=lambda t: [int(x) for x in t.split(",")], default=[2_000, 10_000, 50_000])
    t = sub.add_parser("triggers", help="a live stream at several trigger intervals")
    t.add_argument("--topic", required=True, help="a prefix: each trigger gets its own topic")
    t.add_argument("--seconds", type=int, default=300, help="how long the producer sends (default %(default)s)")
    t.add_argument("--sessions-per-second", type=float, default=Config.sessions_per_second)
    t.add_argument("--triggers", type=lambda t: [float(x) for x in t.split(",")], default=[1.0, 5.0, 10.0])
    t.add_argument("--warmup", type=float, default=20.0, help="seconds between starting the job and the producer")
    args = parser.parse_args()
    os.environ.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@21")
    wait_for_broker(args.bootstrap, timeout_s=30)
    args.out.mkdir(parents=True, exist_ok=True)
    rows = sizes(args) if args.bench == "sizes" else triggers(args)
    results = args.out / "results.json"
    previous = json.loads(results.read_text()) if results.exists() else []
    stamp = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    results.write_text(json.dumps([*previous, {"bench": args.bench, "at": stamp, "rows": rows}], indent=2) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
