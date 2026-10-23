"""The `flowcast` command line.

Exit codes: 0 done, 1 the work failed (Kafka, a bad input file), 2 a usage error (arguments or generator settings).
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path

from flowcast import __version__
from flowcast.generator import Config, Emitted, Generator
from flowcast.kafka_io import (
    DEFAULT_BOOTSTRAP,
    PARTITIONS,
    TOPIC,
    KafkaError,
    ensure_topic,
    produce,
    topic_counts,
    wait_for_broker,
)
from flowcast.schema import Event, InvalidEvent, decode, encode
from flowcast.truth import duckdb_minute_aggregates, minute_aggregates


class UsageError(Exception):
    pass


def _positive(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a whole number: {text!r}") from None
    if value < 0:
        raise argparse.ArgumentTypeError(f"must not be negative: {text}")
    return value


def _stream_options(parser: argparse.ArgumentParser, *, from_file: bool = False) -> None:
    size = parser.add_mutually_exclusive_group(required=True)
    size.add_argument("--sessions", type=_positive, help="every record of this many whole sessions")
    size.add_argument("--records", type=_positive, help="the first this-many records of the endless stream")
    if from_file:
        size.add_argument("--input", type=Path, help="a file of JSON lines instead (each line checked)")
    parser.add_argument("--seed", type=int, default=Config.seed, help="generator seed (default %(default)s)")
    parser.add_argument("--duplicate-fraction", type=float, default=Config.duplicate_fraction)
    parser.add_argument("--late-fraction", type=float, default=Config.late_fraction)
    parser.add_argument("--very-late-fraction", type=float, default=Config.very_late_fraction)


def _kafka_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap servers (default %(default)s)")
    parser.add_argument("--topic", default=TOPIC, help="topic (default %(default)s)")
    parser.add_argument("--wait", type=float, default=30.0, help="seconds to wait for the broker (default %(default)s)")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="flowcast", description="FlowCast: a clickstream through Kafka and Spark.")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("version", help="print the version")

    generate = sub.add_parser("generate", help="write generated events as JSON lines, in arrival order")
    _stream_options(generate)
    generate.add_argument("--out", type=Path, help="file to write (default: standard output)")

    truth = sub.add_parser("truth", help="print the exact minute aggregates of generated (or given) events as CSV")
    _stream_options(truth, from_file=True)
    truth.add_argument("--engine", choices=("python", "duckdb"), default="python")

    topic = sub.add_parser("topic", help="create the topic if it doesn't exist")
    _kafka_options(topic)
    topic.add_argument("--partitions", type=_positive, default=PARTITIONS)

    send = sub.add_parser("produce", help="send generated events to Kafka, in arrival order")
    _kafka_options(send)
    _stream_options(send)

    count = sub.add_parser("count", help="records in each partition of a topic")
    _kafka_options(count)
    return parser


def _config(args: argparse.Namespace) -> Config:
    try:
        return Config(
            seed=args.seed,
            duplicate_fraction=args.duplicate_fraction,
            late_fraction=args.late_fraction,
            very_late_fraction=args.very_late_fraction,
        )
    except ValueError as exc:
        raise UsageError(str(exc)) from exc


def _records(args: argparse.Namespace) -> Iterator[Emitted]:
    config = _config(args)
    if args.sessions is not None:
        return iter(Generator(config, sessions=args.sessions))
    return islice(iter(Generator(config)), args.records)


def _read_events(path: Path) -> Iterator[Event]:
    with path.open("rb") as lines:
        for number, line in enumerate(lines, start=1):
            try:
                yield decode(line.removesuffix(b"\n"))
            except InvalidEvent as exc:
                raise InvalidEvent(f"{path}:{number}: {exc}") from None


def _generate(args: argparse.Namespace) -> None:
    records = _records(args)
    if args.out is None:
        out = sys.stdout.buffer
        for record in records:
            out.write(encode(record.event) + b"\n")
        out.flush()
        return
    with args.out.open("wb") as file:
        for record in records:
            file.write(encode(record.event) + b"\n")


def _truth(args: argparse.Namespace) -> None:
    if args.input is not None:
        events: list[Event] = list(_read_events(args.input))
    else:
        events = [record.event for record in _records(args)]
    rows = minute_aggregates(events) if args.engine == "python" else duckdb_minute_aggregates(events)
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(["window_start", "window_start_ms", "event_type", "events", "revenue_cents"])
    for row in rows:
        start = datetime.fromtimestamp(row.window_start / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        writer.writerow([start, row.window_start, row.event_type, row.events, row.revenue_cents])


def _topic(args: argparse.Namespace) -> None:
    if args.partitions < 1:
        raise UsageError("--partitions must be at least 1")
    wait_for_broker(args.bootstrap, timeout_s=args.wait)
    created = ensure_topic(args.bootstrap, args.topic, partitions=args.partitions)
    print(f"topic {args.topic}: {'created' if created else 'exists'}, {args.partitions} partitions")


def _produce(args: argparse.Namespace) -> None:
    records = _records(args)
    wait_for_broker(args.bootstrap, timeout_s=args.wait)
    produced = produce(args.bootstrap, args.topic, (record.event for record in records))
    print(f"produced {produced.records} records to {args.topic} in {produced.seconds:.2f} s")


def _count(args: argparse.Namespace) -> None:
    wait_for_broker(args.bootstrap, timeout_s=args.wait)
    counts = topic_counts(args.bootstrap, args.topic)
    for partition, records in counts.items():
        print(f"partition {partition}: {records}")
    print(f"total: {sum(counts.values())}")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    commands = {
        "generate": _generate,
        "truth": _truth,
        "topic": _topic,
        "produce": _produce,
        "count": _count,
    }
    if args.command == "version":
        print(f"flowcast {__version__}")
        return 0
    try:
        commands[args.command](args)
    except BrokenPipeError:
        # The reader went away (`flowcast generate | head`): stop quietly, and keep Python's own flush at exit from
        # failing on the same pipe.
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        return 1
    except UsageError as exc:
        print(f"flowcast {args.command}: {exc}", file=sys.stderr)
        return 2
    except (KafkaError, InvalidEvent, OSError) as exc:
        print(f"flowcast {args.command}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
