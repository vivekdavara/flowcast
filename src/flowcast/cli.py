"""The `flowcast` command line.

Exit codes: 0 done, 1 the work failed (Kafka, a bad input file), 2 a usage error (arguments or generator settings).
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import sys
from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path

import duckdb

from flowcast import __version__
from flowcast.check import CheckpointError, check_run
from flowcast.generator import Config, Emitted, Generator
from flowcast.kafka_io import (
    DEFAULT_BOOTSTRAP,
    MAX_PARTITIONS,
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
from flowcast.views import SinkLogError, connect

DEFAULT_OUT = Path("data/run")


class UsageError(Exception):
    pass


class StreamFailed(Exception):
    """The job or a check of it failed; the message says how."""


def _positive(text: str) -> int:
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a whole number: {text!r}") from None
    if value < 0:
        raise argparse.ArgumentTypeError(f"must not be negative: {text}")
    return value


def _seconds(text: str) -> float:
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a number of seconds: {text!r}") from None
    # NaN compares false with everything, so a deadline of NaN never passes: refuse it, and infinity, outright.
    if not math.isfinite(value) or value < 0:
        raise argparse.ArgumentTypeError(f"must be a finite number of seconds, at least 0: {text}")
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
    parser.add_argument(
        "--wait", type=_seconds, default=30.0, help="seconds to wait for the broker (default %(default)s)"
    )


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

    stream = sub.add_parser("stream", help="run the Spark job from the topic to Parquet under --out (Ctrl-C stops it)")
    _kafka_options(stream)
    stream.add_argument("--out", type=Path, default=DEFAULT_OUT, help="output directory (default %(default)s)")
    stream.add_argument("--trigger", type=_seconds, default=5.0, help="seconds between batches (default %(default)s)")
    stream.add_argument("--max-offsets", type=_positive, help="at most this many records a batch")
    stream.add_argument("--available-now", action="store_true", help="read what the topic holds now, then stop")

    check = sub.add_parser("check", help="check a stopped run's outputs exactly against the records in the topic")
    _kafka_options(check)
    check.add_argument("--out", type=Path, default=DEFAULT_OUT, help="the run's directory (default %(default)s)")

    query = sub.add_parser("query", help="run SQL over a run's DuckDB views and print CSV")
    query.add_argument("sql", help="e.g. 'SELECT * FROM minute_aggregates ORDER BY window_start LIMIT 10'")
    query.add_argument("--out", type=Path, default=DEFAULT_OUT, help="the run's directory (default %(default)s)")
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
    if not 1 <= args.partitions <= MAX_PARTITIONS:
        raise UsageError(f"--partitions must be between 1 and {MAX_PARTITIONS}")
    wait_for_broker(args.bootstrap, timeout_s=args.wait)
    created = ensure_topic(args.bootstrap, args.topic, partitions=args.partitions)
    print(f"topic {args.topic}: {'created' if created else 'exists'}, {args.partitions} partitions")


def _produce(args: argparse.Namespace) -> None:
    records = _records(args)
    wait_for_broker(args.bootstrap, timeout_s=args.wait)
    produced = produce(args.bootstrap, args.topic, (record.event for record in records))
    print(f"produced {produced.records} records to {args.topic} in {produced.seconds:.2f} s")


def _stream(args: argparse.Namespace) -> None:
    if args.max_offsets == 0:
        raise UsageError("--max-offsets must be at least 1")
    wait_for_broker(args.bootstrap, timeout_s=args.wait)
    topic_counts(args.bootstrap, args.topic)  # the topic must exist: the job doesn't create it
    # Imported here: starting Spark takes seconds and needs Java, which no other command does.
    from flowcast.layout import Paths
    from flowcast.spark_job import kafka_records, session, start

    spark = session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        records = kafka_records(spark, args.bootstrap, args.topic, max_offsets_per_trigger=args.max_offsets)
        queries = start(spark, records, Paths(args.out), trigger_s=args.trigger, available_now=args.available_now)
        try:
            if args.available_now:
                queries.await_termination()
            else:
                print(f"streaming {args.topic} to {args.out} every {args.trigger:g} s; Ctrl-C stops", file=sys.stderr)
                spark.streams.awaitAnyTermination()  # returns or raises only if a query ends by itself (a failure)
        except KeyboardInterrupt:
            pass
        finally:
            queries.stop()
        failed = [q.name for q in queries.all() if q.exception() is not None]
        if failed:
            raise StreamFailed(f"queries failed: {', '.join(failed)} (see the log above)")
    finally:
        spark.stop()


def _check(args: argparse.Namespace) -> None:
    report = check_run(args.bootstrap, args.topic, args.out, timeout_s=args.wait)
    print("\n".join(report.lines()))
    if not report.ok:
        raise StreamFailed("the outputs don't match the replay of the job's batches")


def _query(args: argparse.Namespace) -> None:
    con = connect(args.out)
    try:
        result = con.execute(args.sql)
        writer = csv.writer(sys.stdout, lineterminator="\n")
        writer.writerow([column[0] for column in result.description or []])
        writer.writerows(result.fetchall())
    except duckdb.Error as exc:
        raise StreamFailed(f"query failed: {exc}") from exc
    finally:
        con.close()


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
        "stream": _stream,
        "check": _check,
        "query": _query,
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
    except (KafkaError, InvalidEvent, OSError, StreamFailed, CheckpointError, SinkLogError) as exc:
        print(f"flowcast {args.command}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
