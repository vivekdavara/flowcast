"""The `flowcast` command line.

Exit codes: 0 done, 1 the work failed (Kafka, a bad input file), 2 a usage error (arguments or generator settings).
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import sys
import time
from collections.abc import Iterable, Iterator, Sequence
from datetime import UTC, datetime
from itertools import islice
from pathlib import Path

import duckdb

from flowcast import __version__, report
from flowcast.check import CheckpointError, check_run
from flowcast.generator import Config, Emitted, Generator
from flowcast.kafka_io import (
    COMPRESSION,
    DEFAULT_BOOTSTRAP,
    MAX_PARTITIONS,
    PARTITIONS,
    TOPIC,
    KafkaError,
    ProducerSettings,
    ensure_topic,
    produce,
    topic_counts,
    wait_for_broker,
)
from flowcast.latency import latencies, throughput
from flowcast.layout import LayoutError
from flowcast.pacing import Pacer
from flowcast.quality import MAX_DEAD_LETTER_RATE, check_quality
from flowcast.restarts import DUCKDB_QUERY, FILE_SINK_QUERIES
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


def _start_ms(text: str) -> int:
    """Epoch milliseconds, or "now". With `produce --speed 1 --start-ms now` each event's time is about when it is
    sent (less its network delay and any lateness), so event time to queryable is a real latency (day 4)."""
    if text == "now":
        return time.time_ns() // 1_000_000
    try:
        return int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not epoch milliseconds or 'now': {text!r}") from None


class _StartMs(argparse.Action):
    """--start-ms, remembering whether it was "now" (`start_now`): `produce --speed` then paces against the wall clock
    from that moment, so a late start can't shift the event times."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: str | Sequence[object] | None,
        option_string: str | None = None,
    ) -> None:
        text = str(values)
        try:
            namespace.start_ms = _start_ms(text)
        except argparse.ArgumentTypeError as exc:
            parser.error(f"argument --start-ms: {exc}")
        namespace.start_now = text == "now"


def _stream_options(parser: argparse.ArgumentParser, *, from_file: bool = False) -> None:
    size = parser.add_mutually_exclusive_group(required=True)
    size.add_argument("--sessions", type=_positive, help="every record of this many whole sessions")
    size.add_argument("--records", type=_positive, help="the first this-many records of the endless stream")
    if from_file:
        size.add_argument("--input", type=Path, help="a file of JSON lines instead (each line checked)")
    parser.add_argument("--seed", type=int, default=Config.seed, help="generator seed (default %(default)s)")
    parser.set_defaults(start_now=False)
    parser.add_argument(
        "--start-ms",
        action=_StartMs,
        default=Config.start_ms,
        help="when the first session starts, epoch ms, or 'now' for the wall clock (default 2026-01-01T00:00:00Z)",
    )
    parser.add_argument(
        "--sessions-per-second",
        type=float,
        default=Config.sessions_per_second,
        help="sessions started per second of event time (default %(default)s)",
    )
    parser.add_argument("--duplicate-fraction", type=float, default=Config.duplicate_fraction)
    parser.add_argument("--late-fraction", type=float, default=Config.late_fraction)
    parser.add_argument("--very-late-fraction", type=float, default=Config.very_late_fraction)
    parser.add_argument(
        "--schema-version", type=int, choices=(1, 2), default=1, help="2 adds a campaign per session (default 1)"
    )


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
    send.add_argument(
        "--speed", type=float, help="pace by arrival time: 1 is real time, 10 ten times faster (default: no pacing)"
    )
    send.add_argument("--linger-ms", type=float, default=ProducerSettings.linger_ms, help="default %(default)s")
    send.add_argument("--batch-bytes", type=int, default=ProducerSettings.batch_bytes, help="default %(default)s")
    send.add_argument("--compression", choices=COMPRESSION, default=ProducerSettings.compression)

    count = sub.add_parser("count", help="records in each partition of a topic")
    _kafka_options(count)

    stream = sub.add_parser("stream", help="run the Spark job from the topic to Parquet under --out (Ctrl-C stops it)")
    _kafka_options(stream)
    stream.add_argument("--out", type=Path, default=DEFAULT_OUT, help="output directory (default %(default)s)")
    stream.add_argument("--trigger", type=_seconds, default=5.0, help="seconds between batches (default %(default)s)")
    stream.add_argument("--max-offsets", type=_positive, help="at most this many records a batch")
    stream.add_argument("--available-now", action="store_true", help="read what the topic holds now, then stop")
    stream.add_argument(
        "--duckdb", action="store_true", help="also write the minute aggregates to the idempotent DuckDB sink"
    )

    check = sub.add_parser("check", help="check a stopped run's outputs exactly against the records in the topic")
    _kafka_options(check)
    check.add_argument("--out", type=Path, default=DEFAULT_OUT, help="the run's directory (default %(default)s)")

    backfill = sub.add_parser(
        "backfill", help="recompute a run's minute aggregates from all its raw events, as a batch, and publish them"
    )
    backfill.add_argument("--out", type=Path, default=DEFAULT_OUT, help="the run's directory (default %(default)s)")

    latency = sub.add_parser(
        "latency", help="a stopped run's latencies (produced, event time, window end to queryable) and throughput"
    )
    latency.add_argument("--out", type=Path, default=DEFAULT_OUT, help="the run's directory (default %(default)s)")

    quality = sub.add_parser("quality", help="check a run's outputs against the data-quality rules (no Kafka needed)")
    quality.add_argument("--out", type=Path, default=DEFAULT_OUT, help="the run's directory (default %(default)s)")
    quality.add_argument(
        "--max-dead-letter-rate",
        type=float,
        default=MAX_DEAD_LETTER_RATE,
        help="the largest share of records that may be dead letters (default %(default)s)",
    )

    page = sub.add_parser("report", help="write a static HTML report of a run (no Kafka needed without --topic)")
    page.add_argument("--out", type=Path, default=DEFAULT_OUT, help="the run's directory (default %(default)s)")
    page.add_argument("--html", type=Path, help="the page to write (default: report.html in the run's directory)")
    page.add_argument("--topic", help="also run the exact check against this topic (needs the broker)")
    page.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help="for --topic (default %(default)s)")
    page.add_argument("--wait", type=_seconds, default=30.0, help="for --topic (default %(default)s)")
    page.add_argument("--bench", type=Path, help="a scripts/bench.py results.json to add")

    query = sub.add_parser("query", help="run SQL over a run's DuckDB views and print CSV")
    query.add_argument("sql", help="e.g. 'SELECT * FROM minute_aggregates ORDER BY window_start LIMIT 10'")
    query.add_argument("--out", type=Path, default=DEFAULT_OUT, help="the run's directory (default %(default)s)")
    return parser


def _config(args: argparse.Namespace) -> Config:
    try:
        return Config(
            seed=args.seed,
            start_ms=args.start_ms,
            sessions_per_second=args.sessions_per_second,
            duplicate_fraction=args.duplicate_fraction,
            late_fraction=args.late_fraction,
            very_late_fraction=args.very_late_fraction,
            schema_version=args.schema_version,
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
    try:
        settings = ProducerSettings(args.linger_ms, args.batch_bytes, args.compression)
        anchor = args.start_ms if args.start_now else None
        pacer = None if args.speed is None else Pacer(_records(args), args.speed, anchor_ms=anchor)
    except ValueError as exc:
        raise UsageError(str(exc)) from exc
    events: Iterable[Event] = pacer if pacer is not None else (record.event for record in _records(args))
    wait_for_broker(args.bootstrap, timeout_s=args.wait)
    produced = produce(args.bootstrap, args.topic, events, settings=settings)
    print(
        f"produced {produced.records} records ({produced.bytes:,} bytes) to {args.topic} in {produced.seconds:.2f} s: "
        f"{produced.records_per_s:,.0f} records/s"
    )
    print("acknowledged by partition: " + ", ".join(f"{p}: {n}" for p, n in produced.partitions.items()))
    if pacer is not None:
        print(f"paced at {args.speed:g}x arrival time; fell behind by at most {pacer.max_lag_s:.3f} s")


def _stream(args: argparse.Namespace) -> None:
    if args.max_offsets == 0:
        raise UsageError("--max-offsets must be at least 1")
    wait_for_broker(args.bootstrap, timeout_s=args.wait)
    topic_counts(args.bootstrap, args.topic)  # the topic must exist: the job doesn't create it
    # Imported here: starting Spark takes seconds and needs Java, which no other command does.
    from pyspark.errors import StreamingQueryException

    from flowcast.layout import Paths
    from flowcast.spark_job import kafka_records, session, start

    spark = session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        records = kafka_records(spark, args.bootstrap, args.topic, max_offsets_per_trigger=args.max_offsets)
        queries = start(
            spark,
            records,
            Paths(args.out),
            trigger_s=args.trigger,
            available_now=args.available_now,
            duckdb=args.duckdb,
        )
        try:
            if args.available_now:
                queries.await_termination()
            else:
                print(f"streaming {args.topic} to {args.out} every {args.trigger:g} s; Ctrl-C stops", file=sys.stderr)
                spark.streams.awaitAnyTermination()  # returns or raises only if a query ends by itself (a failure)
        except KeyboardInterrupt:
            pass
        except StreamingQueryException:
            pass  # reported below, with every query that failed, once the others are stopped
        finally:
            queries.stop()
            queries.save_progress(Paths(args.out))
        failures = [(q.name, q.exception()) for q in queries.all() if q.exception() is not None]
        if failures:
            first = str(failures[0][1]).strip().splitlines()[0][:300]
            names = ", ".join(name for name, _ in failures)
            raise StreamFailed(f"queries failed: {names}; the first: {first} (Spark's log above has the rest)")
    finally:
        spark.stop()


def _check(args: argparse.Namespace) -> None:
    report = check_run(args.bootstrap, args.topic, args.out, timeout_s=args.wait)
    print("\n".join(report.lines()))
    if not report.ok:
        raise StreamFailed("the outputs don't match the replay of the job's batches")


def _latency(args: argparse.Namespace) -> None:
    print("\n".join(latencies(args.out).lines()))
    print("throughput:")
    for query in (*FILE_SINK_QUERIES, DUCKDB_QUERY):
        rate = throughput(args.out, query)
        if rate is not None:
            print(f"  {rate.line()}")


def _quality(args: argparse.Namespace) -> None:
    # Checked here, not by catching ValueError from the rules: a torn JSON line is one too (day-4 review).
    if not 0 <= args.max_dead_letter_rate <= 1:
        raise UsageError("--max-dead-letter-rate must be between 0 and 1")
    report = check_quality(args.out, args.max_dead_letter_rate)
    print("\n".join(report.lines()))
    if not report.ok:
        broken = sum(not r.ok for r in report.results) + (report.dead_letter_rate > report.max_dead_letter_rate)
        raise StreamFailed(f"the outputs break {broken} data-quality rule{'' if broken == 1 else 's'}")


def _backfill(args: argparse.Namespace) -> None:
    # Imported here, as for stream: Spark takes seconds to start and needs Java.
    from flowcast.backfill import BackfillError, run
    from flowcast.layout import Paths
    from flowcast.spark_job import session

    spark = session("flowcast-backfill", kafka=False)
    spark.sparkContext.setLogLevel("WARN")
    try:
        written = run(spark, Paths(args.out))
    except BackfillError as exc:
        raise StreamFailed(str(exc)) from exc
    finally:
        spark.stop()
    con = connect(args.out)
    try:
        rows = con.execute("SELECT count(*), coalesce(sum(events), 0) FROM backfill_aggregates").fetchone()
        # Where the stream wrote a window, it can only have fewer events: the late records it dropped.
        fewer = con.execute(
            "SELECT count(*), coalesce(sum(b.events - s.events), 0) FROM minute_aggregates s "
            "JOIN backfill_aggregates b USING (window_start, event_type) WHERE b.events <> s.events"
        ).fetchone()
        streamed = con.execute("SELECT count(*) FROM minute_aggregates").fetchone()
    finally:
        con.close()
    assert rows is not None and fewer is not None and streamed is not None
    print(f"backfill: {rows[0]} rows, {rows[1]} events, published as {written}")
    print(
        f"of the {streamed[0]} rows the stream wrote, {fewer[0]} have fewer events than the backfill "
        f"({fewer[1]} events in all: late records the stream dropped)"
    )


def _report(args: argparse.Namespace) -> None:
    if not args.out.is_dir():
        raise UsageError(f"{args.out}: no run directory there")
    check = check_run(args.bootstrap, args.topic, args.out, timeout_s=args.wait) if args.topic else None
    generated = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    try:
        data = report.collect(args.out, generated, check=check, bench=args.bench)
    except report.BenchError as exc:
        raise StreamFailed(str(exc)) from exc
    path = args.html or args.out / "report.html"
    report.write(path, data)
    verdicts = [f"quality {'ok' if data.quality.ok else 'FAILED'}"]
    if check is not None:
        verdicts.append(f"check {'ok' if check.ok else 'MISMATCH'}")
    print(f"wrote {path} ({', '.join(verdicts)})")


def _query(args: argparse.Namespace) -> None:
    try:
        con = connect(args.out)
        try:
            result = con.execute(args.sql)
            columns = [column[0] for column in result.description or []]
            rows = result.fetchall()
        finally:
            con.close()
    except duckdb.Error as exc:
        raise StreamFailed(f"query failed: {exc}") from exc
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(columns)
    writer.writerows(rows)


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
        "quality": _quality,
        "latency": _latency,
        "backfill": _backfill,
        "report": _report,
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
    except (
        KafkaError,
        InvalidEvent,
        OSError,
        StreamFailed,
        CheckpointError,
        SinkLogError,
        LayoutError,
        duckdb.Error,
    ) as exc:
        print(f"flowcast {args.command}: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
