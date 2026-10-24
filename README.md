# FlowCast

A streaming analytics pipeline built end to end on one laptop: a synthetic clickstream goes through Kafka (KRaft
mode) and a Spark Structured Streaming job into Parquet files that DuckDB queries. The question it answers is per
minute of event time, how many events of each type happened and how much revenue purchases brought in; the questions
behind it are how fresh that answer is and how right it is when events arrive late, twice, or out of order.

The data is synthetic on purpose: the generator made every event, so the exact answer is known and every streaming
result can be checked against it.

## Status

| Day | What | State |
|---|---|---|
| 1 | Design, event schema, generator, ground truth, local Kafka, round trip, CI | done |
| 2 | Producer at volume; the Spark job (validate, dead letters, raw events, windowed aggregates); DuckDB views; the exact check | done |
| 3 | Exactly-once across restarts; late data; schema version 2; batch backfill | |
| 4 | Benchmarks (throughput, end-to-end latency, trigger intervals); data-quality checks | |
| 5 | Static HTML report, results, walkthrough, v1.0.0 | |

[DESIGN.md](DESIGN.md) is the specification for all five days; [WALKTHROUGH.md](WALKTHROUGH.md) has the questions an
interviewer is likely to ask, with answers grounded in the code.

## Architecture

```mermaid
flowchart LR
  G["generator<br/>seeded, arrival order"] --> P["producer<br/>idempotent, key = user_id"]
  G -.-> T["ground truth<br/>Python + DuckDB SQL"]
  P --> K[("Kafka: clickstream<br/>6 partitions, KRaft")]
  K --> S["Spark Structured Streaming<br/>3 queries, one checkpoint each"]
  S --> PQ["Parquet<br/>raw, aggregates, dead letters"]
  PQ --> D["DuckDB views<br/>(from the sinks' logs)"]
  S -.->|"checkpoints: offsets + watermark per batch"| C["flowcast check<br/>replay vs truth"]
  T -.-> C
  D -.-> C
```

Built so far (days 1 and 2): the whole pipeline, and an exact check of its output. Day 3 adds exactly-once across a
killed job, schema version 2 and the batch backfill.

## Layout

```
src/flowcast/
  schema.py               the event (version 1): fields, strict decoder, byte-stable encoder
  generator.py            sessions, arrival order, late records, duplicates, determinism
  truth.py                exact minute aggregates in Python and DuckDB
  sql/minute_aggregates.sql
  kafka_io.py             topic creation, idempotent producer (settings, accounting), snapshot reader, counts
  pacing.py               replays arrival times against the wall clock
  spark_job.py            the job: parse (decode in a UDF), dead letters, raw events, dedupe, windows, start
  layout.py               where a run writes (an output and a checkpoint per query)
  check.py                the replay of a run's batches over Kafka, and the report
  views.py                DuckDB views from the file sinks' logs
  cli.py                  flowcast generate | truth | topic | produce | count | stream | check | query | version
scripts/kafka.sh          local single-node Kafka (start/stop/status/bootstrap/logs/reset)
scripts/measure_generator.py, scripts/measure_encode.py
tests/                    unit, property, differential, Kafka and Spark tests
```

## How to run

Needs Python 3.11 and, for Kafka and Spark, Homebrew's `kafka` (4.3.1) and `openjdk@21` (the Makefile points
`JAVA_HOME` at it). PySpark 4.2.0 comes with `make venv`; its Kafka source jars are fetched from Maven Central on first
use (into `~/.ivy2.5.2`).

```bash
make venv
make kafka-start
make check
```

`make check` runs ruff, ruff format, mypy --strict and the tests, Kafka and Spark ones included (they use their own
topics and delete them; about two minutes). `make kafka-stop` stops the broker; `scripts/kafka.sh reset` deletes its data.

Try the pieces:

```bash
.venv/bin/flowcast generate --sessions 2
.venv/bin/flowcast truth --sessions 1000 --engine duckdb
.venv/bin/flowcast topic
.venv/bin/flowcast produce --sessions 1000
.venv/bin/flowcast count
```

Run the job and check it:

```bash
.venv/bin/flowcast topic --topic demo
.venv/bin/flowcast produce --topic demo --sessions 10000
.venv/bin/flowcast stream --topic demo --out data/demo --available-now --max-offsets 2000
.venv/bin/flowcast check --topic demo --out data/demo
.venv/bin/flowcast query --out data/demo "SELECT * FROM minute_aggregates ORDER BY window_start LIMIT 10"
```

`stream` without `--available-now` runs until Ctrl-C, a batch every `--trigger` seconds (5); `produce --speed 1`
paces records by their arrival times (real time; 10 is ten times faster), and `--linger-ms`, `--batch-bytes` and
`--compression` set the producer's batching.

`generate --sessions N` writes every record of N whole sessions (very late ones included); `--records N` is the first
N records of the endless stream, which is what a collector would have received by then. Both take `--seed` and the
disorder settings (`--late-fraction`, `--very-late-fraction`, `--duplicate-fraction`).

## Results (day 1)

Synthetic data throughout.

Measured on an Apple M3 (macOS 26.6.2, Python 3.11.12), on battery, Low Power Mode off.

| What | Result | Command |
|---|---|---|
| Generator, 50,000 sessions (198,236 records) | **181,902 records/s** (1.090 s, median of 5) at `b6aefbb`; 200,425 (0.989 s) at `26b835e` | `.venv/bin/python scripts/measure_generator.py` |
| Generator + `encode` (validation and JSON) | **62,278 records/s** (3.183 s) at `b6aefbb`; 62,907 (3.151 s) at `26b835e`: encoding is about two-thirds of the time | same |
| Records the generator holds in memory | peak **1,810** pending (sampled every 1,024 records), 1,855 over the first 100,000 records of the endless stream | same; `test_memory_stays_bounded_while_streaming` |
| Records per session at the defaults | 3.9 (38,963 events in 10,000 sessions, plus 383 duplicate copies) | `.venv/bin/python -c "from flowcast.generator import sessions; c = sessions(10_000); print(len(c), sum(r.duplicate for r in c))"` |
| Determinism | the SHA-256 of the first 10,000 records is the same on macOS (here) and Linux (CI) | `test_the_first_10k_records_match_the_pinned_hash` |
| Kafka round trip | 300 sessions through a 6-partition topic: every record back byte for byte, one partition per user, each user's records in send order | `tests/test_kafka.py` |

Two runs on battery (40% and 28%): at `26b835e` with a review agent open but idle (load average 1.66 before, 2.06
after), and at `b6aefbb` right after the test suite had run (load 4.21 before, 3.85 after). Both made the same 198,236
records; the gap in generation speed isn't separated between the code (an event-time check per event was added) and
the load. These numbers size day 2's producer runs; the encode cost (the strict checks run on every record) is the
first thing day 2's volume producer will look at.

## Results (day 2)

Synthetic data. Apple M3, macOS 26.6.2, Python 3.11.12, PySpark 4.2.0 on Java 21.0.12; on AC power from 16:30 (the
battery at 22-42%, charging), with Vivek's own apps open (load average 8 to 17 during the runs).

| What | Result | Command |
|---|---|---|
| The exact check of a run | 39,346 records (10,000 sessions, default settings) read in **21 batches**: 39,291 kept, **55 dropped as late**, 0 invalid; **48 aggregate rows written, 48 expected: equal**; every batch's watermark as recomputed from the records; 39,346 raw events for 39,346 valid records read | the "Run the job and check it" commands above, at `f26b685`, topic `demo-d2`, `--max-offsets 2000` |
| That run's length | 46 s for `stream --available-now` (Spark's start included; three queries) | same |
| `encode` | **277,755 events/s**, from 101,924 at `e2f0a30` (2.8x; `dataclasses.astuple` was 180,914/s alone); same bytes | `.venv/bin/python scripts/measure_encode.py` (79,390 events, best of 7 interleaved rounds); on battery with a review agent running: 100,530 to 278,420 |
| `produce`, unpaced | 39,346 records (11.4 MB) in 0.56 s: **70,670 records/s**, generating and encoding included; acknowledgements by partition match the topic | `.venv/bin/flowcast produce --topic demo-d2 --sessions 10000` (one run) |
| `produce --speed 50` | 2,000 sessions (8,029 records) in 36.75 s, at most 6 ms behind schedule: the very late tail (up to 30 minutes of arrival time) sets the length | `.venv/bin/flowcast produce --sessions 2000 --speed 50` (one run, on battery) |

What these aren't: day 4's benchmarks. The job's throughput, the cost of the parse UDF and end-to-end latency are
measured there, on an idle machine.

## Tests

- `tests/test_schema.py`: every rule of the field table refuses a bad value with its reason; JSON edge cases
  (duplicate keys, `NaN`, booleans and floats for integers, deep nesting); a Hypothesis round trip byte for byte.
- `tests/test_generator.py`: the pinned SHA-256 of the first 10,000 records; arrival order; delays matching their
  labels; duplicates identical and 0.1 to 5 s after their original; the disorder rates; the session rules (rebuilt
  from the events); bounded memory; a finite stream equal to the endless stream's records of the same sessions; a
  Hypothesis test of the invariants over random settings.
- `tests/test_truth.py`: hand-made windows with a duplicate and window edges; Python and DuckDB equal at the defaults
  and over random streams (Hypothesis); 9.9e12 cents summed exactly.
- `tests/test_kafka.py` (needs a broker): the round trip through a 6-partition topic (every value byte for byte, one
  partition per user, each user's records in send order); topic creation idempotent and partition counts checked; a
  produce to a missing topic fails rather than creating it.
- `tests/test_kafka_script.py` (`FLOWCAST_TEST_KAFKA_SCRIPT=1`, CI's kafka-script job): the script with a broker of its
  own: start twice, a round trip, data kept across a restart, reset; a busy port and a foreign directory refused.
- `tests/test_cli.py`: each command's output and exit codes.

- `tests/test_kafka_config.py`: the producer stays idempotent with `acks=all` (a fake producer, no broker); waits that
  could never end are refused.
- `tests/review/` and `tests/review2/`: the two day-1 reviews' findings as tests, each failing at the commit it
  reviewed (`943596b`, `a403362`).
- `tests/test_spark_rules.py` (Spark): the late-row rules on the job's own functions, one file per batch: the previous
  batch's watermark, inclusive, for the dedupe and the windows; the dedupe's state is evicted; revenue only for
  purchases.
- `tests/test_spark_parse.py` (Spark): `parse` gives `decode`'s verdict and reason on 170 records (150 generated, 20
  bad); the dead letter and raw event split, columns and dates.
- `tests/test_spark_stream.py` (Kafka and Spark): 300 thin sessions (20% late, 5% very late, 5% duplicates) and four bad
  records through two runs of the job at 150 records a batch; the aggregates must equal the replay row for row, with
  watermarks as specified and more than 10 records dropped; raw events and dead letters reconcile; the DuckDB views
  match; the truth's SQL over the raw events equals the Python truth. Then the CLI: `stream`, `check`, `query` as
  subprocesses, and a forged aggregate row that `check` must reject.
- `tests/test_check.py`: the replay and `check_run` on hand-made checkpoints and run directories (no Spark, no broker).
- `tests/test_views.py`: reading the sink log (compaction, deletes, gaps, strangers, URIs) and the views.
- `tests/test_pacing.py`: the pacer on a fake clock.
- `tests/review_d2/`: the day-2 review's findings, each failing at `0eb546a`.

CI (`.github/workflows/ci.yml`): lint, types and every test (Spark on Temurin 21, its jars cached) against an
`apache/kafka:4.3.1` service container; and
`scripts/kafka.sh` against the Apache tarball (SHA-256 checked) on Java 21.

## Review of day 1

One review agent read the day's code at `943596b` (16:04 to 16:26) with a correctness brief, ran its own broker on
other ports, and wrote each defect it reproduced as a failing test (10 tests in 6 files; one, the `product_id`
length, was settled by changing DESIGN's table instead, so `tests/review/` keeps the other 9, in 5 files). All fixed:

- **Python and DuckDB truths disagreed** when copies of an `event_id` differ: the SQL's `DISTINCT ON` had no order,
  so with 200,000 ids sent twice DuckDB counted the later copies. Both now keep the first copy in input order.
- **`ensure_topic` lost create races** (the broker's metadata lagged the controller: "has None partitions", 11 of 15
  races of 6 threads), and **a topic just created couldn't always be read** ("Not leader for partition", 7 of 60).
  `ensure_topic` now returns only once every partition has a leader; reads retry those settling errors.
- **`kafka.sh` trusted any live pid** in its pid file, so after a crash or reboot `stop` could kill an unrelated
  process that had been given the same pid. The pid's command line must now name this directory's config.
- **`max_session_events` wasn't a cap** (a purchase writes one event per cart line); **a start near 2100 made invalid
  events**; **`--wait nan` waited forever**; **a huge `--partitions` was a traceback**.

Of its 12 surviving mutants, 10 are caught now, each run again here as a mutant against the fixed code (bare `u`/`s`/`p` ids, a 33-character category, reading and counting
from a low watermark above 0, the producer's idempotence, the broker refusing auto-creation, the session cap, the
quantity cap, the closed-pipe exit code, `--partitions 0`); two are still open: the snapshot reader's skip of records
written after it started, and `delete_topic` swallowing errors. The test that a purchase empties the cart is new too:
the review's mutant that bought only the first line was caught only by the slow property test, and now fails the
10,000-session rules test as well. Its doubt about DESIGN's watermark accounting matched a correction already made from Spark's
source (`26b835e`).

A second agent then read only the fixes (`943596b..a403362`, 16:39 to 16:48) and found two regressions in them, both
fixed (`tests/review2/`, 4 tests): `kafka.sh`'s new pid check compared the config path as a string, so the same
directory spelled another way (a symlink; `/tmp` is `/private/tmp` on macOS; `./kafka`) made `stop` delete the pid
file of the live broker and leave it running unfound; the script now resolves the directory to its physical path
first, and keeps a pid file it can't verify. And `produce`'s new flush after an error was one long call into
librdkafka, which holds off Python's signal handling, so a second Ctrl-C couldn't end it; it flushes in 0.1 s slices
now. From its suspicions: every Kafka call refuses NaN or infinite timeouts, `ensure_topic`'s calls share one
deadline, and a prefix that wasn't all delivered says so on the error. No third review this time.

## Review of day 2

One review agent read the day's code at `0eb546a` (15:34 to 15:47, its own worktree and broker on other ports, a
correctness brief) and wrote four defects as failing tests (`tests/review_d2/`, 5 tests in 3 files); each failed here
before its fix and passes after:

- **One bad record could stop the job.** `decode`'s reason can quote a key; a JSON escape can make that key a lone
  surrogate (`{"\ud800":1,"\ud800":2}`), which Arrow can't encode, so the UDF threw, the task failed, and the query
  with it. Reasons are escaped to valid UTF-8 now (`29332c3`).
- `flowcast stream` gave a traceback when a query failed (its own report was unreachable), and DuckDB errors escaped
  `query` and `check` (`84b19c0`). The stream test needed a new trigger once the first fix landed: records deleted from
  Kafka before the job read them, which `failOnDataLoss` makes a failure.
- `raw_events` took columns from every `key=value` segment of the run directory's path (`83c8007`).

It ran 38 mutants and listed 16 as surviving (or not run against every test). 14 are killed now, each re-run here
as a mutant against the new tests: 7 in check.py and 5 in views.py (a throwaway script, not committed, applied each mutant, ran the tests and
restored the file from a copy), and in
spark_job.py `_revenue`'s `lit(1)` and `dropDuplicates` for `dropDuplicatesWithinWatermark` (against
`test_spark_rules.py`). The latter gives the same output with state that never shrinks, so a test checks the state
size (`f26b685`). The most important one, **a job run with a 2-minute watermark passed every test**: the replay now
recomputes every batch's watermark from the records and the report fails on a difference (`52f2023`), unit-tested
with 2- and 4-minute checkpoints and required by the end-to-end test; the job mutant itself wasn't re-run. The 16th,
`hive=False`, no longer exists after `83c8007`. Two of my own new mutants survived at first: one was an equivalent
`max()` (removed), the other got a test. Its suspicions, noted in the limitations: copies of an event_id that differ
(outside the spec) could make `check` report a correct job as wrong; `check` on a running job can race a commit.
It also said that its `pkill -f` had killed a process of my test run (see `b5b6b09` and `29332c3`'s messages).

## Design decisions

- **Integer cents and integer milliseconds**, so Python, DuckDB and Spark compute identical answers, compared with `==`.
- **A strict decoder** (no booleans or floats as integers, no `NaN`, no duplicate or unknown keys) that is the
  reference for day 2's dead letters.
- **Every draw from `random()` alone**, the one `random` method Python keeps stable for a seed; event ids hash (seed,
  sequence). Determinism is checked, not assumed: the pinned hash passes on macOS and Linux.
- **Disorder on purpose**: 3% late (at most about 2 minutes, inside the 3-minute watermark), 0.2% very late (5 to
  30 minutes, past it), 1% sent twice. Each exercises a different part of the streaming job.
- **Key = `user_id`**: a user's events stay in one partition, in order. **Idempotent producer**: the producer's own
  retries can't duplicate or reorder; the generator's duplicates stay, and the pipeline must remove them.
- **Topics are never auto-created**, and an existing topic with the wrong partition count is refused.
- **Validation is `decode` itself**, in an Arrow UDF: `from_json` keeps the last of two duplicate keys, ignores
  unknown keys and reads `NaN`, so it can't give the same verdicts. One definition of valid, at a Python call per record.
- **The check replays the job's own batches** (checkpoint offsets and watermarks) instead of trusting a metric: the
  stream's output must equal the truth of the records it had to keep, exactly.
- **Views read the sinks' logs**, never a glob: files of a failed batch are never seen.
- **Spark on loopback**, UTC, 6 state partitions, standard Parquet timestamps.
- **`kafka.sh` stops only its own pid** (`kafka-server-stop` would signal every broker on the machine), binds to
  loopback, and deletes only a directory it made.

## Limitations (day 2)

- Three queries each read Kafka and run the parse UDF: three times the reading and parsing of one query with a shared
  source (day 4 measures the cost; `foreachBatch` into three sinks would trade it for doing exactly-once by hand).
- Exactly-once across a killed job isn't tested yet (day 3), and `flowcast check` must run on a stopped job: on a live
  one a batch can commit between its reads. It compares raw events and dead letters by count, not record by record.
- The check assumes copies of an event_id are identical, as the generator makes them; copies that differ (outside the
  spec, though `decode` accepts them) could make it report a correct job as wrong.
- Event-time skew between devices isn't modelled: each event's time is its true time, and lateness is all in transit.
- One broker, replication factor 1, Spark in local mode: a laptop pipeline, not a fault-tolerant cluster.
- `read_topic` gives up on any consumer error rather than retrying transient ones, and can't finish a compacted or
  transactional topic (it reads to the high-water mark). Untested still: its skip of records written after the
  snapshot, and `delete_topic` swallowing errors.

## License

MIT
