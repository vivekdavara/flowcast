# Changelog

## v1.0.0 (2026-10-27)

The first release, built over five days (October 23 to 27, 2026). What a later 1.x keeps: the `flowcast` command's
subcommands, options and exit codes (0 done, 1 the work failed, 2 a usage error); the event's wire format (schema
versions 1 and 2, compact JSON in field order; a version-1 event's bytes never change, pinned by the generator's
hash test); the generator's output for a given seed and settings; a run directory's layout (an output and a
checkpoint per query, `_progress/`, `_restarts.jsonl`, `backfill/CURRENT`, `aggregates.duckdb`); the DuckDB views'
names and columns; the data-quality rules' names; and `scripts/bench.py`'s `results.json`. A new rule, view column or
report section may be added; a change to any of these is recorded here.

### The event, the generator and the truth (day 1)

- Schema version 1: 13 fields, a strict decoder (no booleans or floats as integers, no `NaN`, no duplicate or unknown
  keys, every string in its format) and a byte-stable encoder.
- A seeded generator of sessions in arrival order, with 3% late, 0.2% very late and 1% duplicate records; bounded
  memory; determinism checked on macOS and Linux by a pinned SHA-256.
- The exact minute aggregates in Python and in DuckDB SQL, equal on random streams.
- `scripts/kafka.sh`: a local single-node KRaft broker that stops only its own pid; topics never auto-created; an
  idempotent producer keyed by user id; the snapshot reader.

### The Spark job (day 2)

- A volume producer (batching, compression, pacing by arrival time, delivery accounting).
- The job: Kafka source, validation by the decoder itself in an Arrow UDF, dead letters, raw events partitioned by
  date, deduplication within the watermark, one-minute windows with a 3-minute watermark, Parquet file sinks.
- DuckDB views that read the sinks' logs, never a glob; `flowcast check`, the exact replay of the job's own batches
  (checkpoint offsets and watermarks) against the truth of the records it had to keep.

### Exactly once, late data, schema version 2, the backfill (day 3)

- Exactly once across crashes, proved by tests that put a run into each state a crash leaves and by real SIGKILLs of
  the running job (`scripts/kill_restart.py`); the idempotent DuckDB sink (rows and batch id in one transaction, bound
  to its query's id); the start log that names re-run batches the file sink skipped.
- Late-data accounting: Spark's dropped and duplicate counts checked against the replay, batch by batch.
- Schema version 2 (an optional `campaign`), read alongside version 1.
- `flowcast backfill`: the stream's own transformations as a batch over the raw events, published by renaming a
  pointer, held to the truth of every raw record.

### Measurement and data quality (day 4)

- `flowcast latency`: produced, event time and window end to queryable, from the sink logs' commit times; throughput
  from the progress reports.
- `scripts/bench.py`: batch sizes over a backlog, trigger intervals on a live stream.
- `batching.py`: Spark's batch split and watermarks without Spark, which explains why small batches over a sparse
  backlog drop on-time records; a Spark test holds the job to it.
- `flowcast quality`: data-quality rules as SQL over the views.
- CI split into Spark and non-Spark jobs.

### The report and the release (day 5)

- `flowcast report`: a static, self-contained HTML page of a run (windows per minute with the late records a backfill
  recovers, events by type, latency and throughput, every quality rule, the exact check with `--topic`, benchmark
  results with `--bench`). Samples in `docs/`.
- Quality rules for the decoder's string formats (the day-4 review's open finding), held to the decoder by a property
  test: a raw event passes every row rule exactly when `decode` accepts it.
- The DuckDB views have every column of their table even when no file has it: a run whose raw files all predate
  schema version 2 (day 2's) had no `campaign` column, and `quality` failed on it with a binder error.
- `tests/test_docs.py`: every `flowcast` command the documents show parses, every subcommand and script they name
  exists, and the version is the same in pyproject, the package and this file.
- Tests from a mutation pass over days 4 and 5 (percentile ranks, sink-log deletes, the report's ordering and tiles).
- Fixes from the day-5 review of the report: NULL keys and moments the rules flag no longer crash it (nor `latency`),
  a window off the minute grid is drawn, bad results files are refused with a reason (bench numbers within 1e15),
  money is exact past 2^53 cents, minutes where the backfill is below the stream are called out; tests for the
  review's surviving mutants.
- Measured: the trigger interval on a live stream (1, 5 and 10 s, windows closing), throughput over a dense backlog
  (2,000 to 50,000 records a batch), and the parse UDF's cost per record (`scripts/measure_parse.py`); the README in
  release form, with the results at a glance.
