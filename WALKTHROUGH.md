# FlowCast walkthrough

The questions an interviewer is likely to ask about FlowCast, with short answers grounded in the code. Day 1 built
the left half of the pipeline (event schema, generator, ground truth, local Kafka, the round trip, CI); later days
add their own sections.

Reading order for day 1: `DESIGN.md` → `src/flowcast/schema.py` → `src/flowcast/generator.py` (`Generator.__iter__`,
`_session`, `_emit`) → `src/flowcast/truth.py` and `src/flowcast/sql/minute_aggregates.sql` →
`src/flowcast/kafka_io.py` (`produce`, `read_topic`) → `scripts/kafka.sh` → `.github/workflows/ci.yml`.

## The project

**What is FlowCast, in a minute?**
A streaming analytics pipeline that runs on a laptop: a synthetic clickstream goes into Kafka, a Spark Structured
Streaming job turns it into per-minute aggregates (events by type, purchase revenue) in Parquet, and DuckDB queries
them. The point isn't the dashboard; it's being able to say how fresh and how right the numbers are when events
arrive late, twice, or out of order, and to prove it against a known answer.

**Why generate data instead of using a real clickstream?**
Because the generator is also the oracle. It made every event, so it knows the exact aggregates (`truth.py`), and it
makes the hard cases happen on purpose at known rates: 3% late, 0.2% very late (past any watermark), 1% duplicates.
With a real dataset I couldn't say which numbers are right. It's labelled synthetic everywhere it's used.

## The event schema (day 1)

**Why integer cents and integer milliseconds?**
So every engine computes the same exact answer. Revenue is `Σ price_cents × quantity`: an integer in Python, BIGINT
in DuckDB, LONG in Spark, so the tests compare with `==`, never a tolerance. Milliseconds since the epoch avoid
time-zone and parsing ambiguity; Spark converts with `timestamp_millis`.

**Why is the decoder so strict?**
It's the reference for what becomes a dead letter in Spark on day 2, so it has to say exactly what "valid" means.
Things Python's `json` accepts by default but the schema doesn't: `true` for an integer (`bool` is a subclass of
`int`, so `_is_int` excludes it), `1.0` for an integer, `NaN`/`Infinity` (not JSON; `parse_constant` refuses them),
a key given twice (`json.loads` silently keeps the last; `_no_duplicate_keys` refuses it), Unicode digits in ids
(`\d` matches `٤`, so the patterns say `[0-9]`). Unknown keys are refused in version 1; day 3's version 2 adds an
optional field and readers that accept both.

**Why always write every key, in a fixed order?**
So an event is always the same bytes (`encode`). That makes the pinned hash possible, lets tests compare Kafka
records byte for byte, and makes a duplicate exactly identical to its original.

## The generator (day 1)

**How does it produce events in arrival order without holding the whole stream?**
Sessions are generated in start-time order; their events go into a heap keyed by (arrival time, push counter).
Before generating the session that starts at `S`, it pops every record that arrives by `S`. That's safe because any
event of this session or a later one happens at or after `S` and arrives at least 5 ms (the minimum network delay)
after it happens, so nothing generated later can arrive at or before `S`. The heap holds only records still in
flight: at the defaults the peak was 1,855 over 100,000 records (`test_memory_stays_bounded_while_streaming`).

**How is it deterministic, and how do you know?**
Python only promises that `random.Random(seed).random()` keeps its sequence across versions; `choice`, `randrange`
and `expovariate` may change. So `_Draw` builds every draw (integers, exponentials, weighted choices) from `random()`
alone, and event ids are a BLAKE2b hash of (seed, event number), not random bits. The remaining risk is `math.log`,
which C libraries needn't round identically, so I don't assume: a test pins the SHA-256 of the first 10,000 records,
and it passes on macOS and on Linux in CI.

**What are the late, very late and duplicate records for?**
Each one exercises a different part of the streaming job. Late records (at most about 2 minutes) must still land in the
right window: that's the watermark's job. Very late (5 to 30 minutes) records arrive after their window has closed:
the streaming aggregate must drop them, and only the batch backfill (day 3) counts them. Duplicates are the same bytes
sent again 0.1 to 5 s later, like a client that retried after a lost acknowledgement: the idempotent producer can't
remove them (to Kafka they're separate sends), so the job has to dedupe by `event_id`.

**Tell me about a mistake you made.**
My first session-rule and rate tests ran on a prefix of the endless stream, `take(50_000)`, and failed: the very-late
rate came out at 0.01% instead of 0.2%, and some sessions seemed to start with a `product_view`. The generator was
right; the test was wrong. A prefix in arrival order is what a collector has received so far, and very late records
from its last 30 minutes haven't arrived yet; a session's landing page view can itself be late. The fix was a finite
mode, `Generator(config, sessions=n)`, that makes n whole sessions and drains the heap, plus a test that it equals the
endless stream's records of those sessions, in the same order. The lesson carries into day 2: a streaming test has to
say which records it expects to have arrived.

## The ground truth (day 1)

**What exactly is the truth, and why have two implementations?**
Per one-minute tumbling window of event time (aligned to the epoch, like Spark's `window()`) and per event type, the
number of events and the purchase revenue, counting each `event_id` once. It's in Python (`minute_aggregates`) and in
SQL (`sql/minute_aggregates.sql`, run by DuckDB over the events' wire form). A Hypothesis test requires them to agree
on random streams. The SQL is the definition DuckDB will use over Spark's output, so the two together pin the meaning
of "the right answer" before any Spark code exists.

**How will Spark's output be checked against it, when Spark drops late rows?**
Exactly, not with a tolerance. Which late records Spark drops depends on where micro-batch boundaries fall, so the
truth alone only bounds the output (`spark ≤ truth` per window). But each micro-batch's progress report says which
Kafka offsets it read and which watermark it used, so replaying those over the records in Kafka says exactly which
records Spark had to keep; the truth of those must equal Spark's output row for row. The batch backfill reads every
raw record, so it must equal the full truth.

**What does Spark drop, exactly?**
From Spark's source (`WatermarkSupport` in `statefulOperators.scala`): streaming dedupe drops an input row whose event
time is at or before the watermark; a windowed aggregation in append mode drops a row whose window ends at or before
it. The job dedupes first, so the dedupe's rule is the one that drops events. Day 2 confirms both with tests.

**Why not just reconcile with `numRowsDroppedByWatermark`?**
I planned to, then read where it's counted: at each operator, on that operator's input. At the window aggregation
that input is partial aggregates, one row per window and key per batch, so five late events in one window can count
as one. The dedupe's count is in input rows, duplicates included, so it's a cross-check against the replay, not the
check. That's a correction to my first design, found by reading the source before writing any Spark code.

**Why a 3-minute watermark?**
It's above the late records' cap (2 minutes plus network delay), so late records are kept by design and very late
ones (5 minutes or more) are dropped. The exception is the end of a finite stream: once the last on-time events are
in, the maximum event time stops moving, and very late records from the last 3 minutes arrive above the watermark and
are counted. The replay check handles that without special cases.

## Kafka (day 1)

**Why key by `user_id`, and what ordering does that give you?**
Kafka orders records within a partition only. Keying by user puts a user's events in one partition, in the order
they were sent, so a session is read in order by one Spark task. Across users there's no order, which is fine because
the aggregates are by event time, not arrival. The round-trip test checks both: one partition per user, each user's
records in send order, and more than one partition used.

**What does the idempotent producer guarantee, and what doesn't it?**
With `enable.idempotence` (which implies `acks=all` and at most 5 requests in flight), the broker deduplicates the
producer's own retries by sequence number, so a retried batch can't create duplicates or reorder a partition. It does
nothing about duplicates the application sends twice, such as the generator's re-sent events, and nothing across
producer restarts. `produce` also refuses to report success unless every record's delivery report came back clean.

**How does `read_topic` always end?**
It takes each partition's high-water mark first and reads to it with directly assigned partitions: no consumer
group, no committed offsets, nothing left behind on the broker. Records written after the snapshot are ignored, and
there's an overall deadline.

**Why `auto.create.topics.enable=false`?**
So a typo in a topic name is an error, not a new topic with the default partition count. The test
`test_producing_to_a_missing_topic_fails_rather_than_creating_it` checks that. `ensure_topic` also refuses an existing
topic with a different partition count, because the key-to-partition mapping depends on it.

**Why does `kafka.sh` keep a pid file instead of using `kafka-server-stop`?**
`kafka-server-stop` finds brokers with `ps ax | grep ' kafka\.Kafka '` and, with no arguments, signals every one on
the machine; its `--node-id` filter wouldn't help, since every local single-node broker here (the project's, the
script test's, a reviewer's) is node 1. The script stops only the pid it started; it also refuses a port someone else holds (by connecting, which on macOS also sees a listener on `*:port`)
and refuses to start in, or delete, a directory without its marker file.

**How does CI run Kafka?**
The test job uses `apache/kafka:4.3.1` (the same version as Homebrew's here) as a service container, configured as one
KRaft node that advertises the runner's mapped port, `localhost:9192`. There's no container health check, because a
check from inside the container can't reach the address it advertises; a step waits with the same `wait_for_broker`
the tests use. A second job runs `scripts/kafka.sh` itself against the Apache tarball (SHA-256 checked) on Java 21.

**What did the review find, and what was the worst of it?**
Nine defects, each written as a failing test first (`tests/review/`). The one I'd lead with: my "same definition in
two engines" truth wasn't. The SQL's `SELECT DISTINCT ON (event_id) *` had no `ORDER BY`, so DuckDB kept an arbitrary
copy of a duplicated id; small inputs happened to agree because DuckDB ran them single-threaded, and my differential
tests used the generator, whose duplicates are byte-identical. With 200,000 ids whose copies fall in different
windows, the engines disagreed. Now both keep the first copy in input order (`input_order`, which for Kafka records is
the offset, since a duplicate shares its original's partition). The lesson: a differential test only covers the
inputs it's given, and mine couldn't produce the case that differs.

**And the Kafka ones?**
Two timing bugs a single broker still has: after a create, the broker's metadata and partition leadership lag the
controller for a moment, so a racing creator saw "no topic" and a reader saw "Not leader for partition". The fix is to
define "created" as "served": `ensure_topic` returns once every partition has a leader. And a safety bug in the
script: a pid file outlives its process, and pids are reused, so `stop` now checks the process's command line names
this directory's config before signalling it.

**What would change in production?**
Three or more brokers with replication factor 3 and `min.insync.replicas=2`; a schema registry instead of a
version field; TLS and authentication; partitions sized to consumer parallelism and throughput rather than a laptop's
cores; and the producer embedded in the collector service rather than replaying a generator.

## The Spark job (day 2)

Reading order for day 2: DESIGN.md "Ground truth" (the drop rule and the exact check), "Spark", "DuckDB", "The
producer" → `src/flowcast/spark_job.py` (`parse`, `deduplicated`, `window_counts`, `start`) →
`tests/test_spark_rules.py` → `src/flowcast/check.py` (`committed_batches`, `replay`, `check_run`) →
`src/flowcast/views.py` (`committed_files`, `_view_sql`) → `tests/test_spark_stream.py` → `src/flowcast/pacing.py`.

**Walk me through the job.**
Three streaming queries over the Kafka topic, one per output, each with its own checkpoint (`start`). All three run the
same `parse`: each record becomes `schema.decode`'s verdict plus the event's fields and the record's coordinates.
Invalid records go to `dead_letters` (bytes, coordinates, reason); valid ones go to `raw_events`, partitioned by event
date, with no state, so nothing is ever dropped there. The third query sets a 3-minute watermark on event time, drops
duplicate `event_id`s within it, and counts one-minute windows by event type in append mode, so each window is written
once, when the watermark passes its end. All three are Parquet file sinks.

**Why validate in a Python UDF? That's slow.**
Because there should be one definition of "valid", and `from_json` isn't it. I tried it: on `{"a":1,"a":2,"z":5,
"e":NaN}` it keeps the last `a`, ignores the unknown `z`, and reads `NaN` as a number, all things `decode` refuses on
purpose. Matching `decode` in Spark SQL would mean re-implementing duplicate-key and token-level checks that
`from_json` doesn't expose. So `parse` calls `decode` in an Arrow UDF, and `test_spark_parse.py` holds it to
`decode`'s exact verdict and reason on 257 records (234 good, 84 of them version 2 since day 3; 23 bad). The cost is a Python call per record,
which day 4 measures; if it matters, the fallback is native checks for the common path with the UDF only for what they
can't decide.

**How do you know the streaming output is right?**
`check.py` replays the job's own batches. Each query's checkpoint has an offset-log entry per batch, holding the Kafka
end offsets it read up to and the watermark it ran with, and a commit once the batch's output is done. Replaying the
committed batches over the records actually in Kafka tells me which batch read each record, and so whether the stream
had to drop it. The truth of the kept records, in the windows the last watermark has passed, must equal what Spark
wrote, row for row. It also recomputes every batch's watermark from the records before it, so a job run with the wrong
delay fails (the review showed that was missing). `flowcast check` does this for any stopped run. On the demo run
(39,346 records, 21 batches) it kept 39,291, dropped 55 as late, and the 48 written rows matched exactly.

**Which watermark decides that a row is late? How did you find out?**
The previous batch's. Spark 3.4+ lets a query chain stateful operators, and to keep a downstream operator from
dropping rows an upstream one just emitted, every operator's late-event filter uses batch *i - 1*'s watermark; batch
*i*'s own watermark only decides eviction and which windows to write. I read it in `WatermarkPropagator.scala`
(`PropagateWatermarkSimulator.getInputWatermarkForLateEvents` returns `getInputWatermark(batchId - 1, ...)`), then
confirmed it with `test_spark_rules.py`: a file source with one file per batch and no-data batches off, so every
batch boundary and watermark is known. A row below batch 1's watermark but read in batch 1 is kept; a row exactly on it
read in batch 2 is dropped (so the comparison is "at or before"). Chained, the window aggregate never drops a row the
dedupe kept, so the dedupe is the one rule the replay needs.

**Why does the replay need a test that a `<` instead of `<=` fails?**
Because the end-to-end test can't see it: generated event times almost never land exactly on a watermark. That mutant
survives `test_spark_stream.py` and dies in `test_check.py`'s hand-made case. Using the current batch's watermark
instead fails the end-to-end test.

**Why do the DuckDB views read `_spark_metadata` instead of globbing the directory?**
Because a file sink writes a batch's Parquet files before it commits the batch in that log. After a failure the
directory can hold files of a batch that will be written again, and a glob would count them twice. `committed_files`
reads the log the way Spark writes it: the newest `.compact` file plus the batch files after it, applying adds and
deletes. A test puts a stray file next to a logged one and the view doesn't see it.

**What did the review find?**
Four defects, each a failing test first (`tests/review_d2/`). The worst: **one bad record could stop the whole job**.
`decode`'s reason can quote a key, a JSON escape can make that key a lone surrogate (`{"\ud800":1,"\ud800":2}`),
Arrow can't encode that as UTF-8, and the UDF's exception failed the task and the query, the opposite of what dead
letters are for. Reasons are now escaped to valid UTF-8. The others: `flowcast stream` gave a traceback on a failed
query (its own error report was unreachable), DuckDB errors escaped `main`, and `raw_events` picked up extra columns
from `key=value` segments in the run directory's path. Its surviving mutants got tests too, all re-run and killed;
the notable one, `dropDuplicates` instead of `dropDuplicatesWithinWatermark`, gives the same output with state that
never shrinks, so only a state-size check catches it.

**What's the producer's pacing for?**
Sending as fast as possible is a backlog test (70,670 records/s here, generation and encoding included). A live
stream is the generator's arrival times replayed against the wall clock (`Pacer`, with a speed factor), and that's
what end-to-end latency on day 4 needs. On the way, the first cost turned out to be `dataclasses.astuple` in `encode`,
not JSON: plain attribute reads made `encode` 2.8x faster with identical bytes, which the pinned hash enforces.

**Tell me about a mistake from day 2.**
Two. One test hung for ten minutes, and I almost blamed Spark. The cause was my test: it called
`AdminClient(...).delete_records(...)` on a temporary client, which was freed straight away, and its futures never
resolve after that. The fix was to keep the client and bound the wait. The other was a wrong commit message: a test
run failed while a review agent was killing processes, and I blamed a driver bind failure, because the tail of the
log said "bind to another address". That was only Spark's warning that it had picked the Wi-Fi address. The change
(bind the driver to loopback) was still right, for a different reason, and the next commit's message corrects the
record.

## Exactly once, late data, schema version 2 and the backfill (day 3)

Reading order for day 3: DESIGN.md "Exactly once", "The batch backfill", "Schema version 2" → `tests/test_recovery.py`
(`cut_off_last_batch`, the test) → `src/flowcast/recovery.py` (`kill_and_restart`) → `src/flowcast/restarts.py` (its
docstring is the day's finding) → `src/flowcast/check.py` (`replay`'s `unobserved`, `late_accounting`, `check_run`) →
`src/flowcast/duckdb_sink.py` → `src/flowcast/backfill.py` and `spark_job.deduplicated` → `src/flowcast/schema.py`
(`decode`, `wire_fields`).

**How does the job get exactly-once output from Kafka?**
Three pieces. The Kafka source is replayable: each batch's end offsets are written to the checkpoint's offset log
before the batch runs, so after a crash Spark runs the uncommitted batch again over exactly the same records, with the
same watermark. The state stores are versioned per batch, so the re-run starts from the state before it. And each sink
makes a second run harmless: Spark's file sink skips a batch id already in its `_spark_metadata` log, and my DuckDB
sink skips a batch id already in its table, checked in the same transaction that inserts the rows. Exactly once is
"at least once from the source, idempotent at the sink". The proof is the replay: after any crash and restart, every
output must still equal the replay of its own query's batches.

**What happens if the job is killed in the middle of a batch?**
It depends on how far the batch got. Planned but not written: the restart runs it again. Written but not logged: its
data files stay in the directory as orphans, the restart writes the batch again under new file names, and readers never
see the orphans because they read the sink's log, never a glob (the test shows a glob would count those rows twice).
Logged but not committed: the restart runs it, and the sink skips it. `test_recovery.py` puts a finished run into the
last two states by hand, and `recovery.py` kills the real process group with SIGKILL at random moments and records which
state each query was in. Both then require `flowcast check` to pass.

**Your kill test failed. What was wrong?**
One run in three, the check said a batch's watermark was lower than the records before it give, while every output
row was right. The kill had left `minute_aggregates`' batch 1 logged by the file sink but not committed. On restart,
Spark ran batch 1 again, and the file sink saw it in its log and returned without executing the batch. So batch 1's
event times never reached the watermark tracker, and batch 2 kept batch 1's watermark. I confirmed it on the failing
run: leaving batch 1, and only batch 1, out of the replay's running maximum explains every recorded watermark. The
checkpoint can't tell you a batch was skipped, so the job now writes each query's state to `_restarts.jsonl` before it
starts, and the check uses that. The DuckDB query doesn't have the problem: `foreachBatch` is always called and my
function collects the frame, which executes it.

**Why bind the DuckDB sink to a query id?**
Batch ids are only unique within one checkpoint. Delete the checkpoint, or start a second job against the same
database, and the batch ids start at 0 again. A sink that only remembered "batch 0 done" would then skip the new
query's first batches as already written, and lose them without an error. The sink records Spark's query id from the
checkpoint's `metadata` file and refuses any other query's batches. It also refuses a gap in the ids, and the table's
primary key on (window, type) refuses a second row for a window, which in append mode can only be a bug.

**Can you trust `numRowsDroppedByWatermark`?**
At the dedupe, yes, and day 3 checks it exactly. The job saves Spark's progress reports, and the check compares them
batch by batch with the replay: the dedupe's `numRowsDroppedByWatermark` with the rows the replay drops as late (every
copy), its `numDroppedDuplicateRows` with the kept records whose id was kept before, and the window aggregation's drops
must be 0. They were equal in every compared batch. The limit is coverage: a killed run saves no reports, and a skipped
re-run's report counts nothing, so the check says how many batches it compared.

**How did you change the schema without breaking anything?**
Version 2 adds one optional field at the end. The decoder reads the version first: version 1 keeps its exact key set,
version 2 may carry `campaign` or leave it out. The encoder writes a version's own keys, so version-1 bytes didn't
change (the generator's pinned hash held). Readers go first: a version-1 reader dead-letters version-2 records, which
you'd notice and can replay. Then the stored data: raw Parquet written before the upgrade has no `campaign` column, so
DuckDB's views read with `union_by_name` (by position it refuses the mix) and the backfill reads with the schema given.

**How does the backfill reuse the stream's code, and what couldn't it reuse?**
It calls the same `minute_aggregates` (dedupe, then `window_counts`) on a batch DataFrame of the raw events. One thing
didn't carry over: Spark 4.2 refuses `dropDuplicatesWithinWatermark` on a batch, so `deduplicated` uses `dropDuplicates`
on the same key when its input isn't a stream. The result must equal the truth of every event, very late ones included,
and `flowcast check` holds it to that. It's published by writing a new directory and renaming a pointer file over
`CURRENT`, so a reader sees a whole result or the previous one.

## Measuring, batch size and data quality (day 4)

**How do you know when a row became queryable?**
From the file sink's own log. Spark writes a batch's Parquet files first and lists them in `_spark_metadata/<batch>`
when the batch commits; the DuckDB views read that log, never a glob, so a row is visible exactly from the moment its
batch's log file exists. That file's modification time is the commit time (`latency.commits`). Each row is then
measured against its batch: from its Kafka timestamp (the producer's CreateTime: the pipeline's own latency), from its
event time (what a user waits), and for a minute window from the window's end. Spark's progress reports would give a
batch's start and duration, but not when a reader could see it.

**Why are your event-time latencies only from the live benchmark?**
Because the generator's clock starts at 2026-01-01 by default: on a backlog run, "event time to queryable" would be
ten months. `--start-ms now` anchors the first session at the wall clock and `--speed 1` sends each record at its
arrival time, so an event's time is when it was sent less its simulated network delay and lateness. `--speed 10` would
be ten times the rate but would also squeeze event time, so the live rate is set with `--sessions-per-second` instead.

**Why did 300-record batches drop 10.7% of a backlog as late, and 2,000-record batches almost nothing?**
Not because those records were late: regenerating day 3's stream to label every record, 1,200 of the 1,280 dropped were
on time (median lateness 128 ms). `maxOffsetsPerTrigger` splits a batch across partitions in proportion to what each
has left, so the partitions stay level in record counts, not in event time, while the watermark is one number for the
whole topic. In a sparse stream (0.5 sessions a second that day), a partition holding fewer active sessions covers
more event time per record, and its lead over the slowest partition reached 388 s at the median batch end, more than
the 180 s delay. A batch drops by the previous batch's watermark, which trails what it reads by the delay plus about a
batch's step. At 300 a batch the step is 138 s, so the lead passes on-time records of the slow partitions; at 2,000 the
step is 1,011 s and covers the lead. `batching.py` reproduces Spark's split (`rateLimit`) and the replay: on day 3's
topic it gives the same end offsets as the real checkpoint for all 41 batches and the same 1,280 dropped event ids.

**So how would you fix it?**
In this job, by reading a backlog in large batches, or by a watermark delay larger than the partitions' lead.
Generally the lead is the thing to bound: Spark has one watermark per query and no per-partition watermarks for
Kafka (Flink has them, with idle-partition handling), so a live stream that is balanced in time is fine, and a
backfill of a sparse topic through the streaming path is where it bites. The batch backfill (day 3) has no watermark
at all, which is why it is the source of truth for history.

**Why data-quality rules when `check` already proves the run exact?**
`check` needs the broker, the checkpoints and a stopped job; it answers "did the job do what it should". The rules
answer "is this data sane" from the Parquet files alone, so they run on a copy, on a run in progress, or after
something other than the job touched the files. Each is SQL that counts the rows breaking it (an unknown event type,
a window off the minute, a Kafka record that's both an event and a dead letter, a window counting more than the raw
events behind it), so a failure names its rows.

## The report, the string rules and the release (day 5)

**What does `flowcast report` show, and why a static page?**
One HTML file per run: row counts, events and revenue per minute of event time, events by type, the latencies and
throughput, every data-quality rule with its verdict, the exact check when given `--topic`, and `scripts/bench.py`'s
results when given `--bench`. Static because it has to outlive the run: no server, no scripts, nothing fetched, so it
opens from disk or as a release asset and shows the same thing later. A test refuses `<script`, `src=`, `href=`,
`url(` and `://` in the page.

**How do you know the page's numbers are right?**
It doesn't compute its own: `collect` calls `run_rules`, `latencies`, `throughput` and `check_run`, the same functions
`flowcast quality`, `latency` and `check` print, and its only SQL is two sums over the views (per minute, per type).
`render` is a pure function of the collected numbers, so the tests build runs by hand (Parquet files and sink logs, no
Spark) and check the page for exact cells; the same numbers always give the same bytes.

**What does the stacked chart mean?**
With a published backfill, each minute's bar is the backfill's events for that minute (the truth of every raw event,
very late ones included), split into what the stream counted and what only the backfill has. The top part is the
records the stream dropped as late. On day 3's kill run that is 1,574 events: 1,263 in rows both wrote, and 311 in 29
rows (a minute and an event type) the stream never wrote, because every record of them came after the watermark.

**Why does the page say a run's event-time latency isn't the pipeline's?**
Because on a backlog it isn't: day 3's kill run replayed events from January, so "event time to queryable" is ten
months. The page compares the medians of event time and produced time to queryable, and when they are more than a
day apart says the run was a backlog. The CLI's `latency` prints the numbers either way.

**What did the day-4 review leave open, and how is it closed?**
The quality rules didn't restate the decoder's string formats (ids, country, page, category, campaign), though the
module said a raw event passing them was one the decoder would accept. `schema.STRING_FORMATS` now holds the decoder's
own patterns, and the rules give them to DuckDB's `regexp_full_match`; they use only what Python's `re` and RE2 read
the same way. A Hypothesis test takes generated events, replaces up to three fields with values on either side of
each rule (a 32-digit user id, `USA`, a page with a space or DEL, a non-ASCII digit, NULL), and requires that the row
passes every rule exactly when `decode` accepts its JSON. Removing the page rule makes it fail with `"page": ""`.

**How fresh is the data, and what sets it?**
At a 1 s trigger, half of all events were queryable 1.05 s after they were produced and 95% within 1.9 s (100 sessions
a second, 112,079 records, five minutes on a live stream; README "Results (day 5)"). A row waits for the next batch,
half the interval on average, then for that batch's work and commit; at 5 and 10 s the median is half the interval plus
0.4 to 0.5 s. The batch's work is mostly fixed cost: raw_events' batches took 0.79 s each at 389 records and 0.77 s at
3,615, so at 1 s the queries were busy 76 to 95% of the time and the trigger can't go much lower on this laptop.

**And how fresh is a minute's count?**
Three minutes late, by design: the watermark trails the newest event time by 180 s so that records up to three
minutes late still count. A minute's row is written 182.4 s after the minute ends at a 1 s trigger, 190.7 s at 5 s and
200.7 s at 10 s: the delay plus about two trigger intervals, because a batch runs with the previous batch's watermark,
so the batch after the one that first sees an event time past the window's end plus 180 s writes it. If a product
needed fresher minutes, the choice is between a shorter delay (more records dropped as late, which `check` would
count exactly) and emitting early results that are later corrected (update mode, which the Parquet sink can't do).

**Why not a 100 ms trigger, then?**
Because the cost per batch is fixed and the queries were already busy 95% of the time at 1 s: at a shorter interval
the batches would run back to back and fall behind, and latency would grow instead of shrink. A lower floor needs
less work per batch: one query reading Kafka and parsing once (today four queries each do), or a different engine
for the fresh path. Spark's experimental continuous trigger supports only map-like queries, not these windows or the
deduplication.

**What's the job's throughput?**
Over a dense backlog of 200,000 records, 18,139 rows a second through raw_events' batches at 50,000 records a batch
(10,911 over the whole 18.3 s run, Spark's start included), against 3,828 at 2,000 a batch. The rate grows with the
batch because each batch carries a fixed cost; on the live stream at 375 records a second the job was nowhere near
that ceiling at 5 and 10 s triggers (busy 8 to 17% of the time) and near it at 1 s. Those are one laptop's numbers in
local mode, four queries sharing its cores.
