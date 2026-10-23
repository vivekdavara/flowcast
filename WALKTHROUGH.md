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
