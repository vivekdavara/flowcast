"""What parsing costs: `schema.decode` alone in Python, and the job's parse UDF inside Spark against a plain scan.

    .venv/bin/python scripts/measure_parse.py [--records N] [--rounds R]

Encodes N generated records (synthetic data) once. Then, best of R rounds each:
- `decode` over every record in this process (what one Python worker does per record, without Arrow);
- in Spark (local[*], a DataFrame of the values cached in memory): a scan that sums the values' lengths, against
  `spark_job.parse` with a filter on its verdict that makes the UDF run for every row. Their difference is what the
  UDF adds per batch: the round trip through Arrow to the Python workers and `decode` itself.
"""

from __future__ import annotations

import argparse
import os
import time
from collections.abc import Callable

from flowcast.generator import Config, take
from flowcast.schema import decode, encode


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--records", type=int, default=200_000)
    parser.add_argument("--rounds", type=int, default=5)
    args = parser.parse_args()
    values = [encode(r.event) for r in take(args.records, Config())]

    best = float("inf")
    for _ in range(args.rounds):
        started = time.perf_counter()
        for value in values:
            decode(value)
        best = min(best, time.perf_counter() - started)
    print(
        f"{len(values):,} records ({sum(map(len, values)) / len(values):.0f} bytes each), best of {args.rounds} rounds"
    )
    print(f"decode in one Python process: {len(values) / best:>12,.0f} records/s")

    os.environ.setdefault("JAVA_HOME", "/opt/homebrew/opt/openjdk@21")
    from pyspark.sql import functions as F
    from pyspark.sql.types import BinaryType, StructField, StructType

    from flowcast.spark_job import parse, session

    spark = session("flowcast-measure-parse", kafka=False)
    spark.sparkContext.setLogLevel("WARN")
    try:
        cores = int(spark.sparkContext.defaultParallelism)
        schema = StructType([StructField("value", BinaryType())])
        frame = spark.createDataFrame([(v,) for v in values], schema).repartition(cores)
        records = frame.select(
            "value",
            F.lit("t").alias("topic"),
            F.lit(0).alias("partition"),
            F.monotonically_increasing_id().alias("offset"),
            F.current_timestamp().alias("timestamp"),
        ).cache()
        records.count()
        cases: dict[str, Callable[[], object]] = {
            "scan (sum of lengths)": lambda: records.agg(F.sum(F.length("value"))).collect(),
            "parse UDF (valid count)": lambda: (
                parse(records).where(F.col("error").isNull()).agg(F.count("*")).collect()
            ),
        }
        times = dict.fromkeys(cases, float("inf"))
        for _ in range(args.rounds):
            for name, run in cases.items():
                started = time.perf_counter()
                run()
                times[name] = min(times[name], time.perf_counter() - started)
        for name, seconds in times.items():
            print(
                f"Spark {name:24} {seconds * 1000:>8,.0f} ms: {len(values) / seconds:>12,.0f} records/s ({cores} cores)"
            )
        added = times["parse UDF (valid count)"] - times["scan (sum of lengths)"]
        print(f"the UDF adds {added * 1e6 / len(values):.2f} µs a record across {cores} cores")
    finally:
        spark.stop()
    print(f"load average {os.getloadavg()[0]:.2f} at the end")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
