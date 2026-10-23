# FlowCast

A streaming analytics pipeline built end to end on one laptop: a synthetic clickstream goes through Kafka (KRaft mode)
and a Spark Structured Streaming job into Parquet files that DuckDB queries.

Status: day 1 of 5 (design and scaffold). See [DESIGN.md](DESIGN.md) for the specification.

## How to run

```bash
make venv
make check
```

## License

MIT
