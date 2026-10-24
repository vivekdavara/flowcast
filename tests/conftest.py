"""Shared fixtures. The Spark session is started once per test run (about 3 s, plus the Kafka source's jars from Maven
Central on the very first run) and only by tests that ask for it."""

import os
import shutil
from collections.abc import Iterator

import pytest


def _java_available() -> bool:
    home = os.environ.get("JAVA_HOME")
    return bool(home and os.path.exists(os.path.join(home, "bin", "java"))) or shutil.which("java") is not None


@pytest.fixture(scope="session")
def spark() -> Iterator[object]:
    if not _java_available():
        pytest.skip("no Java for Spark (set JAVA_HOME to a Java 21)")
    from flowcast.spark_job import session

    spark = session("flowcast-tests", "local[4]")
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
