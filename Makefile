PY := .venv/bin/python
# The Kafka tests need a broker; `make kafka-start` runs the local one this points at (CI sets its own).
export FLOWCAST_TEST_BOOTSTRAP ?= 127.0.0.1:9192
# Spark 4 runs on Java 17 or 21; the Mac's default java is 23, so point Spark at Homebrew's 21 when it's there.
ifneq ($(wildcard /opt/homebrew/opt/openjdk@21),)
export JAVA_HOME ?= /opt/homebrew/opt/openjdk@21
endif

.PHONY: venv lint test check coverage kafka-start kafka-stop

venv:
	python3.11 -m venv .venv
	$(PY) -m pip install -q --upgrade pip
	$(PY) -m pip install -q -e '.[dev]'

lint:
	$(PY) -m ruff check .
	$(PY) -m ruff format --check .
	$(PY) -m mypy

test:
	$(PY) -m pytest

check: lint test

coverage:
	$(PY) -m pytest --cov --cov-report=term-missing

kafka-start:
	scripts/kafka.sh start

kafka-stop:
	scripts/kafka.sh stop
