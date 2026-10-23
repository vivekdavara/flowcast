#!/usr/bin/env bash
# A local single-node Kafka for FlowCast: KRaft mode (broker and controller in one process), loopback only.
#
#   scripts/kafka.sh start | stop | status | bootstrap | logs | reset
#
# Config, data and logs live in .kafka/ (git-ignored). Broker 127.0.0.1:9192, controller 127.0.0.1:9193 (the
# portfolio's port map). Overrides: FLOWCAST_KAFKA_DIR, FLOWCAST_KAFKA_PORT, FLOWCAST_KAFKA_CONTROLLER_PORT, KAFKA_BIN,
# FLOWCAST_JAVA_HOME. `reset` deletes the directory, but only one holding the script's marker: one it made, or an empty
# one it was started in. `stop` signals only a process started with this directory's config.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DIR="${FLOWCAST_KAFKA_DIR:-$ROOT/.kafka}"
case "$DIR" in /*) ;; *) DIR="$PWD/$DIR" ;; esac
PORT="${FLOWCAST_KAFKA_PORT:-9192}"
CONTROLLER_PORT="${FLOWCAST_KAFKA_CONTROLLER_PORT:-9193}"
KAFKA_BIN="${KAFKA_BIN:-/opt/homebrew/opt/kafka/bin}"
# Kafka 4 runs on Java 17+; Java 21 is what Spark uses here too (DESIGN.md "Kafka").
export JAVA_HOME="${FLOWCAST_JAVA_HOME:-/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home}"
export LOG_DIR="$DIR/logs"
MARKER="$DIR/.flowcast-kafka"
PIDFILE="$DIR/kafka.pid"
START_TIMEOUT="${FLOWCAST_KAFKA_START_TIMEOUT:-60}"

die() { echo "kafka.sh: $*" >&2; exit 1; }

# Homebrew installs the tools without the .sh the Apache tarball has (bin/kafka-server-start.sh); take either.
tool() {
  if [ -x "$KAFKA_BIN/$1" ]; then
    echo "$KAFKA_BIN/$1"
  elif [ -x "$KAFKA_BIN/$1.sh" ]; then
    echo "$KAFKA_BIN/$1.sh"
  else
    die "no $1 in $KAFKA_BIN (brew install kafka, or set KAFKA_BIN to a Kafka bin directory)"
  fi
}

# The pid in the pid file, if that process is this directory's broker. A pid is reused once its process exits (a
# crash, a reboot), so a live pid alone proves nothing: the process must have been started with this config.
running_pid() {
  [ -f "$PIDFILE" ] || return 1
  local pid
  pid="$(cat "$PIDFILE")"
  case "$pid" in '' | *[!0-9]* | 0) return 1 ;; esac
  kill -0 "$pid" 2>/dev/null || return 1
  ps -ww -p "$pid" -o command= 2>/dev/null | grep -qF -- " $DIR/server.properties" || return 1
  echo "$pid"
}

# A connect, not lsof: it needs no tool, and on macOS it also sees a listener on *:port, which can coexist with a
# bind to 127.0.0.1:port.
listening() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

write_config() {
  cat > "$DIR/server.properties" <<EOF
# Written by scripts/kafka.sh on every start; edit the script, not this file.
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:$CONTROLLER_PORT
listeners=PLAINTEXT://127.0.0.1:$PORT,CONTROLLER://127.0.0.1:$CONTROLLER_PORT
advertised.listeners=PLAINTEXT://127.0.0.1:$PORT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
log.dirs=$DIR/data
num.partitions=6
auto.create.topics.enable=false
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
share.coordinator.state.topic.replication.factor=1
share.coordinator.state.topic.min.isr=1
group.initial.rebalance.delay.ms=0
EOF
}

start() {
  if pid="$(running_pid)"; then
    echo "kafka already running (pid $pid) on 127.0.0.1:$PORT"
    return 0
  fi
  for port in "$PORT" "$CONTROLLER_PORT"; do
    listening "$port" && die "something else is listening on port $port"
  done
  local server storage
  server="$(tool kafka-server-start)"
  storage="$(tool kafka-storage)"
  [ -x "$JAVA_HOME/bin/java" ] || die "no Java at $JAVA_HOME (brew install openjdk@21, or set FLOWCAST_JAVA_HOME)"
  if [ -e "$DIR" ] && [ ! -f "$MARKER" ] && [ -n "$(ls -A "$DIR")" ]; then
    die "$DIR exists and wasn't made by this script"
  fi
  mkdir -p "$DIR/logs"
  touch "$MARKER"
  write_config
  if [ ! -f "$DIR/data/meta.properties" ]; then
    local cluster_id
    cluster_id="$("$storage" random-uuid)"
    "$storage" format -t "$cluster_id" -c "$DIR/server.properties" > "$DIR/logs/format.log" 2>&1 \
      || { cat "$DIR/logs/format.log" >&2; die "formatting the log directory failed"; }
  fi
  : > "$DIR/logs/console.log"
  nohup "$server" "$DIR/server.properties" > "$DIR/logs/console.log" 2>&1 &
  echo $! > "$PIDFILE"
  local deadline=$((SECONDS + START_TIMEOUT))
  until grep -q "Kafka Server started" "$DIR/logs/console.log"; do
    if ! running_pid >/dev/null; then
      tail -20 "$DIR/logs/console.log" >&2
      rm -f "$PIDFILE"
      die "kafka exited during startup"
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      tail -20 "$DIR/logs/console.log" >&2
      die "kafka didn't start within ${START_TIMEOUT}s (still running: scripts/kafka.sh stop)"
    fi
    sleep 0.5
  done
  echo "kafka running (pid $(cat "$PIDFILE")) on 127.0.0.1:$PORT"
}

stop() {
  local pid
  if ! pid="$(running_pid)"; then
    rm -f "$PIDFILE"
    echo "kafka not running"
    return 0
  fi
  kill "$pid" 2>/dev/null || true  # it may have exited since running_pid looked
  for _ in $(seq 1 60); do
    kill -0 "$pid" 2>/dev/null || break
    sleep 0.5
  done
  if kill -0 "$pid" 2>/dev/null; then
    echo "kafka.sh: pid $pid still running after 30s; killing it" >&2
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$PIDFILE"
  echo "kafka stopped"
}

status() {
  local pid
  if pid="$(running_pid)"; then
    echo "running (pid $pid) on 127.0.0.1:$PORT, data in $DIR/data"
  else
    echo "stopped"
    return 3
  fi
}

reset() {
  stop
  [ -e "$DIR" ] || { echo "nothing to reset"; return 0; }
  [ -f "$MARKER" ] || die "$DIR wasn't made by this script; not deleting it"
  rm -rf "$DIR"
  echo "deleted $DIR"
}

case "${1:-}" in
  start) start ;;
  stop) stop ;;
  status) status ;;
  bootstrap) echo "127.0.0.1:$PORT" ;;
  logs) tail -n "${2:-50}" "$DIR/logs/console.log" ;;
  reset) reset ;;
  *) echo "usage: scripts/kafka.sh start|stop|status|bootstrap|logs [n]|reset" >&2; exit 2 ;;
esac
