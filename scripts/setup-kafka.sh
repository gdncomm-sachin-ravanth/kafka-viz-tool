#!/usr/bin/env bash
#
# Downloads and extracts an Apache Kafka distribution for local use with
# kafka-viz-tool, verifies the download's SHA512 checksum, and (optionally)
# starts a ZooKeeper + broker pair on localhost:9092.
#
# Usage:
#   scripts/setup-kafka.sh [options]
#
# Options:
#   -v, --version VERSION   Kafka version to install (default: 3.2.1)
#   -s, --scala VERSION     Scala build to use (default: 2.13)
#   -d, --dir DIR           Install parent directory (default: your home directory)
#   --start                 Start ZooKeeper + broker after installing
#   --force                 Re-download/overwrite an existing install
#   --no-update-config      Don't write kafkaHome into ../config.json
#   -h, --help              Show this help
#
# Example:
#   scripts/setup-kafka.sh --version 3.2.1 --dir ~/tools --start

set -euo pipefail

KAFKA_VERSION="3.2.1"
SCALA_VERSION="2.13"
INSTALL_PARENT="$HOME"
START_AFTER_INSTALL=false
FORCE=false
UPDATE_CONFIG=true

usage() {
  sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
  case "$1" in
    -v|--version) KAFKA_VERSION="$2"; shift 2 ;;
    -s|--scala) SCALA_VERSION="$2"; shift 2 ;;
    -d|--dir) INSTALL_PARENT="$2"; shift 2 ;;
    --start) START_AFTER_INSTALL=true; shift ;;
    --force) FORCE=true; shift ;;
    --no-update-config) UPDATE_CONFIG=false; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

ARCHIVE_NAME="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
INSTALL_DIR="${INSTALL_PARENT}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
DOWNLOAD_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${ARCHIVE_NAME}"
CHECKSUM_URL="${DOWNLOAD_URL}.sha512"
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT

log() { printf '\033[1;36m==>\033[0m %s\n' "$1"; }
die() { printf '\033[1;31mError:\033[0m %s\n' "$1" >&2; exit 1; }

command -v curl >/dev/null 2>&1 || die "curl is required but not found"
command -v tar >/dev/null 2>&1 || die "tar is required but not found"

if ! command -v java >/dev/null 2>&1; then
  echo "Warning: no 'java' found on PATH. Kafka needs a JRE (11+ recommended)" \
       "to actually run - install one before starting the broker." >&2
fi

if [ -d "$INSTALL_DIR" ] && [ "$FORCE" != true ]; then
  log "Already installed at $INSTALL_DIR (use --force to re-download)"
else
  rm -rf "$INSTALL_DIR"
  mkdir -p "$INSTALL_PARENT"

  log "Downloading $ARCHIVE_NAME from $DOWNLOAD_URL"
  curl -fSL --progress-bar "$DOWNLOAD_URL" -o "$WORK_DIR/$ARCHIVE_NAME" \
    || die "Download failed - check that $KAFKA_VERSION/$SCALA_VERSION is a valid, released combination at https://kafka.apache.org/downloads"

  log "Verifying checksum"
  # Apache's .sha512 files look like "<file>: AAAA BBBB ... \n CCCC DDDD ..."
  # (hex in space-separated groups, wrapped across lines) rather than a
  # single hash column, so strip everything up to the colon and all
  # whitespace before comparing.
  EXPECTED_SHA512="$(curl -fSL "$CHECKSUM_URL" | sed 's/^[^:]*:[[:space:]]*//' | tr -d '[:space:]' | tr 'A-F' 'a-f')"
  [ -n "$EXPECTED_SHA512" ] || die "Could not fetch checksum from $CHECKSUM_URL"

  if command -v sha512sum >/dev/null 2>&1; then
    ACTUAL_SHA512="$(sha512sum "$WORK_DIR/$ARCHIVE_NAME" | awk '{print $1}')"
  else
    ACTUAL_SHA512="$(shasum -a 512 "$WORK_DIR/$ARCHIVE_NAME" | awk '{print $1}')"
  fi

  [ "$EXPECTED_SHA512" = "$ACTUAL_SHA512" ] \
    || die "Checksum mismatch - downloaded file may be corrupt or tampered with"

  log "Extracting to $INSTALL_DIR"
  tar -xzf "$WORK_DIR/$ARCHIVE_NAME" -C "$INSTALL_PARENT"
fi

for script in kafka-topics.sh kafka-console-consumer.sh kafka-console-producer.sh \
              kafka-run-class.sh kafka-configs.sh kafka-consumer-groups.sh \
              kafka-delete-records.sh kafka-server-start.sh zookeeper-server-start.sh; do
  [ -x "$INSTALL_DIR/bin/$script" ] || die "Expected $script under $INSTALL_DIR/bin but it's missing"
done

log "Installed at $INSTALL_DIR"

CONFIG_JSON="$(cd "$(dirname "$0")/.." && pwd)/config.json"
if [ "$UPDATE_CONFIG" = true ] && [ -f "$CONFIG_JSON" ] && command -v node >/dev/null 2>&1; then
  node -e "
    const fs = require('fs');
    const path = '$CONFIG_JSON';
    const cfg = JSON.parse(fs.readFileSync(path, 'utf8'));
    cfg.kafkaHome = '$INSTALL_DIR';
    fs.writeFileSync(path, JSON.stringify(cfg, null, 2));
  " && log "Updated kafkaHome in $CONFIG_JSON"
else
  log "Set 'Kafka home' to $INSTALL_DIR in kafka-viz-tool's Settings"
fi

if [ "$START_AFTER_INSTALL" = true ]; then
  log "Starting ZooKeeper and the broker (logs under $INSTALL_DIR/logs)"
  cd "$INSTALL_DIR"
  mkdir -p logs

  nohup bin/zookeeper-server-start.sh config/zookeeper.properties \
    > logs/zookeeper-stdout.log 2>&1 &
  ZK_PID=$!
  echo "$ZK_PID" > "$INSTALL_DIR/zookeeper.pid"
  log "ZooKeeper starting (pid $ZK_PID) - waiting a few seconds before the broker"
  sleep 5

  nohup bin/kafka-server-start.sh config/server.properties \
    > logs/kafka-stdout.log 2>&1 &
  BROKER_PID=$!
  echo "$BROKER_PID" > "$INSTALL_DIR/kafka.pid"
  log "Broker starting (pid $BROKER_PID) on localhost:9092"

  log "To stop later: kill \$(cat $INSTALL_DIR/kafka.pid) && kill \$(cat $INSTALL_DIR/zookeeper.pid)"
else
  cat <<EOF

Next steps:
  cd $INSTALL_DIR
  bin/zookeeper-server-start.sh config/zookeeper.properties   # terminal 1
  bin/kafka-server-start.sh config/server.properties          # terminal 2

Then open kafka-viz-tool and confirm the 'Local' environment points at
localhost:9092 (re-run this script with --start to have it do this for you).
EOF
fi
