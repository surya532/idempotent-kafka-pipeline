#!/usr/bin/env bash
# Usage: scripts/load_test.sh [count] [duplicates]
# Sends `count` unique events plus `duplicates` replays, waits until all unique events are processed,
# and prints MEASURED end-to-end throughput on your machine. Report this number as-is; do not extrapolate.
set -euo pipefail
COUNT=${1:-10000}
DUPS=${2:-1000}
BASE=${BASE:-http://localhost:8080}

START_COUNT=$(curl -s "$BASE/stats" | sed 's/[^0-9]*\([0-9]*\).*/\1/')
START=$(date +%s.%N)
curl -s -X POST "$BASE/demo/burst?count=$COUNT&duplicates=$DUPS" > /dev/null
TARGET=$((START_COUNT + COUNT))
while true; do
  NOW_COUNT=$(curl -s "$BASE/stats" | sed 's/[^0-9]*\([0-9]*\).*/\1/')
  [ "$NOW_COUNT" -ge "$TARGET" ] && break
  sleep 0.2
done
END=$(date +%s.%N)
ELAPSED=$(echo "$END - $START" | bc)
echo "unique=$COUNT duplicates_replayed=$DUPS processed_total=$NOW_COUNT elapsed=${ELAPSED}s"
echo "throughput: $(echo "scale=1; $COUNT / $ELAPSED" | bc) events/sec (local, single broker, single instance)"
if [ "$NOW_COUNT" -ne "$TARGET" ]; then echo "WARNING: duplicates leaked: expected $TARGET got $NOW_COUNT"; else echo "OK: no duplicate effects"; fi
