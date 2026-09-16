#!/bin/sh
# Refresh the read-only snapshot of the warehouse.
#
# The live file takes one writer, so anything that only wants to read works
# against include/data/snapshot/. include/lib/warehouse.py refreshes the
# snapshot itself when connect(read_only=True) finds it stale; this script is
# the by-hand version for a shell session, and the pattern the 01:00
# retention DAG calls.
#
# The copy has to happen through DuckDB, not cp: a file copied mid-checkpoint
# is a corrupt database, and we learned that the hard way (see
# ops/incidents/, the February archive gap started as a cp of a live file).
set -eu

LIVE="${DUCKDB_PATH:-include/data/copperline.duckdb}"
SNAP_DIR="$(dirname "$LIVE")/snapshot"
SNAP="$SNAP_DIR/$(basename "$LIVE")"

mkdir -p "$SNAP_DIR"
duckdb "$LIVE" -readonly -c "ATTACH '$SNAP.tmp' AS snap; COPY FROM DATABASE \"$(basename "$LIVE" .duckdb)\" TO snap; DETACH snap;"
mv "$SNAP.tmp" "$SNAP"
echo "snapshot refreshed: $SNAP"
