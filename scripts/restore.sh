#!/usr/bin/env bash
#
# Restore a nightly dump into a fresh database.
#
# The archive alone cannot rebuild the ledger. Identity lives in the database:
# `assets`, `players` and `asset_source_ids` are what turn an archived payload
# back into rows. This script is the other half of the backup.
#
# The order below is not optional. `timescaledb_pre_restore()` must run before
# pg_restore and `timescaledb_post_restore()` after it. Skipping the first gives
# a misleading failure part way through the restore:
#
#     ERROR: table "market_observations" is not a hypertable
#
# by which point the database holds a partial restore.
#
# Usage:
#   scripts/restore.sh <dump-file> [target-database]
#
# To fetch a dump from the archive bucket first:
#   mc cp fcm/fc-market-raw/backup/pg/<timestamp>.dump ./restore.dump

set -euo pipefail

DUMP=${1:?usage: scripts/restore.sh <dump-file> [target-database]}
TARGET=${2:-fcmarket_restored}

# The dump must be read by a pg_restore at least as new as the pg_dump that
# wrote it, and pg_dump must be at least as new as the server it read. Check the
# three versions before a restore you are depending on.
PSQL=${PSQL:-psql}
PG_RESTORE=${PG_RESTORE:-pg_restore}

: "${PGHOST:=localhost}"
: "${PGPORT:=5434}"
: "${PGUSER:=fcmarket}"
export PGHOST PGPORT PGUSER

[ -f "$DUMP" ] || { echo "no dump at $DUMP" >&2; exit 1; }

echo "restoring $DUMP into $TARGET"

# WITH (FORCE) because the TimescaleDB background worker holds a session open
# and blocks a plain DROP.
"$PSQL" -d postgres -q -c "DROP DATABASE IF EXISTS \"$TARGET\" WITH (FORCE);"
"$PSQL" -d postgres -q -c "CREATE DATABASE \"$TARGET\" OWNER $PGUSER;"

# The extension must exist at the server's version before anything is loaded.
"$PSQL" -d "$TARGET" -q -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"

"$PSQL" -d "$TARGET" -qtAc "SELECT timescaledb_pre_restore();" >/dev/null
"$PG_RESTORE" -d "$TARGET" "$DUMP"
"$PSQL" -d "$TARGET" -qtAc "SELECT timescaledb_post_restore();" >/dev/null

echo
echo "row counts"
"$PSQL" -d "$TARGET" -c "
SELECT 'assets' AS table, count(*) FROM assets
UNION ALL SELECT 'asset_source_ids', count(*) FROM asset_source_ids
UNION ALL SELECT 'market_observations', count(*) FROM market_observations
UNION ALL SELECT 'ingest_polls', count(*) FROM ingest_polls
ORDER BY 1;"

# A restore that lost these is a restore that lost the ledger's guarantees, even
# though every row came back. Compression settings must still cover every column
# of the idempotency index, or uniqueness stops being enforceable on a
# compressed chunk.
echo "compression settings, which must cover every column of the idempotency index"
"$PSQL" -d "$TARGET" -c "
SELECT hypertable_name, attname, segmentby_column_index AS seg, orderby_column_index AS ord
  FROM timescaledb_information.compression_settings
 ORDER BY 1, seg NULLS LAST, ord;"

echo "the idempotency index"
"$PSQL" -d "$TARGET" -tAc "
SELECT indexdef FROM pg_indexes WHERE indexname = 'market_observations_idem_idx';"

echo
echo "restored into $TARGET"
echo "now point the server at it and run one ingest: a healthy restore reports"
echo "every known price as unchanged, which proves identity resolution and the"
echo "idempotency index both survived."
