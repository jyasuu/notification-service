#!/usr/bin/env bash
# check_business_db_migrations.sh
#
# Verifies that every SQL file in migrations/business_db/ is byte-for-byte
# identical to the corresponding file in migrations/.
#
# Run this in CI (or as a pre-commit hook) to catch accidental drift between
# the two directories.  Both directories must be kept in sync because:
#
#   migrations/            — applied to the anvil-notify DB
#   migrations/business_db/ — applied to the upstream business-service DB
#
# The files that appear in both directories contain shared schema objects
# (e.g. the outbox table, the notify_send_email() helper function).  Any
# change to the shared SQL must be applied in both places.
#
# Usage:
#   bash scripts/check_business_db_migrations.sh
#
# Exit codes:
#   0 — all shared files are identical
#   1 — one or more files differ or are missing from migrations/

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

BUSINESS_DB_DIR="$ROOT/migrations/business_db"
MAIN_DIR="$ROOT/migrations"

drift=0

for biz_file in "$BUSINESS_DB_DIR"/*.sql; do
    base="$(basename "$biz_file")"
    main_file="$MAIN_DIR/$base"

    if [[ ! -f "$main_file" ]]; then
        echo "MISSING  $base  (exists in migrations/business_db/ but not in migrations/)" >&2
        drift=1
        continue
    fi

    if ! diff -q "$biz_file" "$main_file" > /dev/null 2>&1; then
        echo "DIFFERS  $base" >&2
        diff --unified "$main_file" "$biz_file" >&2 || true
        drift=1
    else
        echo "OK       $base"
    fi
done

if [[ $drift -ne 0 ]]; then
    echo ""
    echo "ERROR: migrations/business_db/ has drifted from migrations/." >&2
    echo "Apply the same changes to both directories and re-run this check." >&2
    exit 1
fi

echo ""
echo "All shared migration files are in sync."

# ── Outbox table DDL consistency check ───────────────────────────────────────
#
# migrations/0001_initial_schema.sql contains the full notify-service schema
# (notification_log, templates, etc.) plus a shadow copy of the outbox table.
# migrations/business_db/0001_initial_schema.sql contains ONLY the outbox
# table.  The two files intentionally differ in their surrounding context, so
# the byte-for-byte check above skips them.
#
# This section extracts the CREATE TABLE outbox ... block from each file and
# compares only that portion, which is the part that must stay in sync.
#
# Extraction: grab every line from "CREATE TABLE IF NOT EXISTS outbox" up to
# the first standalone ");" that closes it.

extract_outbox_ddl() {
    local file="$1"
    awk '/^CREATE TABLE IF NOT EXISTS outbox/,/^\);/' "$file"
}

notify_outbox=$(extract_outbox_ddl "$ROOT/migrations/0001_initial_schema.sql")
business_outbox=$(extract_outbox_ddl "$ROOT/migrations/business_db/0001_initial_schema.sql")

if [[ "$notify_outbox" != "$business_outbox" ]]; then
    echo "" >&2
    echo "DIFFERS  outbox table DDL in 0001_initial_schema.sql" >&2
    diff <(echo "$business_outbox") <(echo "$notify_outbox") >&2 || true
    echo "" >&2
    echo "ERROR: The outbox CREATE TABLE block has drifted between" >&2
    echo "  migrations/0001_initial_schema.sql" >&2
    echo "  migrations/business_db/0001_initial_schema.sql" >&2
    echo "Apply the same column/index changes to both files." >&2
    exit 1
else
    echo "OK       outbox table DDL (0001_initial_schema.sql)"
fi
