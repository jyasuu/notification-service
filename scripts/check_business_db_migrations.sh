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
