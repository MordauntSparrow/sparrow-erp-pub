#!/usr/bin/env bash
# Simple MySQL dump backup script
# Environment variables used:
#  DB_HOST (default localhost)
#  DB_PORT (default 3306)
#  DB_USER
#  DB_PASS
#  DB_NAME
#  BACKUP_DIR (default ./backups)

set -euo pipefail

DB_HOST=${DB_HOST:-localhost}
DB_PORT=${DB_PORT:-3306}
DB_USER=${DB_USER:-root}
DB_PASS=${DB_PASS:-}
DB_NAME=${DB_NAME:-sparrow}
BACKUP_DIR=${BACKUP_DIR:-$(pwd)/backups}

mkdir -p "$BACKUP_DIR"
TS=$(date +"%Y%m%d-%H%M%S")
OUTFILE="$BACKUP_DIR/${DB_NAME}-${TS}.sql.gz"

echo "Starting backup of $DB_NAME to $OUTFILE"
mysqldump -h "$DB_HOST" -P "$DB_PORT" -u "$DB_USER" ${DB_PASS:+-p"$DB_PASS"} --single-transaction --quick --routines --events "$DB_NAME" | gzip -c > "$OUTFILE"

echo "Backup complete: $OUTFILE"

# Rotate: remove backups older than 30 days
find "$BACKUP_DIR" -type f -name "${DB_NAME}-*.sql.gz" -mtime +30 -print -delete || true
echo "Old backups (30+ days) removed"
