#!/usr/bin/env bash
# Install a cronjob to run nightly DB backups using scripts/backup_db.sh

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
BACKUP_SCRIPT="$SCRIPT_DIR/backup_db.sh"

if [ ! -f "$BACKUP_SCRIPT" ]; then
  echo "Backup script not found: $BACKUP_SCRIPT"
  exit 2
fi

CRON_LINE="15 2 * * * $(whoami) $BACKUP_SCRIPT > /var/log/backup_db.log 2>&1"
echo "Installing cron job: $CRON_LINE"

sudo bash -c "(crontab -l 2>/dev/null; echo '$CRON_LINE') | crontab -"
echo "Cron installed. Backups will run nightly at 02:15 local time. Logs: /var/log/backup_db.log"
