-- Backfill: rows with NULL/empty username get values from install.py (see _backfill_contractor_usernames).

SET @tb_contractor_username_backfill_migration_014 = 1;
