-- Add optional colour per job type for UI (runsheets, timesheets, summaries).
-- Also in 001_schema (new installs) and install.py _ensure_job_types_colour_hex (ledger drift).
-- Duplicate column (1060) is ignored by install.py when re-running.
ALTER TABLE job_types
  ADD COLUMN colour_hex VARCHAR(7) DEFAULT NULL
  COMMENT 'Hex colour e.g. #3366cc for badges/rows';
