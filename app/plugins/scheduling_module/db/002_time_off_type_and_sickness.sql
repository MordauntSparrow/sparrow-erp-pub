-- Add type to time off (annual / sickness / other) for staff requests and sickness reporting.
ALTER TABLE schedule_time_off
  ADD COLUMN type ENUM('annual','sickness','other') NOT NULL DEFAULT 'annual' AFTER contractor_id;
