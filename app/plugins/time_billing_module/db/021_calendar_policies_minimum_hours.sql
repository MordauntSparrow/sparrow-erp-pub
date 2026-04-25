-- Minimum paid hours / minimum charge style rules for timesheets & runsheets.
-- See RateResolver + TimesheetService._refresh_week_pay_and_daily_mins.

ALTER TABLE calendar_policies
  ADD COLUMN minimum_hours DECIMAL(5,2) NULL DEFAULT NULL
  COMMENT 'Floor hours: per-shift paid hours, or daily total hours threshold (by policy type)'
  AFTER ot_tier2_mult;

ALTER TABLE calendar_policies
  MODIFY COLUMN type ENUM(
    'WEEKEND',
    'BANK_HOLIDAY',
    'NIGHT',
    'OVERTIME_SHIFT',
    'OVERTIME_DAILY',
    'OVERTIME_WEEKLY',
    'MINIMUM_HOURS_PER_SHIFT',
    'MINIMUM_HOURS_DAILY_GLOBAL',
    'MINIMUM_HOURS_DAILY_CLIENT',
    'MINIMUM_HOURS_DAILY_CONTRACTOR_CLIENT'
  ) NOT NULL;
