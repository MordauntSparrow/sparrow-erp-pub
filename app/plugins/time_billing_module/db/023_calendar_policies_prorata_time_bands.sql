-- Night: PRORATA mode splits pay by clock overlap with window (vs legacy max-of whole shift).
-- TIME_BANDS: JSON-defined clock windows (optional weekdays) with multiplier / absolute / uplift per hour.

ALTER TABLE calendar_policies
  ADD COLUMN time_bands_json JSON NULL DEFAULT NULL
  COMMENT 'TIME_BANDS policy: JSON array of {weekdays?,window_start,window_end,multiplier?,absolute_rate?,uplift_per_hour?,label?}'
  AFTER minimum_hours;

ALTER TABLE calendar_policies
  MODIFY COLUMN type ENUM(
    'WEEKEND',
    'BANK_HOLIDAY',
    'NIGHT',
    'TIME_BANDS',
    'OVERTIME_SHIFT',
    'OVERTIME_DAILY',
    'OVERTIME_WEEKLY',
    'MINIMUM_HOURS_PER_SHIFT',
    'MINIMUM_HOURS_DAILY_GLOBAL',
    'MINIMUM_HOURS_DAILY_CLIENT',
    'MINIMUM_HOURS_DAILY_CONTRACTOR_CLIENT'
  ) NOT NULL;

ALTER TABLE calendar_policies
  MODIFY COLUMN mode ENUM('OFF','MULTIPLIER','ABSOLUTE','PRORATA') NOT NULL DEFAULT 'OFF';
