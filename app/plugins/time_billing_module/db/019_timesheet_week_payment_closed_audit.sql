-- Admin can mark an approved timesheet as paid/closed without a portal invoice (PAYE, legacy, or paid off-system).
-- Sets week status to invoiced; optional audit columns distinguish from contractor-submitted invoices.

ALTER TABLE tb_timesheet_weeks
  ADD COLUMN payment_closed_at DATETIME DEFAULT NULL
    COMMENT 'Set when admin marks week paid without portal invoice'
    AFTER updated_at,
  ADD COLUMN payment_closed_by VARCHAR(64) DEFAULT NULL
    COMMENT 'Admin user id or name'
    AFTER payment_closed_at,
  ADD COLUMN payment_closed_note VARCHAR(512) DEFAULT NULL
    COMMENT 'Optional note e.g. PAY run reference'
    AFTER payment_closed_by;
