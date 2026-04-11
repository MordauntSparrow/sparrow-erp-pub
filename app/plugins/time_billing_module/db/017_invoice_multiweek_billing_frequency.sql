-- Multi-week invoices (one invoice, many timesheet weeks), billing frequency, bank + staff ref on PDF.
-- contractor_invoices.timesheet_week_id remains the anchor week (first by week_ending) for joins; all weeks are listed in contractor_invoice_weeks.

ALTER TABLE tb_contractors
  ADD COLUMN invoice_billing_frequency ENUM('weekly','monthly') NOT NULL DEFAULT 'weekly'
    COMMENT 'weekly=invoice from timesheet week; monthly=combine from My invoices'
    AFTER invoice_country,
  ADD COLUMN invoice_bank_account_name VARCHAR(255) DEFAULT NULL AFTER invoice_billing_frequency,
  ADD COLUMN invoice_bank_sort_code VARCHAR(32) DEFAULT NULL AFTER invoice_bank_account_name,
  ADD COLUMN invoice_bank_account_number VARCHAR(64) DEFAULT NULL AFTER invoice_bank_sort_code,
  ADD COLUMN invoice_iban VARCHAR(64) DEFAULT NULL AFTER invoice_bank_account_number,
  ADD COLUMN invoice_staff_reference VARCHAR(128) DEFAULT NULL COMMENT 'Staff/contractor ref on PDF' AFTER invoice_iban;

CREATE TABLE IF NOT EXISTS contractor_invoice_weeks (
  invoice_id BIGINT NOT NULL,
  timesheet_week_id BIGINT NOT NULL,
  PRIMARY KEY (invoice_id, timesheet_week_id),
  KEY idx_ciw_invoice (invoice_id),
  KEY idx_ciw_week (timesheet_week_id),
  CONSTRAINT fk_ciw_invoice FOREIGN KEY (invoice_id) REFERENCES contractor_invoices(id) ON DELETE CASCADE,
  CONSTRAINT fk_ciw_week FOREIGN KEY (timesheet_week_id) REFERENCES tb_timesheet_weeks(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT IGNORE INTO contractor_invoice_weeks (invoice_id, timesheet_week_id)
SELECT id, timesheet_week_id FROM contractor_invoices;
