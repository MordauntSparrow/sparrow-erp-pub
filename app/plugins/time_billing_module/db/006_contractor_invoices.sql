-- Self-employed contractor invoicing: invoice per approved week, optional number, status flow submitted -> approved -> invoiced.
-- Rejection after invoice: void invoice and allow "create new invoice" after re-approval.

-- Contractor employment type: PAYE (timesheet only) vs self-employed (timesheet + invoice)
ALTER TABLE tb_contractors
  ADD COLUMN employment_type ENUM('paye','self_employed') NOT NULL DEFAULT 'paye'
  COMMENT 'paye = timesheet only, self_employed = can create invoice after approval'
  AFTER status;

-- Invoices created by self-employed contractors for an approved week
CREATE TABLE IF NOT EXISTS contractor_invoices (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  timesheet_week_id BIGINT NOT NULL,
  invoice_number VARCHAR(64) NOT NULL,
  total_amount DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  status ENUM('draft','sent','void') NOT NULL DEFAULT 'draft',
  sent_at DATETIME DEFAULT NULL,
  voided_at DATETIME DEFAULT NULL,
  void_reason VARCHAR(255) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_contractor_invoice_number (contractor_id, invoice_number),
  KEY idx_ci_contractor (contractor_id),
  KEY idx_ci_week (timesheet_week_id),
  KEY idx_ci_status (status),
  CONSTRAINT fk_ci_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE,
  CONSTRAINT fk_ci_week FOREIGN KEY (timesheet_week_id) REFERENCES tb_timesheet_weeks(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Link entries to invoice; when invoice is voided we clear this
ALTER TABLE tb_timesheet_entries
  ADD COLUMN invoice_id BIGINT DEFAULT NULL AFTER runsheet_id,
  ADD KEY idx_tse_invoice (invoice_id),
  ADD CONSTRAINT fk_tse_invoice FOREIGN KEY (invoice_id) REFERENCES contractor_invoices(id) ON DELETE SET NULL;

-- Week status: add 'invoiced' for completed flow (submitted -> approved -> invoiced)
ALTER TABLE tb_timesheet_weeks
  MODIFY COLUMN status ENUM('draft','submitted','approved','rejected','invoiced') NOT NULL DEFAULT 'draft';
