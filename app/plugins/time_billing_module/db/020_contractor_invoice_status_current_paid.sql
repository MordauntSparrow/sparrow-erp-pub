-- Contractor portal invoices: replace legacy draft/sent with current/paid (void unchanged).
-- current = on file, awaiting finalisation when timesheet not yet fully approved for that invoice
-- paid = finalised (was sent): weeks marked invoiced, sent_at set

ALTER TABLE contractor_invoices
  MODIFY COLUMN status ENUM('draft','sent','void','current','paid') NOT NULL DEFAULT 'draft';

UPDATE contractor_invoices SET status = 'current' WHERE status = 'draft';
UPDATE contractor_invoices SET status = 'paid' WHERE status = 'sent';

ALTER TABLE contractor_invoices
  MODIFY COLUMN status ENUM('current','paid','void') NOT NULL DEFAULT 'current';
