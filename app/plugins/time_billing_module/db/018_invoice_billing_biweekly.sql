-- Allow bi-weekly invoice / pay cadence alongside weekly and monthly (HR + contractor portal).
ALTER TABLE tb_contractors
  MODIFY COLUMN invoice_billing_frequency
    ENUM('weekly','biweekly','monthly') NOT NULL DEFAULT 'weekly';
