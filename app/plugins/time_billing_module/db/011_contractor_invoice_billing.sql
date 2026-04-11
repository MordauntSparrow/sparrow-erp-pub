-- Contractor invoice PDF / portal billing (when HR address is not used).
-- Also backfills employment_type for sandbox testing (remove UPDATE line for production if you use PAYE).

ALTER TABLE tb_contractors
  ADD COLUMN invoice_business_name VARCHAR(255) DEFAULT NULL
    COMMENT 'Trading name on contractor invoices (PDF)'
    AFTER ui_theme,
  ADD COLUMN invoice_address_line1 VARCHAR(255) DEFAULT NULL AFTER invoice_business_name,
  ADD COLUMN invoice_address_line2 VARCHAR(255) DEFAULT NULL AFTER invoice_address_line1,
  ADD COLUMN invoice_city VARCHAR(128) DEFAULT NULL AFTER invoice_address_line2,
  ADD COLUMN invoice_postcode VARCHAR(32) DEFAULT NULL AFTER invoice_city,
  ADD COLUMN invoice_country VARCHAR(128) DEFAULT NULL AFTER invoice_postcode;

-- Sandbox / migration: default everyone to self-employed so invoice flow is visible without per-row edits.
UPDATE tb_contractors SET employment_type = 'self_employed';
