-- ROTA-ADDR-001: optional full shift address (in addition to client/site directory).
-- ROTA-PAY-001: hourly vs flat day rate on the shift (inherit = existing wage card logic).

ALTER TABLE runsheets
    ADD COLUMN shift_address_line1 VARCHAR(255) NULL DEFAULT NULL
        COMMENT 'Optional shift-specific address (ROTA-ADDR-001)'
    AFTER site_free_text,
    ADD COLUMN shift_address_line2 VARCHAR(255) NULL DEFAULT NULL
    AFTER shift_address_line1,
    ADD COLUMN shift_city VARCHAR(120) NULL DEFAULT NULL
    AFTER shift_address_line2,
    ADD COLUMN shift_postcode VARCHAR(32) NULL DEFAULT NULL
    AFTER shift_city,
    ADD COLUMN shift_staff_role_id INT NULL DEFAULT NULL
        COMMENT 'Job role for this shift (display/reporting; eligibility ladder uses required_role_id)'
    AFTER shift_postcode,
    ADD COLUMN shift_pay_model ENUM ('inherit', 'hourly', 'day') NOT NULL DEFAULT 'inherit'
        COMMENT 'inherit = wage card; hourly/day use shift_pay_rate'
    AFTER shift_staff_role_id,
    ADD COLUMN shift_pay_rate DECIMAL(10, 2) NULL DEFAULT NULL
        COMMENT 'When hourly: per hour; when day: flat amount for the shift day'
    AFTER shift_pay_model;

ALTER TABLE runsheets
    ADD CONSTRAINT fk_rs_shift_staff_role
        FOREIGN KEY (shift_staff_role_id) REFERENCES roles (id)
        ON DELETE SET NULL ON UPDATE CASCADE;
