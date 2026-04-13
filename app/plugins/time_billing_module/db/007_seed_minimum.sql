-- Neutral baseline for all tenants (industry-specific roles/job types/rates: see industry_seed via install.py).
-- Ensures wage_rate_rows uniqueness for effective-dated rates.

ALTER TABLE wage_rate_rows
ADD UNIQUE KEY uq_wrr (rate_card_id, job_type_id, effective_from);

INSERT INTO roles (name, code) VALUES
    ('Staff', 'staff')
ON DUPLICATE KEY UPDATE
    name = VALUES(name);

INSERT INTO job_types (name, code) VALUES
    ('Standard shift', 'standard_shift')
ON DUPLICATE KEY UPDATE
    name = VALUES(name);

INSERT INTO wage_rate_cards (name) VALUES
    ('Organisation default'),
    ('Alternate card')
ON DUPLICATE KEY UPDATE
    name = VALUES(name);

UPDATE wage_rate_cards
SET role_id = (SELECT id FROM roles WHERE code = 'staff' LIMIT 1)
WHERE name = 'Organisation default';

INSERT INTO wage_rate_rows (rate_card_id, job_type_id, rate, effective_from)
SELECT
    (SELECT id FROM wage_rate_cards WHERE name = 'Organisation default' LIMIT 1),
    jt.id,
    12.00,
    CURDATE()
FROM job_types jt
WHERE jt.code = 'standard_shift'
ON DUPLICATE KEY UPDATE
    rate = VALUES(rate);

INSERT INTO wage_rate_rows (rate_card_id, job_type_id, rate, effective_from)
SELECT
    (SELECT id FROM wage_rate_cards WHERE name = 'Alternate card' LIMIT 1),
    jt.id,
    13.00,
    CURDATE()
FROM job_types jt
WHERE jt.code = 'standard_shift'
ON DUPLICATE KEY UPDATE
    rate = VALUES(rate);
