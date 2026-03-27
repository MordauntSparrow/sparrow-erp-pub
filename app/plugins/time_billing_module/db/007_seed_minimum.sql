INSERT INTO roles (name, code) VALUES
    ('Cleaner', 'Cleaner'),
    ('Specialist', 'Specialist')
ON DUPLICATE KEY UPDATE 
    name = VALUES(name);

INSERT INTO job_types (name, code) VALUES
    ('School Clean', 'School Clean'),
    ('Residential Clean', 'Residential Clean'),
    ('Commercial Clean', 'Commercial Clean'),
    ('Deep Clean', 'Deep Clean'),
    ('End of Tenancy Clean', 'End of Tenancy Clean')
ON DUPLICATE KEY UPDATE 
    name = VALUES(name);

INSERT INTO wage_rate_cards (name) VALUES 
    ('Default Rate Card'), 
    ('Premium Rate Card')
ON DUPLICATE KEY UPDATE 
    name = VALUES(name);

UPDATE wage_rate_cards
SET role_id = (SELECT id FROM roles WHERE code = 'Cleaner')
WHERE name = 'Default Rate Card';

ALTER TABLE wage_rate_rows
ADD UNIQUE KEY uq_wrr (rate_card_id, job_type_id, effective_from);

INSERT INTO wage_rate_rows (rate_card_id, job_type_id, rate, effective_from)
SELECT
    (SELECT id FROM wage_rate_cards WHERE name = 'Default Rate Card'),
    jt.id,
    CASE 
        WHEN jt.code IN ('Deep Clean', 'End of Tenancy Clean') THEN 15.00 
        ELSE 14.50 
    END,
    CURDATE()
FROM job_types jt
ON DUPLICATE KEY UPDATE 
    rate = VALUES(rate);

INSERT INTO wage_rate_rows (rate_card_id, job_type_id, rate, effective_from)
SELECT
    (SELECT id FROM wage_rate_cards WHERE name = 'Premium Rate Card'),
    jt.id,
    15.00,
    CURDATE()
FROM job_types jt
ON DUPLICATE KEY UPDATE 
    rate = VALUES(rate);
