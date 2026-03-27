-- Per-assignment inclusion in payroll when a runsheet is published.
-- Allows one contractor to withdraw from this runsheet without removing others' assignments.

ALTER TABLE runsheet_assignments
    ADD COLUMN payroll_included TINYINT(1) NOT NULL DEFAULT 1
        COMMENT '0 = withdrawn from payroll for this runsheet; publish skips and timesheet row removed'
        AFTER notes,
    ADD COLUMN withdrawn_at DATETIME NULL DEFAULT NULL AFTER payroll_included,
    ADD COLUMN withdrawn_by_user_id INT NULL DEFAULT NULL AFTER withdrawn_at,
    ADD COLUMN reactivated_at DATETIME NULL DEFAULT NULL AFTER withdrawn_by_user_id;

ALTER TABLE runsheet_assignments
    ADD CONSTRAINT fk_rsa_withdrawn_by
        FOREIGN KEY (withdrawn_by_user_id) REFERENCES tb_contractors(id)
        ON DELETE SET NULL ON UPDATE CASCADE;
