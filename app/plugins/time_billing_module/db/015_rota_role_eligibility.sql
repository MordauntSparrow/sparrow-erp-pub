-- ROTA-ROLE-001: configurable role ladder for shift eligibility (client audit §3.3).
-- Higher ladder_rank = more qualified. Contractor may take a shift if max(rank) >= required role's rank.

ALTER TABLE roles
    ADD COLUMN ladder_rank INT NOT NULL DEFAULT 0
        COMMENT 'Higher = more senior; shift required_role uses same scale'
    AFTER active;

ALTER TABLE runsheets
    ADD COLUMN required_role_id INT NULL DEFAULT NULL
        COMMENT 'NULL = any contractor; else max(contractor ladder_rank) must be >= roles.ladder_rank'
    AFTER job_type_id,
    ADD CONSTRAINT fk_rs_required_role
        FOREIGN KEY (required_role_id) REFERENCES roles (id)
        ON DELETE SET NULL ON UPDATE CASCADE;

ALTER TABLE runsheet_assignments
    ADD COLUMN role_eligibility_override TINYINT(1) NOT NULL DEFAULT 0
        COMMENT '1 = admin forced assign despite ladder (audited)'
    AFTER reactivated_at,
    ADD COLUMN role_eligibility_override_reason VARCHAR(255) NULL DEFAULT NULL
    AFTER role_eligibility_override,
    ADD COLUMN role_eligibility_override_at DATETIME NULL DEFAULT NULL
    AFTER role_eligibility_override_reason,
    ADD COLUMN role_eligibility_override_staff_user_id INT NULL DEFAULT NULL
        COMMENT 'Core app user id (Flask-Login) when admin saved override'
    AFTER role_eligibility_override_at;

CREATE TABLE IF NOT EXISTS tb_runsheet_role_audit (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    runsheet_id BIGINT NOT NULL,
    assignment_id BIGINT NULL,
    event_type VARCHAR(32) NOT NULL,
    contractor_id INT NULL,
    required_role_id INT NULL,
    contractor_max_rank INT NULL,
    required_rank INT NULL,
    message VARCHAR(512) NULL,
    actor_staff_user_id INT NULL,
    actor_contractor_id INT NULL,
    INDEX idx_rra_runsheet (runsheet_id),
    INDEX idx_rra_created (created_at),
    CONSTRAINT fk_rra_runsheet FOREIGN KEY (runsheet_id) REFERENCES runsheets (id)
        ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
