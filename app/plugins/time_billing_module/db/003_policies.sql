CREATE TABLE IF NOT EXISTS bank_holidays (
    date DATE NOT NULL,
    region VARCHAR(100) NULL,
    region_norm VARCHAR(100) 
        GENERATED ALWAYS AS (IFNULL(region, '')) STORED,
    name VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, region_norm)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS calendar_policies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(150) NOT NULL,

    type ENUM(
        'WEEKEND',
        'BANK_HOLIDAY',
        'NIGHT',
        'OVERTIME_SHIFT',
        'OVERTIME_DAILY',
        'OVERTIME_WEEKLY',
        'MINIMUM_HOURS_PER_SHIFT',
        'MINIMUM_HOURS_DAILY_GLOBAL',
        'MINIMUM_HOURS_DAILY_CLIENT',
        'MINIMUM_HOURS_DAILY_CONTRACTOR_CLIENT'
    ) NOT NULL,

    scope ENUM(
        'GLOBAL',
        'ROLE',
        'JOB_TYPE',
        'CLIENT',
        'CONTRACTOR_CLIENT'
    ) NOT NULL,

    role_id INT NULL,
    job_type_id INT NULL,
    client_id INT NULL,
    contractor_id INT NULL,

    mode ENUM('OFF','MULTIPLIER','ABSOLUTE') NOT NULL DEFAULT 'OFF',
    multiplier DECIMAL(5,2) NULL,
    absolute_rate DECIMAL(10,2) NULL,

    window_start TIME NULL,
    window_end TIME NULL,

    ot_threshold_hours DECIMAL(5,2) NULL,
    ot_tier2_threshold_hours DECIMAL(5,2) NULL,
    ot_tier1_mult DECIMAL(5,2) NULL,
    ot_tier2_mult DECIMAL(5,2) NULL,

    minimum_hours DECIMAL(5,2) NULL DEFAULT NULL,

    applies_to ENUM('WAGE','BILL','BOTH') NOT NULL DEFAULT 'WAGE',
    stacking ENUM('NONE','OT_ON_TOP','FULL') NOT NULL DEFAULT 'OT_ON_TOP',

    effective_from DATE NOT NULL,
    effective_to DATE NULL,

    active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_cp_role       FOREIGN KEY (role_id)       REFERENCES roles(id)          ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_cp_jobtype    FOREIGN KEY (job_type_id)   REFERENCES job_types(id)      ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_cp_client     FOREIGN KEY (client_id)     REFERENCES clients(id)        ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_cp_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE,

    INDEX idx_cp_effective (effective_from, effective_to, active),
    INDEX idx_cp_scope_type (scope, type),
    INDEX idx_cp_role_type (role_id, type, effective_from, effective_to),
    INDEX idx_cp_job_type (job_type_id, type, effective_from, effective_to),
    INDEX idx_cp_client_type (client_id, type, effective_from, effective_to),
    INDEX idx_cp_contractor_type (contractor_id, type, effective_from, effective_to)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
