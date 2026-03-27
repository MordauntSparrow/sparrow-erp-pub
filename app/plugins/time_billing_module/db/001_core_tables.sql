CREATE TABLE IF NOT EXISTS roles (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    code VARCHAR(50) UNIQUE,
    active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_roles_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS job_types (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    code VARCHAR(80) UNIQUE,
    active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS clients (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    external_ref VARCHAR(255) NULL,
    active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS sites (
    id INT AUTO_INCREMENT PRIMARY KEY,
    client_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    postcode VARCHAR(32),
    active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_sites_client FOREIGN KEY (client_id)
        REFERENCES clients(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS tb_contractors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) UNIQUE,
    initials VARCHAR(32) UNIQUE,
    name VARCHAR(255),
    role_id INT NULL,
    wage_rate_card_id INT NULL,
    wage_rate_override DECIMAL(10,2) NULL,
    status ENUM('active','inactive') NOT NULL DEFAULT 'active',
    password_hash VARCHAR(255) NULL,
    view_mode ENUM('form','grid') NOT NULL DEFAULT 'form',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_contractor_role FOREIGN KEY (role_id)
        REFERENCES roles(id) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS tb_contractor_roles (
    contractor_id INT NOT NULL,
    role_id INT NOT NULL,
    PRIMARY KEY (contractor_id, role_id),
    CONSTRAINT fk_cr_contractor FOREIGN KEY (contractor_id)
        REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_cr_role FOREIGN KEY (role_id)
        REFERENCES roles(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_cr_contractor ON tb_contractor_roles (contractor_id);
CREATE INDEX idx_cr_role       ON tb_contractor_roles (role_id);
CREATE INDEX idx_contractor_status_role ON tb_contractors (status, role_id);
CREATE INDEX idx_contractor_email       ON tb_contractors (email);

CREATE TABLE IF NOT EXISTS tb_timesheet_weeks (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    week_id CHAR(6) NOT NULL,
    week_ending DATE NOT NULL,
    status ENUM('draft','submitted','approved','rejected') NOT NULL DEFAULT 'draft',
    submitted_at DATETIME NULL,
    submitted_by INT NULL,
    approved_at DATETIME NULL,
    approved_by INT NULL,
    rejected_at DATETIME NULL,
    rejected_by INT NULL,
    rejection_reason TEXT NULL,
    total_hours DECIMAL(10,4) NOT NULL DEFAULT 0,
    total_pay DECIMAL(10,2) NOT NULL DEFAULT 0,
    total_travel DECIMAL(10,2) NOT NULL DEFAULT 0,
    total_lateness_mins INT NOT NULL DEFAULT 0,
    total_overrun_mins INT NOT NULL DEFAULT 0,
    breakdown_json JSON NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uq_user_week (user_id, week_id),
    INDEX idx_week (week_id),
    INDEX idx_user_status (user_id, status),
    INDEX idx_status (status),
    CONSTRAINT fk_week_user FOREIGN KEY (user_id)
        REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS tb_timesheet_entries (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    week_id BIGINT NOT NULL,
    user_id INT NOT NULL,
    client_id INT NULL,
    site_id INT NULL,
    job_type_id INT NOT NULL,
    work_date DATE NOT NULL,
    scheduled_start TIME NOT NULL,
    scheduled_end   TIME NOT NULL,
    actual_start    TIME NOT NULL,
    actual_end      TIME NOT NULL,
    break_mins INT NOT NULL DEFAULT 0,
    travel_parking DECIMAL(10,2) NOT NULL DEFAULT 0,
    notes TEXT,
    source ENUM('manual','runsheet','scheduler') NOT NULL DEFAULT 'manual',
    runsheet_id BIGINT NULL,
    lock_job_client TINYINT(1) NOT NULL DEFAULT 0,
    scheduled_hours DECIMAL(10,4) NOT NULL DEFAULT 0,
    actual_hours    DECIMAL(10,4) NOT NULL DEFAULT 0,
    labour_hours    DECIMAL(10,4) NOT NULL DEFAULT 0,
    wage_rate_used  DECIMAL(10,2) NOT NULL DEFAULT 0,
    pay             DECIMAL(10,2) NOT NULL DEFAULT 0,
    lateness_mins   INT NOT NULL DEFAULT 0,
    overrun_mins    INT NOT NULL DEFAULT 0,
    variance_mins   INT NOT NULL DEFAULT 0,
    policy_applied TEXT NULL,
    policy_source VARCHAR(32) NULL,
    rate_overridden TINYINT(1) NOT NULL DEFAULT 0,
    edited_by INT NULL,
    edited_at DATETIME NULL,
    edit_reason VARCHAR(255) NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_week_user_date (week_id, user_id, work_date),
    INDEX idx_user_date      (user_id, work_date),
    INDEX idx_week_only      (week_id),
    INDEX idx_job_type       (job_type_id),
    CONSTRAINT fk_entry_week FOREIGN KEY (week_id)
        REFERENCES tb_timesheet_weeks(id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_entry_user FOREIGN KEY (user_id)
        REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_entry_client FOREIGN KEY (client_id)
        REFERENCES clients(id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_entry_site FOREIGN KEY (site_id)
        REFERENCES sites(id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_entry_jobtype FOREIGN KEY (job_type_id)
        REFERENCES job_types(id) ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
