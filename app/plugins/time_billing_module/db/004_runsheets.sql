CREATE TABLE IF NOT EXISTS runsheets (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    client_id INT NOT NULL,
    site_id INT NULL,
    job_type_id INT NOT NULL,
    work_date DATE NOT NULL,
    window_start TIME NULL,
    window_end TIME NULL,
    template_id INT NULL,
    template_version INT NULL,
    payload_json JSON NULL,
    mapping_json JSON NULL,
    lead_user_id INT NULL,
    status ENUM('draft','submitted','approved','published') NOT NULL DEFAULT 'draft',
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    CONSTRAINT fk_rs_client  FOREIGN KEY (client_id)     REFERENCES clients(id)        ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_rs_site    FOREIGN KEY (site_id)       REFERENCES sites(id)          ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_rs_jobtype FOREIGN KEY (job_type_id)  REFERENCES job_types(id)      ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT fk_rs_lead    FOREIGN KEY (lead_user_id) REFERENCES tb_contractors(id) ON DELETE SET NULL ON UPDATE CASCADE,

    INDEX idx_rs_date (work_date),
    INDEX idx_rs_client (client_id),
    INDEX idx_rs_site (site_id),
    INDEX idx_rs_job_type (job_type_id),
    INDEX idx_rs_status (status),
    INDEX idx_rs_client_date (client_id, work_date),
    INDEX idx_rs_site_date (site_id, work_date),
    INDEX idx_rs_job_date (job_type_id, work_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS runsheet_assignments (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    runsheet_id BIGINT NOT NULL,
    user_id INT NOT NULL,
    scheduled_start TIME NULL,
    scheduled_end   TIME NULL,
    actual_start    TIME NULL,
    actual_end      TIME NULL,
    break_mins INT NOT NULL DEFAULT 0,
    travel_parking DECIMAL(10,2) NOT NULL DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    CONSTRAINT fk_rsa_rs   FOREIGN KEY (runsheet_id) REFERENCES runsheets(id)      ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_rsa_user FOREIGN KEY (user_id)     REFERENCES tb_contractors(id) ON DELETE RESTRICT ON UPDATE CASCADE,

    UNIQUE KEY uq_rsa_runsheet_user (runsheet_id, user_id),
    INDEX idx_rsa_runsheet (runsheet_id),
    INDEX idx_rsa_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
