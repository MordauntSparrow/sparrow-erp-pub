CREATE TABLE IF NOT EXISTS wage_rate_cards (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    role_id INT NULL,
    active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_wagecard_role FOREIGN KEY (role_id)
        REFERENCES roles(id) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Functional unique index (MySQL 8.0.13+ / MariaDB 10.5+): expression must be (( ... ))
CREATE UNIQUE INDEX uq_wage_rate_cards_role_name 
    ON wage_rate_cards ((COALESCE(role_id, 0)), name);

CREATE TABLE IF NOT EXISTS wage_rate_rows (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rate_card_id INT NOT NULL,
    job_type_id INT NOT NULL,
    rate DECIMAL(10,2) NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_wrr_card FOREIGN KEY (rate_card_id)
        REFERENCES wage_rate_cards(id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_wrr_jobtype FOREIGN KEY (job_type_id)
        REFERENCES job_types(id) ON DELETE CASCADE ON UPDATE CASCADE,
    INDEX idx_wrr_card_job_from_to (rate_card_id, job_type_id, effective_from, effective_to),
    INDEX idx_wrr_effective_from (effective_from)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bill_rate_cards (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    client_id INT NULL,
    site_id INT NULL,
    active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_brc_client FOREIGN KEY (client_id)
        REFERENCES clients(id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_brc_site FOREIGN KEY (site_id)
        REFERENCES sites(id) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE UNIQUE INDEX uq_bill_rate_cards_scope_name 
    ON bill_rate_cards ((COALESCE(client_id, 0)), (COALESCE(site_id, 0)), name);

CREATE TABLE IF NOT EXISTS bill_rate_rows (
    id INT AUTO_INCREMENT PRIMARY KEY,
    rate_card_id INT NOT NULL,
    job_type_id INT NOT NULL,
    rate DECIMAL(10,2) NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_brr_card FOREIGN KEY (rate_card_id)
        REFERENCES bill_rate_cards(id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_brr_jobtype FOREIGN KEY (job_type_id)
        REFERENCES job_types(id) ON DELETE CASCADE ON UPDATE CASCADE,
    INDEX idx_brr_card_job_from_to (rate_card_id, job_type_id, effective_from, effective_to),
    INDEX idx_brr_effective_from (effective_from)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS contractor_client_overrides (
    id INT AUTO_INCREMENT PRIMARY KEY,
    contractor_id INT NOT NULL,
    client_id INT NOT NULL,
    job_type_id INT NULL,
    wage_rate_override DECIMAL(10,2) NOT NULL,
    margin_policy ENUM('allow','warn','enforce') NOT NULL DEFAULT 'warn',
    min_margin_override DECIMAL(10,2) NULL,
    effective_from DATE NOT NULL,
    effective_to DATE NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_cco_contractor FOREIGN KEY (contractor_id)
        REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_cco_client FOREIGN KEY (client_id)
        REFERENCES clients(id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_cco_jobtype FOREIGN KEY (job_type_id)
        REFERENCES job_types(id) ON DELETE SET NULL ON UPDATE CASCADE,
    INDEX idx_cco_lookup (contractor_id, client_id, job_type_id),
    INDEX idx_cco_effective_range (effective_from, effective_to),
    INDEX idx_cco_effective (contractor_id, client_id, job_type_id, effective_from)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
