-- Scheduling module: Sling-level shift scheduling, availability, labour.
-- Depends on time_billing_module (tb_contractors, clients, sites, job_types).

-- ========================
-- 1. Migrations ledger
-- ========================
CREATE TABLE IF NOT EXISTS schedule_migrations (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 2. schedule_shifts
-- ========================
CREATE TABLE IF NOT EXISTS schedule_shifts (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  client_id INT NOT NULL,
  site_id INT DEFAULT NULL,
  job_type_id INT NOT NULL,
  work_date DATE NOT NULL,
  scheduled_start TIME NOT NULL,
  scheduled_end TIME NOT NULL,
  actual_start TIME DEFAULT NULL,
  actual_end TIME DEFAULT NULL,
  break_mins INT NOT NULL DEFAULT 0,
  notes TEXT,
  status ENUM('draft','published','in_progress','completed','cancelled','no_show') NOT NULL DEFAULT 'draft',
  source ENUM('manual','ventus','scheduler','work_module') NOT NULL DEFAULT 'manual',
  external_id VARCHAR(255) DEFAULT NULL COMMENT 'e.g. ventus callsign, runsheet_id',
  runsheet_id BIGINT DEFAULT NULL,
  runsheet_assignment_id BIGINT DEFAULT NULL,
  labour_cost DECIMAL(10,2) DEFAULT NULL,
  shared_labour_hours DECIMAL(6,2) DEFAULT NULL COMMENT 'Fixed person-hours, duration shrinks as crew or required headcount grows',
  recurrence_id BIGINT DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_ss_contractor_date (contractor_id, work_date),
  KEY idx_ss_date_status (work_date, status),
  KEY idx_ss_client_date (client_id, work_date),
  KEY idx_ss_source (source),
  KEY idx_ss_runsheet (runsheet_id),
  KEY idx_ss_recurrence (recurrence_id),
  CONSTRAINT fk_ss_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE,
  CONSTRAINT fk_ss_client FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE,
  CONSTRAINT fk_ss_site FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE SET NULL,
  CONSTRAINT fk_ss_jobtype FOREIGN KEY (job_type_id) REFERENCES job_types(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 3. schedule_availability
-- ========================
CREATE TABLE IF NOT EXISTS schedule_availability (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  day_of_week TINYINT NOT NULL COMMENT '0=Mon..6=Sun',
  start_time TIME NOT NULL,
  end_time TIME NOT NULL,
  effective_from DATE NOT NULL,
  effective_to DATE DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_sa_contractor (contractor_id),
  KEY idx_sa_dow (contractor_id, day_of_week),
  CONSTRAINT fk_sa_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 4. schedule_time_off
-- ========================
CREATE TABLE IF NOT EXISTS schedule_time_off (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  start_date DATE NOT NULL,
  end_date DATE NOT NULL,
  reason VARCHAR(255) DEFAULT NULL,
  status ENUM('requested','approved','rejected') NOT NULL DEFAULT 'requested',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_sto_contractor (contractor_id),
  KEY idx_sto_dates (start_date, end_date),
  CONSTRAINT fk_sto_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 5. shift_swap_requests
-- ========================
CREATE TABLE IF NOT EXISTS shift_swap_requests (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  shift_id BIGINT NOT NULL,
  requester_contractor_id INT NOT NULL,
  requested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  status ENUM('open','claimed','approved','rejected','cancelled') NOT NULL DEFAULT 'open',
  claimer_contractor_id INT DEFAULT NULL,
  claimed_at DATETIME DEFAULT NULL,
  resolved_at DATETIME DEFAULT NULL,
  resolved_by INT DEFAULT NULL,
  notes TEXT,
  KEY idx_ssr_shift (shift_id),
  KEY idx_ssr_requester (requester_contractor_id),
  KEY idx_ssr_status (status),
  CONSTRAINT fk_ssr_shift FOREIGN KEY (shift_id) REFERENCES schedule_shifts(id) ON DELETE CASCADE,
  CONSTRAINT fk_ssr_requester FOREIGN KEY (requester_contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE,
  CONSTRAINT fk_ssr_claimer FOREIGN KEY (claimer_contractor_id) REFERENCES tb_contractors(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 6. schedule_templates (reusable weekly patterns)
-- ========================
CREATE TABLE IF NOT EXISTS schedule_templates (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(150) NOT NULL,
  client_id INT DEFAULT NULL,
  site_id INT DEFAULT NULL,
  job_type_id INT DEFAULT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY fk_st_client (client_id),
  KEY fk_st_site (site_id),
  KEY fk_st_jobtype (job_type_id),
  CONSTRAINT fk_st_client FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE SET NULL,
  CONSTRAINT fk_st_site FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE SET NULL,
  CONSTRAINT fk_st_jobtype FOREIGN KEY (job_type_id) REFERENCES job_types(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 7. schedule_template_slots
-- ========================
CREATE TABLE IF NOT EXISTS schedule_template_slots (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  template_id INT NOT NULL,
  day_of_week TINYINT NOT NULL,
  start_time TIME NOT NULL,
  end_time TIME NOT NULL,
  position_label VARCHAR(100) DEFAULT NULL,
  KEY fk_sts_template (template_id),
  CONSTRAINT fk_sts_template FOREIGN KEY (template_id) REFERENCES schedule_templates(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
