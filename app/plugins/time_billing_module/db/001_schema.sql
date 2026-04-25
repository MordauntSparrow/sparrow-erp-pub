-- ========================
-- 1. roles
-- ========================
CREATE TABLE IF NOT EXISTS roles (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  code VARCHAR(50) DEFAULT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY code (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 2. job_types
-- ========================
CREATE TABLE IF NOT EXISTS job_types (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(150) NOT NULL,
  code VARCHAR(80) DEFAULT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  colour_hex VARCHAR(7) DEFAULT NULL COMMENT 'Hex e.g. #3366cc for badges/rows',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY code (code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 3. clients
-- ========================
CREATE TABLE IF NOT EXISTS clients (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  external_ref VARCHAR(255) DEFAULT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 4. sites
-- ========================
CREATE TABLE IF NOT EXISTS sites (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  client_id INT NOT NULL,
  name VARCHAR(255) NOT NULL,
  postcode VARCHAR(32) DEFAULT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY fk_sites_client (client_id),
  CONSTRAINT fk_sites_client FOREIGN KEY (client_id)
    REFERENCES clients(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 5. tb_contractors
-- ========================
CREATE TABLE IF NOT EXISTS tb_contractors (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  email VARCHAR(255) DEFAULT NULL,
  initials VARCHAR(32) DEFAULT NULL,
  name VARCHAR(255) DEFAULT NULL,
  role_id INT DEFAULT NULL,
  wage_rate_card_id INT DEFAULT NULL,
  wage_rate_override DECIMAL(10,2) DEFAULT NULL,
  status ENUM('active','inactive') NOT NULL DEFAULT 'active',
  password_hash VARCHAR(255) DEFAULT NULL,
  view_mode ENUM('form','grid') NOT NULL DEFAULT 'form',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY email (email),
  UNIQUE KEY initials (initials),
  KEY fk_contractor_role (role_id),
  CONSTRAINT fk_contractor_role FOREIGN KEY (role_id)
    REFERENCES roles(id) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 6. tb_contractor_roles
-- ========================
CREATE TABLE IF NOT EXISTS tb_contractor_roles (
  contractor_id INT NOT NULL,
  role_id INT NOT NULL,
  PRIMARY KEY (contractor_id, role_id),
  KEY idx_cr_contractor (contractor_id),
  KEY idx_cr_role (role_id),
  CONSTRAINT fk_cr_contractor FOREIGN KEY (contractor_id)
    REFERENCES tb_contractors(id) ON DELETE CASCADE,
  CONSTRAINT fk_cr_role FOREIGN KEY (role_id)
    REFERENCES roles(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 7. wage_rate_cards
-- ========================
CREATE TABLE IF NOT EXISTS wage_rate_cards (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(150) NOT NULL,
  role_id INT DEFAULT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY fk_wagecard_role (role_id),
  CONSTRAINT fk_wagecard_role FOREIGN KEY (role_id)
    REFERENCES roles(id) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 8. wage_rate_rows
-- ========================
CREATE TABLE IF NOT EXISTS wage_rate_rows (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  rate_card_id INT NOT NULL,
  job_type_id INT NOT NULL,
  rate DECIMAL(10,2) NOT NULL,
  effective_from DATE NOT NULL,
  effective_to DATE DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_wrr (rate_card_id, job_type_id, effective_from),
  KEY fk_wrr_jobtype (job_type_id),
  KEY idx_wrr_card_job_from (rate_card_id, job_type_id, effective_from),
  CONSTRAINT fk_wrr_card FOREIGN KEY (rate_card_id)
    REFERENCES wage_rate_cards(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_wrr_jobtype FOREIGN KEY (job_type_id)
    REFERENCES job_types(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 9. bill_rate_cards
-- ========================
CREATE TABLE IF NOT EXISTS bill_rate_cards (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(150) NOT NULL,
  client_id INT DEFAULT NULL,
  site_id INT DEFAULT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY fk_brc_client (client_id),
  KEY fk_brc_site (site_id),
  CONSTRAINT fk_brc_client FOREIGN KEY (client_id)
    REFERENCES clients(id) ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT fk_brc_site FOREIGN KEY (site_id)
    REFERENCES sites(id) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 10. bill_rate_rows
-- ========================
CREATE TABLE IF NOT EXISTS bill_rate_rows (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  rate_card_id INT NOT NULL,
  job_type_id INT NOT NULL,
  rate DECIMAL(10,2) NOT NULL,
  effective_from DATE NOT NULL,
  effective_to DATE DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY fk_brr_jobtype (job_type_id),
  KEY idx_brr_card_job_from (rate_card_id, job_type_id, effective_from),
  CONSTRAINT fk_brr_card FOREIGN KEY (rate_card_id)
    REFERENCES bill_rate_cards(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_brr_jobtype FOREIGN KEY (job_type_id)
    REFERENCES job_types(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 11. contractor_client_overrides
-- ========================
CREATE TABLE IF NOT EXISTS contractor_client_overrides (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  client_id INT NOT NULL,
  job_type_id INT DEFAULT NULL,
  wage_rate_override DECIMAL(10,2) NOT NULL,
  margin_policy ENUM('allow','warn','enforce') NOT NULL DEFAULT 'warn',
  min_margin_override DECIMAL(10,2) DEFAULT NULL,
  effective_from DATE NOT NULL,
  effective_to DATE DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY fk_cco_client (client_id),
  KEY fk_cco_jobtype (job_type_id),
  KEY idx_cco_effective (contractor_id, client_id, job_type_id, effective_from),
  CONSTRAINT fk_cco_client FOREIGN KEY (client_id)
    REFERENCES clients(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_cco_contractor FOREIGN KEY (contractor_id)
    REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_cco_jobtype FOREIGN KEY (job_type_id)
    REFERENCES job_types(id) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 12. tb_timesheet_weeks
-- ========================
CREATE TABLE IF NOT EXISTS tb_timesheet_weeks (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  user_id INT NOT NULL,
  week_id CHAR(6) NOT NULL,
  week_ending DATE NOT NULL,
  status ENUM('draft','submitted','approved','rejected') NOT NULL DEFAULT 'draft',
  submitted_at DATETIME DEFAULT NULL,
  submitted_by INT DEFAULT NULL,
  approved_at DATETIME DEFAULT NULL,
  approved_by INT DEFAULT NULL,
  rejected_at DATETIME DEFAULT NULL,
  rejected_by INT DEFAULT NULL,
  rejection_reason TEXT,
  total_hours DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
  total_pay DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  total_travel DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  total_lateness_mins INT NOT NULL DEFAULT 0,
  total_overrun_mins INT NOT NULL DEFAULT 0,
  breakdown_json JSON DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_user_week (user_id, week_id),
  KEY idx_week (week_id),
  KEY idx_tsw_user_status (user_id, status),
  CONSTRAINT fk_week_user FOREIGN KEY (user_id)
    REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 13. tb_timesheet_entries
-- ========================
CREATE TABLE IF NOT EXISTS tb_timesheet_entries (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  week_id BIGINT NOT NULL,
  user_id INT NOT NULL,
  client_name VARCHAR(255) DEFAULT NULL,
  site_name VARCHAR(255) DEFAULT NULL,
  job_type_id INT NOT NULL,
  work_date DATE NOT NULL,
  scheduled_start TIME NOT NULL,
  scheduled_end TIME NOT NULL,
  actual_start TIME NOT NULL,
  actual_end TIME NOT NULL,
  break_mins INT NOT NULL DEFAULT 0,
  travel_parking DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  notes TEXT,
  source ENUM('manual','runsheet','scheduler') NOT NULL DEFAULT 'manual',
  runsheet_id BIGINT DEFAULT NULL,
  lock_job_client TINYINT(1) NOT NULL DEFAULT 0,
  scheduled_hours DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
  actual_hours DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
  labour_hours DECIMAL(10,4) NOT NULL DEFAULT 0.0000,
  wage_rate_used DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  pay DECIMAL(10,2) NOT NULL DEFAULT 0.00,
  lateness_mins INT NOT NULL DEFAULT 0,
  overrun_mins INT NOT NULL DEFAULT 0,
  variance_mins INT NOT NULL DEFAULT 0,
  policy_applied TEXT,
  policy_source VARCHAR(32) DEFAULT NULL,
  rate_overridden TINYINT(1) NOT NULL DEFAULT 0,
  edited_by INT DEFAULT NULL,
  edited_at DATETIME DEFAULT NULL,
  edit_reason VARCHAR(255) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_week_user_date (week_id, user_id, work_date),
  KEY fk_entry_user (user_id),
  KEY fk_entry_site (site_name),
  KEY fk_entry_jobtype (job_type_id),
  KEY idx_tse_source (source),
  KEY idx_tse_client (client_name),
  CONSTRAINT fk_entry_jobtype FOREIGN KEY (job_type_id)
    REFERENCES job_types(id) ON DELETE RESTRICT ON UPDATE CASCADE,
  CONSTRAINT fk_entry_user FOREIGN KEY (user_id)
    REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_entry_week FOREIGN KEY (week_id)
    REFERENCES tb_timesheet_weeks(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 14. bank_holidays
-- ========================
CREATE TABLE IF NOT EXISTS bank_holidays (
  date DATE NOT NULL,
  region VARCHAR(100) DEFAULT NULL,
  name VARCHAR(255) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
-- ========================
-- 15. calendar_policies
-- ========================
CREATE TABLE IF NOT EXISTS calendar_policies (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(150) NOT NULL,
  type ENUM('WEEKEND','BANK_HOLIDAY','NIGHT','OVERTIME_SHIFT','OVERTIME_DAILY','OVERTIME_WEEKLY','MINIMUM_HOURS_PER_SHIFT','MINIMUM_HOURS_DAILY_GLOBAL','MINIMUM_HOURS_DAILY_CLIENT','MINIMUM_HOURS_DAILY_CONTRACTOR_CLIENT') NOT NULL,
  scope ENUM('GLOBAL','ROLE','JOB_TYPE','CLIENT','CONTRACTOR_CLIENT') NOT NULL,
  role_id INT DEFAULT NULL,
  job_type_id INT DEFAULT NULL,
  client_id INT DEFAULT NULL,
  contractor_id INT DEFAULT NULL,
  mode ENUM('OFF','MULTIPLIER','ABSOLUTE') NOT NULL DEFAULT 'OFF',
  multiplier DECIMAL(5,2) DEFAULT NULL,
  absolute_rate DECIMAL(10,2) DEFAULT NULL,
  window_start TIME DEFAULT NULL,
  window_end TIME DEFAULT NULL,
  ot_threshold_hours DECIMAL(5,2) DEFAULT NULL,
  ot_tier2_threshold_hours DECIMAL(5,2) DEFAULT NULL,
  ot_tier1_mult DECIMAL(5,2) DEFAULT NULL,
  ot_tier2_mult DECIMAL(5,2) DEFAULT NULL,
  minimum_hours DECIMAL(5,2) DEFAULT NULL,
  applies_to ENUM('WAGE','BILL','BOTH') NOT NULL DEFAULT 'WAGE',
  stacking ENUM('NONE','OT_ON_TOP','FULL') NOT NULL DEFAULT 'OT_ON_TOP',
  effective_from DATE NOT NULL,
  effective_to DATE DEFAULT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY fk_cp_role (role_id),
  KEY fk_cp_jobtype (job_type_id),
  KEY fk_cp_client (client_id),
  KEY fk_cp_contractor (contractor_id),
  KEY idx_cp_effective (effective_from, effective_to, active),
  KEY idx_cp_scope (scope, type),
  KEY idx_cp_type_scope (type, scope, active, effective_from),
  CONSTRAINT fk_cp_client FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_cp_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_cp_jobtype FOREIGN KEY (job_type_id) REFERENCES job_types(id) ON DELETE SET NULL ON UPDATE CASCADE,
  CONSTRAINT fk_cp_role FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE SET NULL ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 16. runsheets
-- ========================
CREATE TABLE IF NOT EXISTS runsheets (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  client_id INT NOT NULL,
  site_id INT DEFAULT NULL,
  job_type_id INT NOT NULL,
  work_date DATE NOT NULL,
  window_start TIME DEFAULT NULL,
  window_end TIME DEFAULT NULL,
  template_id INT DEFAULT NULL,
  template_version INT DEFAULT NULL,
  payload_json JSON DEFAULT NULL,
  mapping_json JSON DEFAULT NULL,
  lead_user_id INT DEFAULT NULL,
  status ENUM('draft','approved','published') DEFAULT 'draft',
  notes TEXT,
  created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY fk_rs_site (site_id),
  KEY fk_rs_jobtype (job_type_id),
  KEY fk_rs_lead (lead_user_id),
  KEY idx_runsheets_client_date (client_id, work_date),
  CONSTRAINT fk_rs_client FOREIGN KEY (client_id) REFERENCES clients(id),
  CONSTRAINT fk_rs_jobtype FOREIGN KEY (job_type_id) REFERENCES job_types(id),
  CONSTRAINT fk_rs_lead FOREIGN KEY (lead_user_id) REFERENCES tb_contractors(id),
  CONSTRAINT fk_rs_site FOREIGN KEY (site_id) REFERENCES sites(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 17. runsheet_templates
-- ========================
CREATE TABLE IF NOT EXISTS runsheet_templates (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(150) NOT NULL,
  code VARCHAR(80) DEFAULT NULL,
  job_type_id INT DEFAULT NULL,
  client_id INT DEFAULT NULL,
  site_id INT DEFAULT NULL,
  active TINYINT(1) DEFAULT 1,
  version INT DEFAULT 1,
  created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY code (code),
  KEY fk_rst_jobtype (job_type_id),
  KEY fk_rst_client (client_id),
  KEY fk_rst_site (site_id),
  CONSTRAINT fk_rst_client FOREIGN KEY (client_id) REFERENCES clients(id),
  CONSTRAINT fk_rst_jobtype FOREIGN KEY (job_type_id) REFERENCES job_types(id),
  CONSTRAINT fk_rst_site FOREIGN KEY (site_id) REFERENCES sites(id)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 18. runsheet_template_fields
-- ========================
CREATE TABLE IF NOT EXISTS runsheet_template_fields (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  template_id INT NOT NULL,
  name VARCHAR(100) NOT NULL,
  label VARCHAR(200) NOT NULL,
  type ENUM('text','number','date','time','datetime','select','multiselect','checkbox','textarea') NOT NULL,
  required TINYINT(1) DEFAULT 0,
  order_index INT DEFAULT 0,
  placeholder VARCHAR(255) DEFAULT NULL,
  help_text VARCHAR(255) DEFAULT NULL,
  options_json JSON DEFAULT NULL,
  validation_json JSON DEFAULT NULL,
  visible_if_json JSON DEFAULT NULL,
  created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  KEY fk_rstf_template (template_id),
  CONSTRAINT fk_rstf_template FOREIGN KEY (template_id) REFERENCES runsheet_templates(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 19. runsheet_template_pdf
-- ========================
CREATE TABLE IF NOT EXISTS runsheet_template_pdf (
  template_id INT NOT NULL PRIMARY KEY,
  html MEDIUMTEXT,
  css MEDIUMTEXT,
  version INT DEFAULT 1,
  CONSTRAINT fk_rstpdf_template FOREIGN KEY (template_id) REFERENCES runsheet_templates(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 20. runsheet_assignments
-- ========================
CREATE TABLE IF NOT EXISTS runsheet_assignments (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  runsheet_id BIGINT NOT NULL,
  user_id INT NOT NULL,
  scheduled_start TIME DEFAULT NULL,
  scheduled_end TIME DEFAULT NULL,
  actual_start TIME DEFAULT NULL,
  actual_end TIME DEFAULT NULL,
  break_mins INT DEFAULT 0,
  travel_parking DECIMAL(10,2) DEFAULT 0.00,
  notes TEXT,
  created_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY fk_rsa_rs (runsheet_id),
  KEY fk_rsa_user (user_id),
  CONSTRAINT fk_rsa_rs FOREIGN KEY (runsheet_id) REFERENCES runsheets(id) ON DELETE CASCADE,
  CONSTRAINT fk_rsa_user FOREIGN KEY (user_id) REFERENCES tb_contractors(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 21. tb_time_billing_migrations
-- ========================
CREATE TABLE IF NOT EXISTS tb_time_billing_migrations (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
