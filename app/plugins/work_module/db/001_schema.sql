-- Work module: photos and extra notes per shift/stop.
-- Depends on scheduling_module (schedule_shifts) and time_billing (tb_contractors).

CREATE TABLE IF NOT EXISTS work_migrations (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Photos attached to a shift (or runsheet assignment) by the contractor.
CREATE TABLE IF NOT EXISTS work_photos (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  shift_id BIGINT NOT NULL,
  contractor_id INT NOT NULL,
  file_path VARCHAR(512) NOT NULL COMMENT 'Relative path under uploads/work_photos/',
  file_name VARCHAR(255) DEFAULT NULL,
  mime_type VARCHAR(128) DEFAULT NULL,
  caption VARCHAR(500) DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_wp_shift (shift_id),
  KEY idx_wp_contractor (contractor_id),
  CONSTRAINT fk_wp_shift FOREIGN KEY (shift_id) REFERENCES schedule_shifts(id) ON DELETE CASCADE,
  CONSTRAINT fk_wp_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
