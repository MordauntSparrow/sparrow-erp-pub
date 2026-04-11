-- Work module: client visit record (1:1 with schedule_shifts) + work_photos.visit_id
-- Prefer: python -m app.plugins.work_module.install upgrade

CREATE TABLE IF NOT EXISTS work_visits (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  schedule_shift_id BIGINT NOT NULL,
  client_id INT NOT NULL,
  site_id INT DEFAULT NULL,
  job_type_id INT NOT NULL,
  contractor_id INT NOT NULL,
  work_date DATE NOT NULL,
  runsheet_assignment_id BIGINT DEFAULT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'open',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_wv_shift (schedule_shift_id),
  KEY idx_wv_client_date (client_id, work_date),
  KEY idx_wv_contractor_date (contractor_id, work_date),
  CONSTRAINT fk_wv_shift FOREIGN KEY (schedule_shift_id) REFERENCES schedule_shifts(id) ON DELETE CASCADE,
  CONSTRAINT fk_wv_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Run only if visit_id is missing:
-- ALTER TABLE work_photos ADD COLUMN visit_id BIGINT DEFAULT NULL AFTER shift_id, ADD KEY idx_wp_visit (visit_id),
--   ADD CONSTRAINT fk_wp_visit FOREIGN KEY (visit_id) REFERENCES work_visits(id) ON DELETE SET NULL;
