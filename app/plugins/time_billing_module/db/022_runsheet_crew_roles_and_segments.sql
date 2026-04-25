-- Intuitive crew roles on assignments + optional journey segments (multiple drivers / roles over time).
-- Pay modes are stored for future payroll rules; 'audit' does not change timesheet publish behaviour today.

ALTER TABLE runsheet_assignments
  ADD COLUMN crew_role VARCHAR(64) NULL DEFAULT NULL
  COMMENT 'Primary role for this crew row e.g. driver, lead, client_contact'
  AFTER notes;

CREATE TABLE IF NOT EXISTS tb_runsheet_crew_segments (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  runsheet_id BIGINT NOT NULL,
  contractor_id INT NOT NULL,
  role_code VARCHAR(32) NOT NULL,
  role_label VARCHAR(120) NULL,
  time_start TIME NULL,
  time_end TIME NULL,
  pay_mode VARCHAR(24) NOT NULL DEFAULT 'audit',
  notes VARCHAR(500) NULL,
  sort_order INT NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_rsc_seg_runsheet (runsheet_id),
  KEY idx_rsc_seg_contractor (contractor_id),
  CONSTRAINT fk_rsc_seg_rs FOREIGN KEY (runsheet_id) REFERENCES runsheets(id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_rsc_seg_user FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id)
    ON DELETE RESTRICT ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
