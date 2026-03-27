-- Default client_id and job_type_id for runsheets created from Ventus sign-on.
-- Admin can set these so Response sign-on creates runsheets against the right client/job.
CREATE TABLE IF NOT EXISTS ventus_integration_defaults (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  client_id INT NOT NULL,
  job_type_id INT NOT NULL,
  site_id INT DEFAULT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY fk_vid_client (client_id),
  KEY fk_vid_jobtype (job_type_id),
  KEY fk_vid_site (site_id),
  CONSTRAINT fk_vid_client FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE,
  CONSTRAINT fk_vid_jobtype FOREIGN KEY (job_type_id) REFERENCES job_types(id) ON DELETE CASCADE,
  CONSTRAINT fk_vid_site FOREIGN KEY (site_id) REFERENCES sites(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
