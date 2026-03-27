-- Links tb_contractors to Ventus callSign for sign-on/off → runsheet & attendance integration.
CREATE TABLE IF NOT EXISTS contractor_ventus_mapping (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  ventus_callsign VARCHAR(64) NOT NULL,
  ventus_division VARCHAR(64) DEFAULT 'general',
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_contractor_ventus (contractor_id),
  UNIQUE KEY uq_ventus_callsign (ventus_callsign),
  KEY idx_ventus_callsign (ventus_callsign),
  CONSTRAINT fk_cvm_contractor FOREIGN KEY (contractor_id)
    REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
