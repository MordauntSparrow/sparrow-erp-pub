-- Compliance: policies and staff acknowledgements. Uses tb_contractors from time_billing.

CREATE TABLE IF NOT EXISTS compliance_migrations (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS compliance_policies (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  title VARCHAR(255) NOT NULL,
  slug VARCHAR(120) NOT NULL,
  summary TEXT,
  body LONGTEXT,
  version INT NOT NULL DEFAULT 1,
  effective_from DATE NOT NULL,
  effective_to DATE DEFAULT NULL,
  required_acknowledgement TINYINT(1) NOT NULL DEFAULT 1,
  active TINYINT(1) NOT NULL DEFAULT 1,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_slug (slug),
  KEY idx_effective (effective_from, effective_to, active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS compliance_acknowledgements (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  policy_id INT NOT NULL,
  contractor_id INT NOT NULL,
  acknowledged_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ip_address VARCHAR(64) DEFAULT NULL,
  user_agent VARCHAR(255) DEFAULT NULL,
  UNIQUE KEY uq_policy_contractor (policy_id, contractor_id),
  KEY idx_contractor (contractor_id),
  CONSTRAINT fk_ca_policy FOREIGN KEY (policy_id) REFERENCES compliance_policies(id) ON DELETE CASCADE,
  CONSTRAINT fk_ca_contractor FOREIGN KEY (contractor_id) REFERENCES tb_contractors(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
