-- Employee Portal: messages and todos for the contractor/employee dashboard.
-- Other modules (HR, Compliance, Training, etc.) can insert into these tables.

-- ========================
-- 1. ep_migrations (ledger for this module's migrations)
-- ========================
CREATE TABLE IF NOT EXISTS ep_migrations (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  filename VARCHAR(255) NOT NULL UNIQUE,
  applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 2. ep_messages
-- ========================
CREATE TABLE IF NOT EXISTS ep_messages (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  source_module VARCHAR(64) NOT NULL COMMENT 'e.g. hr_module, compliance_module',
  subject VARCHAR(255) NOT NULL,
  body TEXT,
  read_at DATETIME DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ep_messages_contractor (contractor_id),
  KEY idx_ep_messages_read (contractor_id, read_at),
  CONSTRAINT fk_ep_messages_contractor FOREIGN KEY (contractor_id)
    REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- ========================
-- 3. ep_todos
-- ========================
CREATE TABLE IF NOT EXISTS ep_todos (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  contractor_id INT NOT NULL,
  source_module VARCHAR(64) NOT NULL COMMENT 'e.g. hr_module, compliance_module, training_module',
  title VARCHAR(255) NOT NULL,
  link_url VARCHAR(512) DEFAULT NULL,
  due_date DATE DEFAULT NULL,
  completed_at DATETIME DEFAULT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_ep_todos_contractor (contractor_id),
  KEY idx_ep_todos_pending (contractor_id, completed_at),
  CONSTRAINT fk_ep_todos_contractor FOREIGN KEY (contractor_id)
    REFERENCES tb_contractors(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
