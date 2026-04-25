CREATE TABLE response_triage (
  id INT AUTO_INCREMENT PRIMARY KEY,
  created_by VARCHAR(255) NOT NULL,
  vita_record_id INT DEFAULT NULL,
  first_name VARCHAR(255) NOT NULL,
  middle_name VARCHAR(255) DEFAULT NULL,
  last_name VARCHAR(255) NOT NULL,
  patient_dob DATE DEFAULT NULL,
  phone_number VARCHAR(50) DEFAULT NULL,
  address VARCHAR(255) DEFAULT NULL,
  postcode VARCHAR(50) DEFAULT NULL,
  entry_requirements JSON DEFAULT NULL,
  reason_for_call TEXT DEFAULT NULL,
  onset_datetime DATETIME DEFAULT NULL,
  patient_alone ENUM('yes','no') DEFAULT 'yes',
  exclusion_data JSON DEFAULT NULL,
  risk_flags JSON DEFAULT NULL,
  decision VARCHAR(50) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB;

