-- db/init_medical_records.sql

-- Create patients table
CREATE TABLE IF NOT EXISTS patients (
    id SERIAL PRIMARY KEY,
    nhs_number VARCHAR(20) UNIQUE NOT NULL,

    first_name VARCHAR(100) NOT NULL,
    middle_name VARCHAR(100),
    last_name VARCHAR(100) NOT NULL,
    date_of_birth DATE,
    gender VARCHAR(20),
    weight NUMERIC(5,2),
    height NUMERIC(5,2),
    blood_type VARCHAR(5),
    primary_language VARCHAR(50),
    address TEXT,
    postcode VARCHAR(20),
    gp_details TEXT,
    medical_conditions TEXT,
    allergies TEXT,
    medications TEXT,
    previous_visit_record INTEGER,
    package_type VARCHAR(50),
    notes TEXT,
    message_log TEXT,
    keysafe_number VARCHAR(50),
    door_code VARCHAR(50),
    payment_method VARCHAR(50),
    payment_details TEXT,
    invoice_email VARCHAR(100),
    invoice_phone VARCHAR(50),
    emergency_contact_name VARCHAR(100),
    emergency_contact_relationship VARCHAR(50),
    emergency_contact_phone VARCHAR(50),
    emergency_contact_address TEXT,
    next_of_kin_details TEXT,
    lpa_details TEXT,
    resuscitation_status VARCHAR(50),
    immunisation_status TEXT,
    health_alerts TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create audit_logs table
CREATE TABLE IF NOT EXISTS audit_logs (
    id SERIAL PRIMARY KEY,
    user VARCHAR(100) NOT NULL,
    principal_role VARCHAR(32) NULL,
    action TEXT NOT NULL,
    case_id BIGINT NULL,
    patient_id INTEGER,
    route VARCHAR(180) NULL,
    ip VARCHAR(64) NULL,
    user_agent VARCHAR(255) NULL,
    reason TEXT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create prescriptions table
CREATE TABLE IF NOT EXISTS prescriptions (
    id SERIAL PRIMARY KEY,
    patient_id INTEGER NOT NULL,
    prescribed_by VARCHAR(100) NOT NULL,
    prescription TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (patient_id) REFERENCES patients(id)
);

-- Create care_company_users table
CREATE TABLE IF NOT EXISTS care_company_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password_hash VARCHAR(128) NOT NULL,
    company_name VARCHAR(150) NOT NULL,
    contact_phone VARCHAR(20),
    contact_address TEXT
);

CREATE TABLE cases (
  id BIGINT PRIMARY KEY,
  data JSON NOT NULL,         -- or LONGTEXT if JSON isn't supported
  status VARCHAR(50) NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  closed_at DATETIME NULL,
  dispatch_reference VARCHAR(64) NULL,
  primary_callsign VARCHAR(64) NULL,
  dispatch_synced_at DATETIME NULL,
  record_version INT NOT NULL DEFAULT 1,
  idempotency_key VARCHAR(128) NULL,
  close_idempotency_key VARCHAR(128) NULL,
  patient_match_meta JSON NULL,
  UNIQUE KEY uq_cases_idempotency_key (idempotency_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;