
CREATE TABLE IF NOT EXISTS runsheet_templates (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    code VARCHAR(80) UNIQUE,
    job_type_id INT NULL,
    client_id INT NULL,
    site_id INT NULL,
    active TINYINT(1) NOT NULL DEFAULT 1,
    version INT NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    CONSTRAINT fk_rst_jobtype FOREIGN KEY (job_type_id)
        REFERENCES job_types(id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_rst_client FOREIGN KEY (client_id)
        REFERENCES clients(id) ON DELETE SET NULL ON UPDATE CASCADE,
    CONSTRAINT fk_rst_site FOREIGN KEY (site_id)
        REFERENCES sites(id) ON DELETE SET NULL ON UPDATE CASCADE,
    INDEX idx_rst_active (active),
    INDEX idx_rst_job (job_type_id),
    INDEX idx_rst_client (client_id),
    INDEX idx_rst_site (site_id),
    INDEX idx_rst_scope (job_type_id, client_id, site_id),
    UNIQUE KEY uq_rst_scope_name (job_type_id, client_id, site_id, name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS runsheet_template_fields (
    id INT AUTO_INCREMENT PRIMARY KEY,
    template_id INT NOT NULL,
    name VARCHAR(100) NOT NULL,   
    label VARCHAR(200) NOT NULL, 
    type ENUM(
        'text',
        'number',
        'date',
        'time',
        'datetime',
        'select',
        'multiselect',
        'checkbox',
        'textarea'
    ) NOT NULL,
    required TINYINT(1) NOT NULL DEFAULT 0,
    order_index INT NOT NULL DEFAULT 0,
    placeholder VARCHAR(255) NULL,
    help_text VARCHAR(255) NULL,
    options_json JSON NULL,       
    validation_json JSON NULL,    
    visible_if_json JSON NULL,   
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_rstf_template FOREIGN KEY (template_id)
        REFERENCES runsheet_templates(id) ON DELETE CASCADE ON UPDATE CASCADE,
    UNIQUE KEY uq_rstf_template_name (template_id, name),
    INDEX idx_rstf_template_order (template_id, order_index)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


CREATE TABLE IF NOT EXISTS runsheet_template_pdf (
    template_id INT PRIMARY KEY,
    html MEDIUMTEXT NULL, 
    css MEDIUMTEXT NULL,  
    version INT NOT NULL DEFAULT 1,
    CONSTRAINT fk_rstpdf_template FOREIGN KEY (template_id)
        REFERENCES runsheet_templates(id) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
