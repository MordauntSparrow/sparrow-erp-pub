-- Allow ad-hoc / emergency run sheets without a directory client; optional free-text site.
-- Template flag: runsheet_templates.allow_free_text_client_site

ALTER TABLE runsheet_templates
  ADD COLUMN allow_free_text_client_site TINYINT(1) NOT NULL DEFAULT 0
    COMMENT '1 = contractor may enter client/site as free text instead of directory IDs'
  AFTER version;

ALTER TABLE runsheets
  DROP FOREIGN KEY fk_rs_client,
  MODIFY COLUMN client_id INT NULL,
  ADD COLUMN client_free_text VARCHAR(255) NULL
    COMMENT 'When client_id is NULL, display/billing may use this label'
    AFTER client_id,
  ADD COLUMN site_free_text VARCHAR(255) NULL
    COMMENT 'Optional free-text site when not using sites.id'
    AFTER site_id;

ALTER TABLE runsheets
  ADD CONSTRAINT fk_rs_client FOREIGN KEY (client_id) REFERENCES clients (id);
