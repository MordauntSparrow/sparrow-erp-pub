-- Per-contractor portal UI theme (light / dark / auto). Core path: app.contractor_ui_theme
ALTER TABLE tb_contractors
ADD COLUMN ui_theme VARCHAR(16) NULL DEFAULT NULL
COMMENT 'Contractor portal: light, dark, auto';
