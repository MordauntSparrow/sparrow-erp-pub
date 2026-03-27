-- Add profile picture path for contractor/employee portal
-- Stores relative path or filename for uploaded profile image
ALTER TABLE tb_contractors
  ADD COLUMN profile_picture_path VARCHAR(512) DEFAULT NULL
  COMMENT 'Relative path or filename for profile/avatar image'
  AFTER password_hash;
