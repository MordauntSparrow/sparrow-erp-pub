-- Login name for contractors (portal + /api/login); email stays for contact/seat limits.
ALTER TABLE tb_contractors
  ADD COLUMN username VARCHAR(64) NULL DEFAULT NULL
  COMMENT 'Login name; unique when set'
  AFTER email;

CREATE UNIQUE INDEX uq_tb_contractors_username ON tb_contractors (username);
