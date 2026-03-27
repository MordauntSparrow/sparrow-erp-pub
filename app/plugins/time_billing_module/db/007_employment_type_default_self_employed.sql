-- Default employment_type to self_employed (contractors submit invoice with timesheet for approval).
ALTER TABLE tb_contractors
  MODIFY COLUMN employment_type ENUM('paye','self_employed') NOT NULL DEFAULT 'self_employed'
  COMMENT 'paye = timesheet only, self_employed = submit invoice with timesheet for approval';
