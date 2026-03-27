CREATE INDEX idx_tsw_user_status ON tb_timesheet_weeks (user_id, status);
CREATE INDEX idx_tse_source ON tb_timesheet_entries (source);
CREATE INDEX idx_tse_client ON tb_timesheet_entries (client_id);
CREATE INDEX idx_runsheets_client_date ON runsheets (client_id, work_date);
CREATE INDEX idx_cp_type_scope ON calendar_policies (type, scope, active, effective_from);