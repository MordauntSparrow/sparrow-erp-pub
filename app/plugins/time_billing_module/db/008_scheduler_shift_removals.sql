-- Tracks scheduler-prefilled timesheet entries that staff deliberately deleted,
-- so the weekly prefill doesn't re-create them on next page load.

CREATE TABLE IF NOT EXISTS tb_scheduler_shift_removals (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  user_id INT NOT NULL,
  schedule_shift_id BIGINT NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_user_shift (user_id, schedule_shift_id),
  KEY idx_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

