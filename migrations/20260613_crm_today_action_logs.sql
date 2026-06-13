-- ======================================================
-- CUSTOMER CRM / TODAY DASHBOARD ACTION LOGS
-- created: 2026-06-13
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_today_action_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  action_type TEXT NOT NULL,
  target_type TEXT,
  target_id TEXT,
  customer_id TEXT,
  customer_name TEXT,
  status_before TEXT,
  status_after TEXT,
  actor_email TEXT,
  result TEXT,
  message TEXT,
  raw_json TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_crm_today_action_logs_created ON crm_today_action_logs(created_at);
CREATE INDEX IF NOT EXISTS idx_crm_today_action_logs_target ON crm_today_action_logs(target_type, target_id, created_at);
CREATE INDEX IF NOT EXISTS idx_crm_today_action_logs_customer ON crm_today_action_logs(customer_id, created_at);

ALTER TABLE customer_line_draft_logs ADD COLUMN sent_by TEXT;
ALTER TABLE crm_follow_tasks ADD COLUMN completed_by TEXT;
ALTER TABLE crm_follow_tasks ADD COLUMN completed_at TEXT;
