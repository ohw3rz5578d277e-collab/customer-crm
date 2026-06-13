-- ======================================================
-- CUSTOMER CRM / FOLLOW TASK TO LINE TEMPLATE BRIDGE
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_follow_template_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  follow_task_id TEXT,
  template_id TEXT,
  line_log_id TEXT,
  customer_id TEXT,
  customer_name TEXT,
  rendered_body TEXT,
  actor_email TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE crm_follow_tasks ADD COLUMN template_id TEXT;
ALTER TABLE crm_follow_tasks ADD COLUMN template_name TEXT;
ALTER TABLE crm_follow_tasks ADD COLUMN line_log_id TEXT;
ALTER TABLE crm_follow_tasks ADD COLUMN line_draft_created_at TEXT;

ALTER TABLE customer_line_draft_logs ADD COLUMN template_id TEXT;
ALTER TABLE customer_line_draft_logs ADD COLUMN follow_task_id TEXT;
ALTER TABLE customer_line_draft_logs ADD COLUMN template_name TEXT;

CREATE INDEX IF NOT EXISTS idx_crm_follow_template_logs_task ON crm_follow_template_logs(follow_task_id, created_at);
CREATE INDEX IF NOT EXISTS idx_line_draft_logs_follow ON customer_line_draft_logs(follow_task_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_follow_tasks_template ON crm_follow_tasks(template_id, line_log_id, line_draft_created_at);
