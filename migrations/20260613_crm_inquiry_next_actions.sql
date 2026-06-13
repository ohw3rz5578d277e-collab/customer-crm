CREATE TABLE IF NOT EXISTS crm_inquiry_action_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  inquiry_id TEXT,
  action_type TEXT,
  target_id TEXT,
  customer_id TEXT,
  customer_name TEXT,
  before_status TEXT,
  after_status TEXT,
  detail_json TEXT,
  actor_email TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE crm_inquiry_pipeline ADD COLUMN line_log_id TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN follow_task_id TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN reservation_draft_id TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN converted_at TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN action_summary TEXT;

CREATE INDEX IF NOT EXISTS idx_crm_inquiry_next_status ON crm_inquiry_pipeline(status, due_date, updated_at);
CREATE INDEX IF NOT EXISTS idx_crm_inquiry_action_logs ON crm_inquiry_action_logs(inquiry_id, created_at);
