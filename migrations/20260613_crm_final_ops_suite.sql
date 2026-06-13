-- ======================================================
-- CUSTOMER CRM / FINAL OPS SUITE
-- Inquiry shortcuts / unified logs / bulk LINE drafts / cancel followups
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_final_ops_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  action_type TEXT,
  target_type TEXT,
  target_id TEXT,
  customer_id TEXT,
  customer_name TEXT,
  result TEXT,
  detail_json TEXT,
  actor_email TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crm_cancel_followups (
  id TEXT PRIMARY KEY,
  customer_id TEXT,
  customer_name TEXT,
  reservation_id TEXT,
  cancel_reason TEXT,
  follow_status TEXT DEFAULT 'open',
  follow_due_date TEXT,
  line_log_id TEXT,
  created_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT
);

ALTER TABLE crm_inquiry_pipeline ADD COLUMN line_log_id TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN follow_task_id TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN reservation_draft_id TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN converted_at TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN action_summary TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN last_action_at TEXT;

ALTER TABLE customers ADD COLUMN next_event_suggestion TEXT;
ALTER TABLE customers ADD COLUMN next_line_suggestion TEXT;
ALTER TABLE customers ADD COLUMN customer_rank TEXT;
ALTER TABLE customers ADD COLUMN customer_rank_reason TEXT;
ALTER TABLE customers ADD COLUMN customer_rank_updated_at TEXT;

CREATE INDEX IF NOT EXISTS idx_crm_final_ops_logs ON crm_final_ops_logs(action_type, created_at);
CREATE INDEX IF NOT EXISTS idx_crm_cancel_followups ON crm_cancel_followups(follow_status, follow_due_date, customer_id);
CREATE INDEX IF NOT EXISTS idx_crm_inquiry_next_actions ON crm_inquiry_pipeline(status, last_action_at, reservation_draft_id);
CREATE INDEX IF NOT EXISTS idx_customers_next_suggestions ON customers(customer_rank, last_shoot_date, next_event_suggestion);
