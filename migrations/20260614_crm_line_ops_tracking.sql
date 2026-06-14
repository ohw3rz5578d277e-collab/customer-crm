-- CRM LINE OPS TRACKING
-- created: 2026-06-14

CREATE TABLE IF NOT EXISTS crm_line_ops_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  line_log_id INTEGER,
  customer_id TEXT,
  action_type TEXT,
  before_status TEXT,
  after_status TEXT,
  payload_json TEXT,
  created_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_crm_line_ops_logs_line
ON crm_line_ops_logs(line_log_id, created_at);
