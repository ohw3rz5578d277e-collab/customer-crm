-- ======================================================
-- customer-crm LINE draft / message log table
-- Created: 2026-06-13
-- ======================================================

CREATE TABLE IF NOT EXISTS customer_line_draft_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  customer_id TEXT NOT NULL,
  customer_name TEXT,
  action_type TEXT,
  action_label TEXT,
  priority TEXT,
  message_text TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'copied',
  channel TEXT NOT NULL DEFAULT 'line',
  created_by TEXT,
  copied_at TEXT,
  sent_at TEXT,
  memo TEXT,
  raw_json TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_customer_line_draft_logs_customer
ON customer_line_draft_logs(customer_id, created_at);

CREATE INDEX IF NOT EXISTS idx_customer_line_draft_logs_status
ON customer_line_draft_logs(status, created_at);
