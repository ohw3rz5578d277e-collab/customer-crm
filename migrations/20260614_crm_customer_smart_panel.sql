-- ======================================================
-- CRM CUSTOMER SMART PANEL
-- build: 20260614_crm_customer_smart_panel
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_customer_smart_action_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  customer_id TEXT,
  action_type TEXT,
  action_label TEXT,
  related_id TEXT,
  before_status TEXT,
  after_status TEXT,
  payload_json TEXT,
  created_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_crm_customer_smart_action_logs_customer
  ON crm_customer_smart_action_logs(customer_id, created_at);

ALTER TABLE customers ADD COLUMN next_action_label TEXT;
ALTER TABLE customers ADD COLUMN next_action_due_at TEXT;
ALTER TABLE customers ADD COLUMN next_action_source TEXT;
ALTER TABLE customers ADD COLUMN smart_panel_updated_at TEXT;
