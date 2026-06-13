-- ======================================================
-- CRM List Workbench
-- - Bulk action logs
-- - Optional pinned fields for list usability
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_list_workbench_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  workbench_type TEXT,
  action_type TEXT,
  target_count INTEGER DEFAULT 0,
  target_ids_json TEXT,
  payload_json TEXT,
  created_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_crm_list_workbench_logs_created
  ON crm_list_workbench_logs(created_at);

ALTER TABLE customers ADD COLUMN list_pinned_at TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN list_pinned_at TEXT;
