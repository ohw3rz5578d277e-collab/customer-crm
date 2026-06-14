-- ======================================================
-- CRM List Workbench Safety
-- Preview / execute run logs for safer bulk operations
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_list_workbench_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_type TEXT,
  action_type TEXT,
  status TEXT DEFAULT 'created',
  target_count INTEGER DEFAULT 0,
  success_count INTEGER DEFAULT 0,
  failed_count INTEGER DEFAULT 0,
  skipped_count INTEGER DEFAULT 0,
  target_ids_json TEXT,
  preview_json TEXT,
  result_json TEXT,
  payload_json TEXT,
  created_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  executed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_crm_list_workbench_runs_created
ON crm_list_workbench_runs(created_at);

ALTER TABLE crm_list_workbench_logs ADD COLUMN run_id INTEGER;
ALTER TABLE crm_list_workbench_logs ADD COLUMN success_count INTEGER DEFAULT 0;
ALTER TABLE crm_list_workbench_logs ADD COLUMN failed_count INTEGER DEFAULT 0;
ALTER TABLE crm_list_workbench_logs ADD COLUMN result_json TEXT;
