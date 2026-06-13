-- ======================================================
-- CUSTOMER CRM / OPS SCREENS
-- LINE template management screen and inquiry pipeline screen helpers
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_line_templates(
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  category TEXT,
  genre TEXT,
  body TEXT NOT NULL,
  variables_json TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  usage_count INTEGER NOT NULL DEFAULT 0,
  created_by TEXT,
  updated_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT,
  deleted_by TEXT
);

CREATE TABLE IF NOT EXISTS crm_inquiry_pipeline(
  id TEXT PRIMARY KEY,
  customer_id TEXT,
  customer_name TEXT,
  source TEXT,
  status TEXT,
  inquiry_text TEXT,
  next_action TEXT,
  next_due_date TEXT,
  expected_amount INTEGER DEFAULT 0,
  lost_reason TEXT,
  memo TEXT,
  created_by TEXT,
  updated_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT,
  deleted_by TEXT
);

ALTER TABLE crm_inquiry_pipeline ADD COLUMN source TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN next_action TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN next_due_date TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN expected_amount INTEGER DEFAULT 0;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN lost_reason TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN memo TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN updated_by TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN deleted_at TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN deleted_by TEXT;

CREATE INDEX IF NOT EXISTS idx_crm_tpl_ops ON crm_line_templates(status, category, genre, deleted_at);
CREATE INDEX IF NOT EXISTS idx_crm_pipe_ops ON crm_inquiry_pipeline(status, next_due_date, updated_at, deleted_at);
