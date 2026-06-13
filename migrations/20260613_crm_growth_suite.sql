-- ======================================================
-- CUSTOMER CRM / GROWTH SUITE
-- Duplicate checks / auto follow-ups / progress / ranks / line templates
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_line_templates (
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

CREATE TABLE IF NOT EXISTS crm_reservation_progress (
  reservation_key TEXT PRIMARY KEY,
  crm_draft_id TEXT,
  reservation_app_reservation_id TEXT,
  customer_id TEXT,
  customer_name TEXT,
  genre TEXT,
  shoot_date TEXT,
  progress_status TEXT NOT NULL DEFAULT '予約確定',
  progress_step INTEGER NOT NULL DEFAULT 1,
  next_action TEXT,
  next_due_date TEXT,
  completed_at TEXT,
  created_by TEXT,
  updated_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT,
  deleted_by TEXT
);

CREATE TABLE IF NOT EXISTS crm_reservation_duplicate_checks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  crm_draft_id TEXT,
  customer_id TEXT,
  customer_name TEXT,
  shoot_date TEXT,
  start_time TEXT,
  genre TEXT,
  duplicate_level TEXT,
  duplicate_count INTEGER DEFAULT 0,
  result_json TEXT,
  checked_by TEXT,
  checked_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crm_growth_suite_logs (
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

ALTER TABLE customers ADD COLUMN customer_rank TEXT;
ALTER TABLE customers ADD COLUMN customer_rank_reason TEXT;
ALTER TABLE customers ADD COLUMN customer_rank_updated_at TEXT;
ALTER TABLE customers ADD COLUMN delivery_progress_status TEXT;
ALTER TABLE customers ADD COLUMN delivery_progress_updated_at TEXT;

ALTER TABLE crm_reservation_drafts ADD COLUMN duplicate_checked_at TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN duplicate_level TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN followup_created_at TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN delivery_progress_status TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN rank_synced_at TEXT;

ALTER TABLE customer_reservations ADD COLUMN delivery_progress_status TEXT;
ALTER TABLE customer_reservations ADD COLUMN progress_step INTEGER;
ALTER TABLE customer_reservations ADD COLUMN progress_updated_at TEXT;

CREATE INDEX IF NOT EXISTS idx_crm_line_templates_lookup ON crm_line_templates(status, category, genre, deleted_at);
CREATE INDEX IF NOT EXISTS idx_crm_progress_customer ON crm_reservation_progress(customer_id, progress_status, shoot_date);
CREATE INDEX IF NOT EXISTS idx_crm_progress_draft ON crm_reservation_progress(crm_draft_id);
CREATE INDEX IF NOT EXISTS idx_crm_dup_checks_draft ON crm_reservation_duplicate_checks(crm_draft_id, checked_at);
CREATE INDEX IF NOT EXISTS idx_customers_rank ON customers(customer_rank, total_revenue, repeat_count);
