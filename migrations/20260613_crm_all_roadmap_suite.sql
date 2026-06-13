-- ======================================================
-- CUSTOMER CRM / ALL ROADMAP SUITE
-- Rank badges / summaries / repeat leads / predictions / checklists / sales / logs / inquiry pipeline
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_checklist_items(
  id TEXT PRIMARY KEY,
  checklist_type TEXT NOT NULL,
  target_type TEXT,
  target_id TEXT,
  customer_id TEXT,
  customer_name TEXT,
  title TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open',
  due_date TEXT,
  priority TEXT DEFAULT 'medium',
  completed_at TEXT,
  completed_by TEXT,
  created_by TEXT,
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
  inquiry_text TEXT,
  inquiry_status TEXT NOT NULL DEFAULT '問い合わせ',
  expected_genre TEXT,
  expected_date TEXT,
  expected_amount REAL DEFAULT 0,
  next_action TEXT,
  next_due_date TEXT,
  lost_reason TEXT,
  assigned_to TEXT,
  created_by TEXT,
  updated_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT,
  deleted_by TEXT
);

CREATE TABLE IF NOT EXISTS crm_operation_logs_unified(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  log_type TEXT,
  action_label TEXT,
  target_type TEXT,
  target_id TEXT,
  customer_id TEXT,
  customer_name TEXT,
  before_json TEXT,
  after_json TEXT,
  result TEXT,
  actor_email TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crm_auto_tags(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  customer_id TEXT,
  tag_name TEXT,
  tag_reason TEXT,
  source TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(customer_id, tag_name)
);

ALTER TABLE customers ADD COLUMN customer_rank TEXT;
ALTER TABLE customers ADD COLUMN customer_rank_reason TEXT;
ALTER TABLE customers ADD COLUMN customer_rank_updated_at TEXT;
ALTER TABLE customers ADD COLUMN next_event_suggestion TEXT;
ALTER TABLE customers ADD COLUMN next_event_reason TEXT;
ALTER TABLE customers ADD COLUMN next_line_suggestion TEXT;
ALTER TABLE customers ADD COLUMN next_line_reason TEXT;
ALTER TABLE customers ADD COLUMN inquiry_status TEXT;

ALTER TABLE crm_reservation_drafts ADD COLUMN checklist_ready_at TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN shooting_day_notes TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN delivery_checklist_status TEXT;

ALTER TABLE customer_reservations ADD COLUMN checklist_status TEXT;
ALTER TABLE customer_reservations ADD COLUMN shooting_day_notes TEXT;

CREATE INDEX IF NOT EXISTS idx_crm_checklists_lookup ON crm_checklist_items(checklist_type,status,due_date,customer_id);
CREATE INDEX IF NOT EXISTS idx_crm_pipeline_status ON crm_inquiry_pipeline(inquiry_status,next_due_date,customer_id);
CREATE INDEX IF NOT EXISTS idx_crm_unified_logs ON crm_operation_logs_unified(log_type,created_at);
CREATE INDEX IF NOT EXISTS idx_crm_auto_tags_customer ON crm_auto_tags(customer_id, tag_name);
CREATE INDEX IF NOT EXISTS idx_customers_next_event ON customers(customer_rank,next_event_suggestion,last_shoot_date);
