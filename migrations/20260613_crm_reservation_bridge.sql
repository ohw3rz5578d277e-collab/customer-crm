-- ======================================================
-- customer-crm reservation bridge
-- Created: 2026-06-13
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_reservation_drafts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  customer_id TEXT NOT NULL,
  customer_name TEXT,
  genre TEXT,
  shoot_date TEXT,
  start_time TEXT,
  place TEXT,
  plan_label TEXT,
  total_amount REAL DEFAULT 0,
  status TEXT DEFAULT 'draft',
  memo TEXT,
  draft_json TEXT,
  created_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  converted_at TEXT,
  converted_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_customer
ON crm_reservation_drafts(customer_id, created_at);

CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_status
ON crm_reservation_drafts(status, created_at);
