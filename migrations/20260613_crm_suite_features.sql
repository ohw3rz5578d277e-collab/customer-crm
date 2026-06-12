-- ======================================================
-- customer-crm suite features
-- Created: 2026-06-13
-- Adds memos, tags, follow tasks, and LINE sent-by metadata.
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_customer_memos (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  customer_id TEXT NOT NULL,
  memo_text TEXT NOT NULL,
  created_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crm_customer_tags (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  customer_id TEXT NOT NULL,
  tag TEXT NOT NULL,
  color TEXT,
  created_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(customer_id, tag)
);

CREATE TABLE IF NOT EXISTS crm_follow_tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  customer_id TEXT NOT NULL,
  customer_name TEXT,
  task_type TEXT,
  title TEXT NOT NULL,
  message_text TEXT,
  due_date TEXT,
  priority TEXT DEFAULT 'medium',
  status TEXT DEFAULT 'open',
  created_by TEXT,
  completed_by TEXT,
  completed_at TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_crm_customer_memos_customer
  ON crm_customer_memos(customer_id, created_at);

CREATE INDEX IF NOT EXISTS idx_crm_customer_tags_customer
  ON crm_customer_tags(customer_id, tag);

CREATE INDEX IF NOT EXISTS idx_crm_follow_tasks_due
  ON crm_follow_tasks(status, due_date, priority);

-- If customer_line_draft_logs already exists, this column tracks who marked a draft as sent.
ALTER TABLE customer_line_draft_logs ADD COLUMN sent_by TEXT;
