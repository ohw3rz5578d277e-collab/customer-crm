-- ======================================================
-- CRM reservation link resync logs
-- date: 2026-06-13
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_reservation_link_resync_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  draft_id INTEGER NOT NULL,
  customer_id TEXT,
  requested_action TEXT,
  resolved_action TEXT,
  actor_email TEXT,
  before_stage TEXT,
  after_stage TEXT,
  ok INTEGER DEFAULT 0,
  message TEXT,
  response_json TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_crm_reservation_link_resync_logs_draft
  ON crm_reservation_link_resync_logs(draft_id, created_at);

CREATE INDEX IF NOT EXISTS idx_crm_reservation_link_resync_logs_actor
  ON crm_reservation_link_resync_logs(actor_email, created_at);
