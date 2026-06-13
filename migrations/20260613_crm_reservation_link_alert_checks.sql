-- ======================================================
-- CUSTOMER CRM / RESERVATION LINK ALERT ACKNOWLEDGEMENTS
-- Created: 2026-06-13
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_reservation_link_alert_checks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  draft_id INTEGER NOT NULL,
  customer_id TEXT,
  stage_key TEXT NOT NULL,
  acknowledged_at TEXT DEFAULT CURRENT_TIMESTAMP,
  acknowledged_by TEXT,
  note TEXT
);

CREATE INDEX IF NOT EXISTS idx_crm_reservation_link_alert_checks_draft
  ON crm_reservation_link_alert_checks(draft_id, stage_key, acknowledged_at);

CREATE INDEX IF NOT EXISTS idx_crm_reservation_link_alert_checks_customer
  ON crm_reservation_link_alert_checks(customer_id, acknowledged_at);
