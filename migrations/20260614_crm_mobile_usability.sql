-- ======================================================
-- CUSTOMER CRM / MOBILE USABILITY
-- build: customer-crm-api-mobile-usability-20260614-01
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_usability_preferences(
  user_email TEXT PRIMARY KEY,
  compact_mode INTEGER DEFAULT 1,
  show_mobile_bar INTEGER DEFAULT 1,
  show_priority_alerts INTEGER DEFAULT 1,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crm_usability_alert_ack(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_email TEXT,
  alert_key TEXT,
  ack_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(user_email, alert_key)
);

CREATE INDEX IF NOT EXISTS idx_crm_usability_alert_ack_user
  ON crm_usability_alert_ack(user_email, alert_key);

CREATE INDEX IF NOT EXISTS idx_crm_usability_preferences_updated
  ON crm_usability_preferences(updated_at);
