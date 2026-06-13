-- ======================================================
-- CUSTOMER CRM / USABILITY HUB
-- Favorites / recent actions / search support
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_usability_favorites (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_email TEXT NOT NULL,
  feature_key TEXT NOT NULL,
  feature_label TEXT,
  feature_group TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(user_email, feature_key)
);

CREATE TABLE IF NOT EXISTS crm_usability_recent_actions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_email TEXT,
  action_key TEXT,
  action_label TEXT,
  detail_json TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE customers ADD COLUMN usability_last_opened_at TEXT;

CREATE INDEX IF NOT EXISTS idx_crm_usability_favorites_user ON crm_usability_favorites(user_email, feature_group);
CREATE INDEX IF NOT EXISTS idx_crm_usability_recent_user ON crm_usability_recent_actions(user_email, created_at);
CREATE INDEX IF NOT EXISTS idx_customers_usability_search ON customers(name, phone, email, kana);
CREATE INDEX IF NOT EXISTS idx_crm_inquiry_usability_search ON crm_inquiry_pipeline(customer_name, status, genre);
CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_usability_search ON crm_reservation_drafts(customer_name, shoot_date, status);
