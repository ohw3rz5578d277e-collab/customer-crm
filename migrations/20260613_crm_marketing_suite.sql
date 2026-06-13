-- ======================================================
-- CUSTOMER CRM / MARKETING SUITE
-- Campaigns / scoring / bulk line drafts / referrals / source analysis
-- ======================================================

CREATE TABLE IF NOT EXISTS crm_marketing_campaigns (
  id TEXT PRIMARY KEY,
  name TEXT,
  segment TEXT,
  season TEXT,
  template_body TEXT,
  status TEXT DEFAULT 'active',
  created_by TEXT,
  updated_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT,
  deleted_by TEXT
);

CREATE TABLE IF NOT EXISTS crm_marketing_scores (
  customer_id TEXT PRIMARY KEY,
  customer_name TEXT,
  score INTEGER DEFAULT 0,
  score_label TEXT,
  reasons_json TEXT,
  next_offer TEXT,
  next_line TEXT,
  last_calculated_at TEXT
);

CREATE TABLE IF NOT EXISTS crm_marketing_line_batches (
  id TEXT PRIMARY KEY,
  mode TEXT,
  segment TEXT,
  template_id TEXT,
  template_name TEXT,
  template_body TEXT,
  target_count INTEGER DEFAULT 0,
  created_count INTEGER DEFAULT 0,
  created_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  result_json TEXT
);

CREATE TABLE IF NOT EXISTS crm_referrals (
  id TEXT PRIMARY KEY,
  referrer_customer_id TEXT,
  referrer_name TEXT,
  referred_customer_id TEXT,
  referred_name TEXT,
  status TEXT DEFAULT 'lead',
  benefit TEXT,
  memo TEXT,
  created_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT
);

CREATE TABLE IF NOT EXISTS crm_line_response_logs (
  id TEXT PRIMARY KEY,
  line_log_id TEXT,
  customer_id TEXT,
  customer_name TEXT,
  template_id TEXT,
  template_name TEXT,
  response_status TEXT,
  led_to_reservation INTEGER DEFAULT 0,
  reservation_id TEXT,
  memo TEXT,
  created_by TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crm_marketing_ops_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  action_type TEXT,
  target_type TEXT,
  target_id TEXT,
  summary TEXT,
  detail_json TEXT,
  actor_email TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE customers ADD COLUMN source_type TEXT;
ALTER TABLE customers ADD COLUMN source_name TEXT;
ALTER TABLE customers ADD COLUMN campaign_name TEXT;
ALTER TABLE customers ADD COLUMN marketing_score INTEGER;
ALTER TABLE customers ADD COLUMN next_offer TEXT;
ALTER TABLE customers ADD COLUMN next_line_suggestion TEXT;
ALTER TABLE customers ADD COLUMN last_marketing_at TEXT;

ALTER TABLE crm_inquiry_pipeline ADD COLUMN source_type TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN source_name TEXT;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN estimated_amount INTEGER DEFAULT 0;
ALTER TABLE crm_inquiry_pipeline ADD COLUMN lost_reason TEXT;

CREATE INDEX IF NOT EXISTS idx_marketing_campaigns_segment ON crm_marketing_campaigns(segment,status,deleted_at);
CREATE INDEX IF NOT EXISTS idx_marketing_scores_score ON crm_marketing_scores(score DESC,score_label);
CREATE INDEX IF NOT EXISTS idx_marketing_batches_segment ON crm_marketing_line_batches(segment,created_at);
CREATE INDEX IF NOT EXISTS idx_referrals_status ON crm_referrals(status,created_at);
CREATE INDEX IF NOT EXISTS idx_line_response_customer ON crm_line_response_logs(customer_id,created_at);
CREATE INDEX IF NOT EXISTS idx_customers_source ON customers(source_type,source_name,campaign_name);
