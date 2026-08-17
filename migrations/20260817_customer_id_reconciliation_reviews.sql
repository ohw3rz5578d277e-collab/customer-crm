CREATE TABLE IF NOT EXISTS customer_id_reconciliation_reviews (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  reservation_customer_id TEXT NOT NULL,
  crm_candidate_customer_id TEXT NOT NULL,
  decision TEXT NOT NULL DEFAULT 'UNREVIEWED' CHECK (decision IN ('UNREVIEWED','SAME_PERSON','DIFFERENT_PERSON','DEFERRED')),
  reason TEXT,
  reviewed_by TEXT,
  reviewed_at TEXT,
  candidate_source TEXT NOT NULL DEFAULT 'manual_review_20260817',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(reservation_customer_id, crm_candidate_customer_id)
);

CREATE INDEX IF NOT EXISTS idx_customer_id_reconciliation_reviews_decision
ON customer_id_reconciliation_reviews(decision, updated_at);

CREATE INDEX IF NOT EXISTS idx_customer_id_reconciliation_reviews_reservation_id
ON customer_id_reconciliation_reviews(reservation_customer_id);

CREATE INDEX IF NOT EXISTS idx_customer_id_reconciliation_reviews_crm_id
ON customer_id_reconciliation_reviews(crm_candidate_customer_id);

CREATE TABLE IF NOT EXISTS customer_id_reconciliation_review_audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  review_id INTEGER NOT NULL,
  reservation_customer_id TEXT NOT NULL,
  crm_candidate_customer_id TEXT NOT NULL,
  from_decision TEXT,
  to_decision TEXT NOT NULL,
  reason TEXT,
  changed_by TEXT NOT NULL,
  changed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_customer_id_reconciliation_review_audit_review
ON customer_id_reconciliation_review_audit(review_id, changed_at);
