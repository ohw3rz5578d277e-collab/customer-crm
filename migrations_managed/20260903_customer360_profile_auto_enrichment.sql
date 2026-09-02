-- TASK-RESERVATION-CUSTOMER360-CRM-PROFILE-AUTO-ENRICHMENT-20260903
-- Additive only. Customer identity remains owned by the existing customers / identity registry.
-- No customer_id generation, merge, delete, or destructive rewrite.

CREATE TABLE IF NOT EXISTS customer_profile_enrichment (
  customer_id TEXT PRIMARY KEY,
  wedding_anniversary TEXT,
  first_inquiry_at TEXT,
  last_contact_at TEXT,
  lead_status TEXT CHECK (lead_status IS NULL OR lead_status IN ('inquiry','scheduling','quoted','booked','completed','lost','cancelled')),
  lost_reason TEXT CHECK (lost_reason IS NULL OR lost_reason IN ('schedule_mismatch','price','competitor','no_response','postponed','other','unknown')),
  referrer_customer_id TEXT,
  referrer_name TEXT,
  nps_score INTEGER CHECK (nps_score IS NULL OR (nps_score >= 0 AND nps_score <= 10)),
  nps_answered_at TEXT,
  nps_comment TEXT,
  publication_permission TEXT NOT NULL DEFAULT 'unknown' CHECK (publication_permission IN ('unknown','allowed','partial','denied')),
  marketing_contact_permission TEXT NOT NULL DEFAULT 'unknown' CHECK (marketing_contact_permission IN ('unknown','allowed','denied')),
  notes TEXT NOT NULL DEFAULT '',
  notes_updated_at TEXT,
  notes_updated_by TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_customer_profile_enrichment_lead
  ON customer_profile_enrichment(lead_status, lost_reason);
CREATE INDEX IF NOT EXISTS idx_customer_profile_enrichment_contact
  ON customer_profile_enrichment(last_contact_at);
CREATE INDEX IF NOT EXISTS idx_customer_profile_enrichment_referrer
  ON customer_profile_enrichment(referrer_customer_id);

CREATE TABLE IF NOT EXISTS customer_family_member_metadata (
  member_id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  birth_order INTEGER CHECK (birth_order IS NULL OR birth_order >= 1),
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_customer_family_member_metadata_order
  ON customer_family_member_metadata(customer_id, birth_order);

CREATE TABLE IF NOT EXISTS customer_field_evidence (
  candidate_id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  field_name TEXT NOT NULL,
  candidate_value TEXT NOT NULL,
  source TEXT NOT NULL CHECK (source IN ('manual','line','reservation','crm','attribution','derived')),
  confidence REAL,
  evidence_snippet TEXT NOT NULL DEFAULT '',
  source_event_id TEXT,
  status TEXT NOT NULL DEFAULT 'candidate' CHECK (status IN ('candidate','conflict','confirmed','rejected')),
  first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  confirmed_by_human INTEGER NOT NULL DEFAULT 0 CHECK (confirmed_by_human IN (0,1)),
  confirmed_at TEXT,
  confirmed_by TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_field_evidence_dedupe
  ON customer_field_evidence(customer_id, field_name, candidate_value, COALESCE(source_event_id, ''));
CREATE INDEX IF NOT EXISTS idx_customer_field_evidence_pending
  ON customer_field_evidence(customer_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_customer_field_evidence_field
  ON customer_field_evidence(customer_id, field_name, confirmed_by_human, confirmed_at);

CREATE TABLE IF NOT EXISTS customer_notes_history (
  note_id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  body TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_customer_notes_history_customer
  ON customer_notes_history(customer_id, created_at DESC);
