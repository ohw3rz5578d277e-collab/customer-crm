-- MEMBER IDENTITY / PROSPECT FOUNDATION CANDIDATE
-- Source only. DO NOT APPLY TO PRODUCTION without a separate exact-SHA Owner schema authorization.
-- Additive candidate tables only. No existing customer table mutation.

CREATE TABLE IF NOT EXISTS member_identities (
  member_identity_id TEXT PRIMARY KEY,
  canonical_customer_id TEXT,
  prospect_id TEXT,
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active','review_required','disabled')),
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CHECK (canonical_customer_id IS NULL OR canonical_customer_id GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]')
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_member_identity_customer
  ON member_identities(canonical_customer_id)
  WHERE canonical_customer_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_member_identity_prospect
  ON member_identities(prospect_id)
  WHERE prospect_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS member_prospects (
  prospect_id TEXT PRIMARY KEY,
  member_identity_id TEXT NOT NULL UNIQUE,
  acquisition_source TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'prospect'
    CHECK (status IN ('prospect','promotion_review','promoted','disabled')),
  promoted_customer_id TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS member_customer_invitations (
  invitation_id TEXT PRIMARY KEY,
  canonical_customer_id TEXT NOT NULL,
  token_sha256 TEXT NOT NULL UNIQUE,
  expires_at TEXT NOT NULL,
  consumed_at TEXT,
  invalidated_at TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  created_by TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_member_customer_invitation_customer
  ON member_customer_invitations(canonical_customer_id,created_at);

-- At most one unused/non-invalidated invitation may exist for a customer.
-- Reissue must invalidate the old row and insert the replacement in one transaction.
CREATE UNIQUE INDEX IF NOT EXISTS idx_member_customer_invitation_one_active
  ON member_customer_invitations(canonical_customer_id)
  WHERE consumed_at IS NULL AND invalidated_at IS NULL;

CREATE TABLE IF NOT EXISTS member_profile_change_review_queue (
  review_id TEXT PRIMARY KEY,
  member_identity_id TEXT NOT NULL,
  claimed_customer_id TEXT,
  reason_code TEXT NOT NULL,
  payload_digest_sha256 TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending','approved','rejected','expired')),
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  decided_at TEXT,
  decided_by TEXT
);

-- Intentionally absent:
-- * Customer ID generation
-- * fuzzy/name/phone/email matching
-- * customer merge/delete
-- * profile PII payload storage in review queue
-- * automatic Production writes
