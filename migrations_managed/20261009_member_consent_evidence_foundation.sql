-- MEMBER CONSENT EVIDENCE FOUNDATION CANDIDATE
-- Source only. DO NOT APPLY TO PRODUCTION without a separate exact-SHA Owner schema authorization.
-- Additive table and append-only protection triggers only.

CREATE TABLE IF NOT EXISTS member_consent_evidence (
  consent_event_id TEXT PRIMARY KEY,
  member_identity_id TEXT NOT NULL,
  prospect_id TEXT,
  terms_version TEXT NOT NULL,
  terms_sha256 TEXT NOT NULL,
  privacy_version TEXT NOT NULL,
  privacy_sha256 TEXT NOT NULL,
  accepted_at TEXT NOT NULL,
  evidence_source TEXT NOT NULL DEFAULT 'prospect_registration',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CHECK (length(member_identity_id) >= 26 AND member_identity_id GLOB 'MID_*'),
  CHECK (prospect_id IS NULL OR (length(prospect_id) >= 26 AND prospect_id GLOB 'PID_*')),
  CHECK (length(terms_version) BETWEEN 1 AND 64),
  CHECK (length(privacy_version) BETWEEN 1 AND 64),
  CHECK (length(terms_sha256) = 64 AND terms_sha256 NOT GLOB '*[^0-9a-f]*'),
  CHECK (length(privacy_sha256) = 64 AND privacy_sha256 NOT GLOB '*[^0-9a-f]*'),
  CHECK (evidence_source IN ('prospect_registration','member_reconsent'))
);

CREATE INDEX IF NOT EXISTS idx_member_consent_evidence_member_time
  ON member_consent_evidence(member_identity_id, accepted_at, consent_event_id);

CREATE TRIGGER IF NOT EXISTS trg_member_consent_evidence_no_update
BEFORE UPDATE ON member_consent_evidence
BEGIN
  SELECT RAISE(ABORT, 'member_consent_evidence_append_only');
END;

CREATE TRIGGER IF NOT EXISTS trg_member_consent_evidence_no_delete
BEFORE DELETE ON member_consent_evidence
BEGIN
  SELECT RAISE(ABORT, 'member_consent_evidence_append_only');
END;

-- Intentionally absent:
-- * Production migration application
-- * Customer ID generation or mutation
-- * Customer/Family linkage
-- * consent-row UPDATE/DELETE path
-- * profile PII payload storage
-- * LINE / Google / R2 side effects
