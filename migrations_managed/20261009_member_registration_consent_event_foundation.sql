-- MEMBER PROSPECT REGISTRATION / CONSENT EVENT FOUNDATION CANDIDATE
-- Source only. DO NOT APPLY TO PRODUCTION without a separate exact-SHA Owner schema authorization.
-- Additive event-evidence tables only. Existing Customer / Member identity tables are not mutated here.

CREATE TABLE IF NOT EXISTS member_registration_events (
  registration_event_id TEXT PRIMARY KEY,
  registration_idempotency_key TEXT NOT NULL UNIQUE,
  member_identity_id TEXT NOT NULL,
  prospect_id TEXT NOT NULL,
  profile_digest_sha256 TEXT NOT NULL,
  terms_version TEXT NOT NULL,
  terms_sha256 TEXT NOT NULL,
  privacy_version TEXT NOT NULL,
  privacy_sha256 TEXT NOT NULL,
  accepted_at TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CHECK (length(registration_event_id) = 68),
  CHECK (substr(registration_event_id, 1, 4) = 'REG_'),
  CHECK (substr(registration_event_id, 5) NOT GLOB '*[^0-9a-f]*'),
  CHECK (length(registration_idempotency_key) BETWEEN 16 AND 128),
  CHECK (registration_idempotency_key NOT GLOB '*[^A-Za-z0-9._:-]*'),
  CHECK (length(member_identity_id) >= 26 AND substr(member_identity_id, 1, 4) = 'MID_'),
  CHECK (substr(member_identity_id, 5) NOT GLOB '*[^A-Za-z0-9_-]*'),
  CHECK (length(prospect_id) >= 26 AND substr(prospect_id, 1, 4) = 'PID_'),
  CHECK (substr(prospect_id, 5) NOT GLOB '*[^A-Za-z0-9_-]*'),
  CHECK (length(profile_digest_sha256) = 64 AND profile_digest_sha256 NOT GLOB '*[^0-9a-f]*'),
  CHECK (length(terms_version) BETWEEN 1 AND 64),
  CHECK (length(privacy_version) BETWEEN 1 AND 64),
  CHECK (length(terms_sha256) = 64 AND terms_sha256 NOT GLOB '*[^0-9a-f]*'),
  CHECK (length(privacy_sha256) = 64 AND privacy_sha256 NOT GLOB '*[^0-9a-f]*'),
  UNIQUE (member_identity_id, prospect_id, registration_idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_member_registration_events_member_time
  ON member_registration_events(member_identity_id, accepted_at, registration_event_id);

CREATE INDEX IF NOT EXISTS idx_member_registration_events_prospect_time
  ON member_registration_events(prospect_id, accepted_at, registration_event_id);

CREATE TABLE IF NOT EXISTS member_consent_evidence (
  consent_event_id TEXT PRIMARY KEY,
  registration_event_id TEXT NOT NULL UNIQUE,
  member_identity_id TEXT NOT NULL,
  prospect_id TEXT NOT NULL,
  terms_version TEXT NOT NULL,
  terms_sha256 TEXT NOT NULL,
  privacy_version TEXT NOT NULL,
  privacy_sha256 TEXT NOT NULL,
  accepted_at TEXT NOT NULL,
  evidence_source TEXT NOT NULL DEFAULT 'prospect_registration',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CHECK (length(consent_event_id) = 69),
  CHECK (substr(consent_event_id, 1, 5) = 'CONS_'),
  CHECK (substr(consent_event_id, 6) NOT GLOB '*[^0-9a-f]*'),
  CHECK (length(registration_event_id) = 68),
  CHECK (substr(registration_event_id, 1, 4) = 'REG_'),
  CHECK (substr(registration_event_id, 5) NOT GLOB '*[^0-9a-f]*'),
  CHECK (length(member_identity_id) >= 26 AND substr(member_identity_id, 1, 4) = 'MID_'),
  CHECK (substr(member_identity_id, 5) NOT GLOB '*[^A-Za-z0-9_-]*'),
  CHECK (length(prospect_id) >= 26 AND substr(prospect_id, 1, 4) = 'PID_'),
  CHECK (substr(prospect_id, 5) NOT GLOB '*[^A-Za-z0-9_-]*'),
  CHECK (length(terms_version) BETWEEN 1 AND 64),
  CHECK (length(privacy_version) BETWEEN 1 AND 64),
  CHECK (length(terms_sha256) = 64 AND terms_sha256 NOT GLOB '*[^0-9a-f]*'),
  CHECK (length(privacy_sha256) = 64 AND privacy_sha256 NOT GLOB '*[^0-9a-f]*'),
  CHECK (evidence_source IN ('prospect_registration','member_reconsent'))
);

CREATE INDEX IF NOT EXISTS idx_member_consent_evidence_member_time
  ON member_consent_evidence(member_identity_id, accepted_at, consent_event_id);

CREATE INDEX IF NOT EXISTS idx_member_consent_evidence_prospect_time
  ON member_consent_evidence(prospect_id, accepted_at, consent_event_id);

CREATE TRIGGER IF NOT EXISTS trg_member_registration_events_no_update
BEFORE UPDATE ON member_registration_events
BEGIN
  SELECT RAISE(ABORT, 'member_registration_events_append_only');
END;

CREATE TRIGGER IF NOT EXISTS trg_member_registration_events_no_delete
BEFORE DELETE ON member_registration_events
BEGIN
  SELECT RAISE(ABORT, 'member_registration_events_append_only');
END;

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
-- * Customer / Family linkage
-- * UPDATE / DELETE path for event evidence
-- * raw profile PII storage
-- * LINE / Google / R2 side effects
