-- ======================================================
-- customer-crm reservation draft send-to-reservation columns
-- Created: 2026-06-13
-- ======================================================

ALTER TABLE crm_reservation_drafts ADD COLUMN reservation_intake_id TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN sent_to_reservation_at TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN sent_to_reservation_by TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN sent_to_reservation_response TEXT;

CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_sent
  ON crm_reservation_drafts(status, sent_to_reservation_at);
