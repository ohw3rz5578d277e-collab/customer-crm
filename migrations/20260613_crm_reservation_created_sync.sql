-- ======================================================
-- CRM reservation draft columns for reservation-app created sync
-- Created: 2026-06-13
-- ======================================================

ALTER TABLE crm_reservation_drafts ADD COLUMN reservation_app_reservation_id TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN reservation_app_intake_id TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN reservation_app_created_at TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN reservation_app_created_by TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN reservation_app_response TEXT;

CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_app_created
ON crm_reservation_drafts(status, reservation_app_created_at);
