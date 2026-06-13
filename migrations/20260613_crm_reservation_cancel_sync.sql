-- ======================================================
-- CUSTOMER CRM / RESERVATION CANCEL SYNC
-- Adds fields used when reservation-app cancellations are reflected back into CRM.
-- ======================================================

ALTER TABLE crm_reservation_drafts ADD COLUMN reservation_app_cancelled_at TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN reservation_app_cancelled_by TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN reservation_app_cancel_reason TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN reservation_app_cancel_response TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN cancellation_synced_at TEXT;

ALTER TABLE customer_reservations ADD COLUMN deleted_at TEXT;
ALTER TABLE customer_reservations ADD COLUMN deleted_by TEXT;
ALTER TABLE customer_reservations ADD COLUMN delete_reason TEXT;

ALTER TABLE customer_timeline ADD COLUMN deleted_at TEXT;
ALTER TABLE customer_timeline ADD COLUMN deleted_by TEXT;
ALTER TABLE customer_timeline ADD COLUMN delete_reason TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_reservations_event_key
  ON customer_reservations(event_key);

CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_timeline_event_key
  ON customer_timeline(event_key);
