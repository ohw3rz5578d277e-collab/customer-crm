-- ======================================================
-- customer-crm reservation history sync columns
-- Created: 2026-06-13
-- ======================================================

ALTER TABLE crm_reservation_drafts ADD COLUMN history_synced_at TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN history_event_key TEXT;

ALTER TABLE customer_reservations ADD COLUMN deleted_at TEXT;
ALTER TABLE customer_reservations ADD COLUMN deleted_by TEXT;
ALTER TABLE customer_reservations ADD COLUMN delete_reason TEXT;

ALTER TABLE customer_timeline ADD COLUMN deleted_at TEXT;
ALTER TABLE customer_timeline ADD COLUMN deleted_by TEXT;
ALTER TABLE customer_timeline ADD COLUMN delete_reason TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_reservations_event_key ON customer_reservations(event_key);
CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_timeline_event_key ON customer_timeline(event_key);
CREATE INDEX IF NOT EXISTS idx_customer_reservations_customer_date_sync ON customer_reservations(customer_id, shoot_date);
