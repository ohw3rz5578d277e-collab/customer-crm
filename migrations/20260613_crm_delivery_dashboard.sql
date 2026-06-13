-- ======================================================
-- CUSTOMER CRM / DELIVERY DASHBOARD
-- Delivery progress delay alerts and indexes
-- ======================================================

ALTER TABLE crm_reservation_progress ADD COLUMN delay_level TEXT;
ALTER TABLE crm_reservation_progress ADD COLUMN delay_reason TEXT;
ALTER TABLE crm_reservation_progress ADD COLUMN delay_checked_at TEXT;
ALTER TABLE crm_reservation_progress ADD COLUMN alert_ack_at TEXT;
ALTER TABLE crm_reservation_progress ADD COLUMN alert_ack_by TEXT;

CREATE INDEX IF NOT EXISTS idx_crm_delivery_progress_status
  ON crm_reservation_progress(progress_status, shoot_date, next_due_date, deleted_at);

CREATE INDEX IF NOT EXISTS idx_crm_delivery_delay
  ON crm_reservation_progress(delay_level, delay_checked_at);
