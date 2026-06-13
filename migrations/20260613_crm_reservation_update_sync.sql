-- ======================================================
-- CUSTOMER CRM / RESERVATION UPDATE SYNC
-- 予約管理側で本予約が更新された時の同期情報
-- ======================================================

ALTER TABLE crm_reservation_drafts ADD COLUMN reservation_app_updated_at TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN reservation_app_updated_by TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN reservation_app_update_response TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_reservations_event_key
  ON customer_reservations(event_key);

CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_timeline_event_key
  ON customer_timeline(event_key);
