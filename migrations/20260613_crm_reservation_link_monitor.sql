-- ======================================================
-- CUSTOMER CRM / RESERVATION LINK MONITOR
-- 予約連携ステータス監視用の補助index
-- ======================================================

CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_monitor_status
  ON crm_reservation_drafts(status, sent_to_reservation_at, reservation_app_created_at, reservation_app_cancelled_at);

CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_monitor_customer
  ON crm_reservation_drafts(customer_id, created_at);

CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_monitor_app_reservation
  ON crm_reservation_drafts(reservation_app_reservation_id);
