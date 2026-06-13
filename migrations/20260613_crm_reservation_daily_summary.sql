-- ======================================================
-- CUSTOMER CRM / RESERVATION LINK DAILY SUMMARY INDEXES
-- date: 2026-06-13
-- ======================================================

CREATE INDEX IF NOT EXISTS idx_crm_daily_drafts_sent
  ON crm_reservation_drafts(sent_to_reservation_at);

CREATE INDEX IF NOT EXISTS idx_crm_daily_drafts_created
  ON crm_reservation_drafts(reservation_app_created_at);

CREATE INDEX IF NOT EXISTS idx_crm_daily_drafts_updated
  ON crm_reservation_drafts(reservation_app_updated_at);

CREATE INDEX IF NOT EXISTS idx_crm_daily_drafts_cancelled
  ON crm_reservation_drafts(reservation_app_cancelled_at);

CREATE INDEX IF NOT EXISTS idx_crm_daily_drafts_history_synced
  ON crm_reservation_drafts(history_synced_at);

CREATE INDEX IF NOT EXISTS idx_crm_daily_alert_checks
  ON crm_reservation_link_alert_checks(draft_id, stage_key, acknowledged_at);
