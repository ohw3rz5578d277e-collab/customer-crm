-- ======================================================
-- customer-crm today action dashboard indexes
-- Created: 2026-06-13
-- Speeds up dashboard counts for reservation links, LINE drafts, follow tasks, and alert checks.
-- ======================================================

CREATE INDEX IF NOT EXISTS idx_crm_today_line_status
  ON customer_line_draft_logs(status, priority, created_at);

CREATE INDEX IF NOT EXISTS idx_crm_today_follow_due
  ON crm_follow_tasks(status, due_date, priority);

CREATE INDEX IF NOT EXISTS idx_crm_today_reservation_drafts
  ON crm_reservation_drafts(status, sent_to_reservation_at, reservation_app_created_at, history_synced_at, reservation_app_cancelled_at);

CREATE INDEX IF NOT EXISTS idx_crm_today_alert_checks
  ON crm_reservation_link_alert_checks(draft_id, stage_key, acknowledged_at);

CREATE INDEX IF NOT EXISTS idx_crm_today_customers_focus
  ON customers(total_revenue, repeat_count, dormant_days, deleted_at);
