-- ======================================================
-- CUSTOMER CRM / TODAY ACTION FILTERS
-- build: 20260613_crm_today_action_filters
-- Adds assignee fields and indexes for filtered quick actions.
-- ======================================================

ALTER TABLE customer_line_draft_logs ADD COLUMN assigned_to TEXT;
ALTER TABLE crm_follow_tasks ADD COLUMN assigned_to TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN assigned_to TEXT;
ALTER TABLE crm_reservation_drafts ADD COLUMN priority TEXT;

CREATE INDEX IF NOT EXISTS idx_crm_today_line_filters ON customer_line_draft_logs(status, priority, assigned_to, created_at);
CREATE INDEX IF NOT EXISTS idx_crm_today_follow_filters ON crm_follow_tasks(status, due_date, priority, assigned_to);
CREATE INDEX IF NOT EXISTS idx_crm_today_reservation_filters ON crm_reservation_drafts(status, priority, assigned_to, updated_at);
