-- TASK-CRM-LINE-OUTBOUND-CONTEXT-001-IMPL-01
-- Actual LINE message event ledger for CRM outbound context.

CREATE TABLE IF NOT EXISTS customer_line_message_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  customer_id TEXT NOT NULL,
  line_user_id TEXT NOT NULL,
  direction TEXT NOT NULL CHECK(direction IN ('incoming','outbound')),
  message_type TEXT NOT NULL DEFAULT 'text',
  message_text TEXT NOT NULL,
  source TEXT NOT NULL,
  send_status TEXT NOT NULL CHECK(send_status IN ('pending','sent','failed','received')),
  send_error TEXT,
  line_message_id TEXT,
  idempotency_key TEXT UNIQUE,
  sender_type TEXT,
  occurred_at TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
  raw_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_customer_line_message_events_customer_time
ON customer_line_message_events(customer_id, occurred_at, id);

CREATE INDEX IF NOT EXISTS idx_customer_line_message_events_line_user_time
ON customer_line_message_events(line_user_id, occurred_at, id);

CREATE INDEX IF NOT EXISTS idx_customer_line_message_events_context
ON customer_line_message_events(line_user_id, direction, send_status, occurred_at);
