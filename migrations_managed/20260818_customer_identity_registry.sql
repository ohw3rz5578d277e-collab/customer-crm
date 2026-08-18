CREATE TABLE IF NOT EXISTS customer_identity_registry (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  customer_id TEXT UNIQUE,
  line_user_id TEXT NOT NULL UNIQUE,
  idempotency_key TEXT NOT NULL UNIQUE,
  source TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  raw_json TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_identity_registry_customer_id
ON customer_identity_registry(customer_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_identity_registry_line_user_id
ON customer_identity_registry(line_user_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_identity_registry_idempotency_key
ON customer_identity_registry(idempotency_key);

CREATE INDEX IF NOT EXISTS idx_customer_identity_registry_status_updated
ON customer_identity_registry(status, updated_at);
