CREATE TABLE IF NOT EXISTS customer_identity_sequence (
  sequence_key TEXT PRIMARY KEY,
  last_value INTEGER NOT NULL CHECK(last_value >= 0 AND last_value <= 999999),
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO customer_identity_sequence (sequence_key, last_value, updated_at)
SELECT
  'canonical_customer_id',
  COALESCE(MAX(CAST(SUBSTR(customer_id, 3, 6) AS INTEGER)), 0),
  CURRENT_TIMESTAMP
FROM customers
WHERE customer_id GLOB '[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]';
