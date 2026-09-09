CREATE TABLE IF NOT EXISTS customer_profile_media (
  customer_id TEXT PRIMARY KEY,
  avatar_data_url TEXT,
  avatar_updated_at TEXT,
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_by TEXT
);

CREATE TABLE IF NOT EXISTS customer_delivery_links (
  link_id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  reservation_id TEXT,
  provider TEXT NOT NULL DEFAULT 'amazon_photos',
  label TEXT,
  url TEXT NOT NULL,
  delivered_at TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  created_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_customer_delivery_links_customer_date
  ON customer_delivery_links(customer_id, delivered_at DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_customer_delivery_links_reservation
  ON customer_delivery_links(reservation_id);
