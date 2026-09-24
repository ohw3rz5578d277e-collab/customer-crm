-- MIZUNO PHOTO MEMBER — Shop Pickup catalog foundation
-- Source-only additive migration. Production apply requires separate Owner authorization.
--
-- This table is a Member presentation catalog only.
-- It is NOT an order, checkout, payment, inventory, price, or discount source of truth.

CREATE TABLE IF NOT EXISTS member_shop_products (
  product_id TEXT PRIMARY KEY
    CHECK (length(product_id) BETWEEN 1 AND 160),
  product_type TEXT NOT NULL
    CHECK (product_type IN (
      'album',
      'canvas',
      'frame',
      'print',
      'kotobuki'
    )),
  title TEXT NOT NULL
    CHECK (length(title) BETWEEN 1 AND 120),
  description TEXT NOT NULL DEFAULT ''
    CHECK (length(description) <= 500),
  season_tag TEXT NOT NULL DEFAULT 'evergreen'
    CHECK (length(season_tag) BETWEEN 1 AND 64),
  starts_on TEXT,
  ends_on TEXT,
  hero_asset_id TEXT NOT NULL
    CHECK (length(hero_asset_id) BETWEEN 1 AND 160),
  shop_path TEXT NOT NULL
    CHECK (length(shop_path) BETWEEN 1 AND 256),
  cta_label TEXT NOT NULL DEFAULT '商品を見る'
    CHECK (length(cta_label) BETWEEN 1 AND 40),
  featured_home INTEGER NOT NULL DEFAULT 0
    CHECK (featured_home IN (0,1)),
  published INTEGER NOT NULL DEFAULT 0
    CHECK (published IN (0,1)),
  sort_order INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_member_shop_products_hero_asset
  ON member_shop_products(hero_asset_id);

CREATE INDEX IF NOT EXISTS idx_member_shop_products_catalog
  ON member_shop_products(published, deleted_at, sort_order, product_id);

CREATE INDEX IF NOT EXISTS idx_member_shop_products_home
  ON member_shop_products(featured_home, published, deleted_at, sort_order, product_id);

CREATE INDEX IF NOT EXISTS idx_member_shop_products_schedule
  ON member_shop_products(starts_on, ends_on);
