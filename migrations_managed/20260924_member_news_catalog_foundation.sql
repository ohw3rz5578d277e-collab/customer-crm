-- MIZUNO PHOTO MEMBER — News catalog foundation
-- Source-only additive migration. Production apply requires separate Owner authorization.
--
-- Plain-text Member announcement catalog only.
-- No HTML, JavaScript, push delivery, LINE send, external URL, or write automation.

CREATE TABLE IF NOT EXISTS member_news_items (
  news_id TEXT PRIMARY KEY
    CHECK (length(news_id) BETWEEN 1 AND 160),
  news_type TEXT NOT NULL
    CHECK (news_type IN (
      'news',
      'campaign',
      'service',
      'maintenance'
    )),
  title TEXT NOT NULL
    CHECK (length(title) BETWEEN 1 AND 120),
  summary TEXT NOT NULL DEFAULT ''
    CHECK (length(summary) <= 280),
  body_text TEXT NOT NULL DEFAULT ''
    CHECK (length(body_text) <= 4000),
  hero_asset_id TEXT
    CHECK (hero_asset_id IS NULL OR length(hero_asset_id) BETWEEN 1 AND 160),
  local_path TEXT,
  starts_on TEXT,
  ends_on TEXT,
  published_at TEXT,
  featured_home INTEGER NOT NULL DEFAULT 0
    CHECK (featured_home IN (0,1)),
  published INTEGER NOT NULL DEFAULT 0
    CHECK (published IN (0,1)),
  sort_order INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_member_news_items_catalog
  ON member_news_items(published, deleted_at, sort_order, news_id);

CREATE INDEX IF NOT EXISTS idx_member_news_items_home
  ON member_news_items(featured_home, published, deleted_at, sort_order, news_id);

CREATE INDEX IF NOT EXISTS idx_member_news_items_schedule
  ON member_news_items(starts_on, ends_on, published_at);
