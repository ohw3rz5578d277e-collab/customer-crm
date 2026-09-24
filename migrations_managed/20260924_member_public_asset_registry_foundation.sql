-- MIZUNO PHOTO MEMBER — Public asset registry foundation
-- Source-only additive migration. Production apply requires separate Owner authorization.
--
-- Maps logical Member asset IDs to trusted local public paths only.
-- No private storage key, arbitrary external URL, signed URL, or customer photo is stored here.

CREATE TABLE IF NOT EXISTS member_public_assets (
  asset_id TEXT PRIMARY KEY
    CHECK (length(asset_id) BETWEEN 1 AND 160),
  asset_kind TEXT NOT NULL
    CHECK (asset_kind IN ('image','video')),
  local_path TEXT NOT NULL
    CHECK (length(local_path) BETWEEN 1 AND 320),
  mime_type TEXT NOT NULL
    CHECK (mime_type IN (
      'image/jpeg',
      'image/png',
      'image/webp',
      'video/mp4'
    )),
  width INTEGER,
  height INTEGER,
  published INTEGER NOT NULL DEFAULT 0
    CHECK (published IN (0,1)),
  sort_order INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT,
  CHECK (
    (asset_kind='image' AND width IS NOT NULL AND height IS NOT NULL AND width BETWEEN 1 AND 8192 AND height BETWEEN 1 AND 8192)
    OR
    (asset_kind='video' AND (width IS NULL OR width BETWEEN 1 AND 8192) AND (height IS NULL OR height BETWEEN 1 AND 8192))
  )
);

CREATE INDEX IF NOT EXISTS idx_member_public_assets_catalog
  ON member_public_assets(published, deleted_at, sort_order, asset_id);
