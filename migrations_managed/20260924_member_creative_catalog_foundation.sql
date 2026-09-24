-- MIZUNO PHOTO MEMBER — Creative catalog foundation
-- Source-only additive migration. Production apply requires separate Owner authorization.

CREATE TABLE IF NOT EXISTS member_creative_templates (
  template_id TEXT PRIMARY KEY
    CHECK (length(template_id) BETWEEN 1 AND 160),
  creative_type TEXT NOT NULL
    CHECK (creative_type IN (
      'wallpaper',
      'calendar',
      'collage',
      'then_and_now',
      'family_card',
      'memory_movie'
    )),
  title TEXT NOT NULL
    CHECK (length(title) BETWEEN 1 AND 120),
  description TEXT NOT NULL DEFAULT ''
    CHECK (length(description) <= 500),
  season_tag TEXT NOT NULL DEFAULT 'evergreen'
    CHECK (length(season_tag) BETWEEN 1 AND 64),
  starts_on TEXT,
  ends_on TEXT,
  photo_slots INTEGER NOT NULL DEFAULT 1
    CHECK (photo_slots BETWEEN 1 AND 12),
  composition_mode TEXT NOT NULL
    CHECK (composition_mode IN (
      'single_photo',
      'multi_photo',
      'pair_photo',
      'sequence'
    )),
  canvas_width INTEGER NOT NULL
    CHECK (canvas_width BETWEEN 320 AND 8192),
  canvas_height INTEGER NOT NULL
    CHECK (canvas_height BETWEEN 320 AND 8192),
  output_mime TEXT NOT NULL
    CHECK (output_mime IN (
      'image/jpeg',
      'image/png',
      'image/webp',
      'video/mp4'
    )),
  asset_id TEXT NOT NULL
    CHECK (length(asset_id) BETWEEN 1 AND 160),
  preview_asset_id TEXT
    CHECK (preview_asset_id IS NULL OR length(preview_asset_id) BETWEEN 1 AND 160),
  minimum_memory_count INTEGER NOT NULL DEFAULT 1
    CHECK (minimum_memory_count BETWEEN 1 AND 12),
  published INTEGER NOT NULL DEFAULT 0
    CHECK (published IN (0,1)),
  sort_order INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_member_creative_templates_asset_id
  ON member_creative_templates(asset_id);

CREATE INDEX IF NOT EXISTS idx_member_creative_templates_catalog
  ON member_creative_templates(published, deleted_at, sort_order, template_id);

CREATE INDEX IF NOT EXISTS idx_member_creative_templates_schedule
  ON member_creative_templates(starts_on, ends_on);
