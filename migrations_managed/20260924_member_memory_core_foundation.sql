-- MIZUNO PHOTO MEMBER / MEMORY CORE FOUNDATION
-- Source-only managed migration. Production application requires a separate Owner-approved schema gate.
--
-- Safety:
--   * additive Member tables only
--   * no canonical customer or reservation table mutation
--   * no automatic historical import
--   * no Production D1 write occurs by adding this source file

CREATE TABLE IF NOT EXISTS member_memories (
  memory_id TEXT PRIMARY KEY,
  family_id TEXT NOT NULL,
  source_system TEXT NOT NULL DEFAULT 'customer-crm',
  source_customer_id TEXT NOT NULL,
  source_reservation_id TEXT NOT NULL,
  shoot_date TEXT,
  genre TEXT,
  title TEXT NOT NULL DEFAULT 'MEMORY',
  delivery_link_id TEXT,
  amazon_photos_url TEXT,
  published INTEGER NOT NULL DEFAULT 0 CHECK (published IN (0,1)),
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT,
  deleted_by TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_member_memories_source_once
  ON member_memories(source_system, source_reservation_id);

CREATE INDEX IF NOT EXISTS idx_member_memories_family_date
  ON member_memories(family_id, shoot_date DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_member_memories_source_customer
  ON member_memories(source_customer_id, source_system, source_reservation_id);

CREATE TABLE IF NOT EXISTS member_memory_media (
  media_id TEXT PRIMARY KEY,
  memory_id TEXT NOT NULL,
  family_id TEXT NOT NULL,
  storage_key TEXT NOT NULL UNIQUE,
  media_type TEXT NOT NULL DEFAULT 'image'
    CHECK (media_type IN ('image','video')),
  role TEXT NOT NULL DEFAULT 'preview'
    CHECK (role IN ('cover','preview')),
  sort_order INTEGER NOT NULL DEFAULT 0,
  width INTEGER,
  height INTEGER,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  deleted_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_member_memory_media_memory
  ON member_memory_media(memory_id, deleted_at, sort_order, media_id);

CREATE INDEX IF NOT EXISTS idx_member_memory_media_family
  ON member_memory_media(family_id, deleted_at, created_at);
