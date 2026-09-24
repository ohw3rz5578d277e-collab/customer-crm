-- MEMBER APP / FAMILY IDENTITY FOUNDATION
-- Managed migration source only.
-- Production application is intentionally deferred and requires a separate Owner-approved schema gate.
--
-- Purpose:
--   Add an explicit household/family identity layer for MIZUNO PHOTO MEMBER.
-- Safety:
--   * additive tables only
--   * the canonical customers table is not altered
--   * no automatic family formation
--   * no name/address/phone/email based linking
--   * no Customer ID generation or mutation
--   * no Production D1 write is performed by adding this source file

CREATE TABLE IF NOT EXISTS customer_family_groups (
  family_id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active','inactive')),
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_by TEXT
);

CREATE TABLE IF NOT EXISTS customer_family_customer_links (
  family_id TEXT NOT NULL,
  customer_id TEXT NOT NULL,
  relation TEXT NOT NULL DEFAULT 'member'
    CHECK (relation IN ('owner','partner','guardian','member','other')),
  access_role TEXT NOT NULL DEFAULT 'member'
    CHECK (access_role IN ('owner','adult','viewer')),
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_by TEXT,
  deleted_at TEXT,
  PRIMARY KEY (family_id, customer_id)
);

CREATE INDEX IF NOT EXISTS idx_customer_family_links_family
  ON customer_family_customer_links(family_id, deleted_at, created_at);

CREATE INDEX IF NOT EXISTS idx_customer_family_links_customer
  ON customer_family_customer_links(customer_id, deleted_at, created_at);

-- A canonical CRM customer may belong to at most one active Member family at a time.
-- Historical links remain possible through soft delete.
CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_family_links_one_active_family
  ON customer_family_customer_links(customer_id)
  WHERE deleted_at IS NULL;
