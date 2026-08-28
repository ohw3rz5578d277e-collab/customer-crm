-- CUSTOMER 360 / FAMILY / MARKETING FOUNDATION
-- Managed migration source only. Production application is intentionally deferred.

CREATE TABLE IF NOT EXISTS customer_family_members (
  id TEXT PRIMARY KEY,
  customer_id TEXT NOT NULL,
  relation TEXT NOT NULL DEFAULT 'other' CHECK (relation IN ('spouse','child','parent','grandparent','other')),
  name TEXT,
  furigana TEXT,
  birthdate TEXT,
  gender TEXT,
  school_stage TEXT,
  memo TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_by TEXT,
  deleted_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_customer_family_members_customer
  ON customer_family_members(customer_id, deleted_at, created_at);

CREATE INDEX IF NOT EXISTS idx_customer_family_members_relation
  ON customer_family_members(customer_id, relation, deleted_at);

CREATE TABLE IF NOT EXISTS customer_marketing_profiles (
  customer_id TEXT PRIMARY KEY,
  postal_code TEXT,
  prefecture TEXT,
  city TEXT,
  address_line1 TEXT,
  address_line2 TEXT,
  marketing_opt_out INTEGER,
  preferred_contact_channel TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_by TEXT
);

CREATE INDEX IF NOT EXISTS idx_customer_marketing_profiles_area
  ON customer_marketing_profiles(prefecture, city);

CREATE INDEX IF NOT EXISTS idx_customer_marketing_profiles_contact
  ON customer_marketing_profiles(marketing_opt_out, preferred_contact_channel);
