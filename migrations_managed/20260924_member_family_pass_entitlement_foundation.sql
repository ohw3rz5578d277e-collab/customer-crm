-- MIZUNO PHOTO MEMBER / FAMILY PASS BLACK ENTITLEMENT FOUNDATION
-- Source-only managed migration. Production application requires a separate Owner-approved schema gate.
--
-- Purpose:
--   persist BLACK achievement so a Family never ranks down after first qualification.
--
-- Safety:
--   * additive Member table only
--   * no canonical customer or reservation table mutation
--   * no automatic entitlement backfill
--   * no discount enforcement
--   * no Production D1 write occurs by adding this source file

CREATE TABLE IF NOT EXISTS member_family_pass_entitlements (
  family_id TEXT PRIMARY KEY,
  black_lifetime INTEGER NOT NULL DEFAULT 1
    CHECK (black_lifetime = 1),
  black_achieved_at TEXT NOT NULL,
  qualifying_memory_count INTEGER NOT NULL
    CHECK (qualifying_memory_count >= 10),
  achievement_source TEXT NOT NULL DEFAULT 'published-member-memories',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_member_family_pass_black_achieved_at
  ON member_family_pass_entitlements(black_achieved_at);
