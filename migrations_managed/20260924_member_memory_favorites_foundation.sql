-- MIZUNO PHOTO MEMBER — per-Member MEMORY Favorites foundation
-- Source-only additive migration. Production apply requires separate Owner authorization.
--
-- Favorite ownership is exact Family + Customer + MEMORY.
-- This schema does not perform writes by itself.

CREATE TABLE IF NOT EXISTS member_memory_favorites (
  family_id TEXT NOT NULL
    CHECK (length(family_id) BETWEEN 1 AND 128),
  customer_id TEXT NOT NULL
    CHECK (
      length(customer_id)=8
      AND customer_id NOT GLOB '*[^0-9]*'
    ),
  memory_id TEXT NOT NULL
    CHECK (length(memory_id) BETWEEN 1 AND 160),
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (family_id, customer_id, memory_id)
);

CREATE INDEX IF NOT EXISTS idx_member_memory_favorites_customer
  ON member_memory_favorites(customer_id, created_at, memory_id);

CREATE INDEX IF NOT EXISTS idx_member_memory_favorites_memory
  ON member_memory_favorites(family_id, memory_id, customer_id);
