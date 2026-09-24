-- MIZUNO PHOTO MEMBER — Favorite mutation rate-limit foundation
-- Source-only additive migration. Production apply requires separate Owner authorization.
--
-- Stores short-lived fixed-window counters for authenticated Member Favorite mutation.
-- No customer profile, LINE, reservation, photo, or commerce data is stored here.

CREATE TABLE IF NOT EXISTS member_favorite_mutation_rate_limits (
  family_id TEXT NOT NULL
    CHECK (length(family_id) BETWEEN 1 AND 128),
  customer_id TEXT NOT NULL
    CHECK (
      length(customer_id)=8
      AND customer_id NOT GLOB '*[^0-9]*'
    ),
  scope_key TEXT NOT NULL
    CHECK (length(scope_key) BETWEEN 1 AND 192),
  window_started_at INTEGER NOT NULL
    CHECK (window_started_at >= 0),
  attempt_count INTEGER NOT NULL DEFAULT 0
    CHECK (attempt_count >= 0),
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (
    family_id,
    customer_id,
    scope_key,
    window_started_at
  )
);

CREATE INDEX IF NOT EXISTS idx_member_favorite_rate_limits_updated
  ON member_favorite_mutation_rate_limits(updated_at);
