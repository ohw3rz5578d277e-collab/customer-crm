# MIZUNO PHOTO MEMBER — Favorite Mutation Rate-Limit Foundation

Baseline: 2026-09-25 JST

## Purpose

This phase adds an abuse-control prerequisite for future customer-facing Favorite mutation.

The Favorite route and Favorite write remain Production-inactive.

## Fixed-window limits

One authenticated Member session is limited to:

- **20 Favorite mutation attempts per 60 seconds** across the Member account
- **6 Favorite mutation attempts per 60 seconds** for the same MEMORY

Both limits are scoped by exact:

- Family ID
- canonical Customer ID

The MEMORY-specific bucket additionally includes exact MEMORY ID.

## Why two limits

The Member-wide limit bounds broad rapid-fire automation.

The same-MEMORY limit bounds repeated add/remove oscillation on one item.

Normal human Favorite usage should remain well below both thresholds.

## Counter storage

Source migration:

`migrations_managed/20260924_member_favorite_mutation_rate_limit_foundation.sql`

Table:

`member_favorite_mutation_rate_limits`

The table stores only:

- Family ID
- Customer ID
- scope key
- fixed-window start
- attempt count
- update timestamp

It does not store:

- name
- email
- phone
- address
- LINE identity
- reservation data
- image data
- commerce data

Production schema application remains separately Owner-gated.

## Rate-limit mode

Counter consumption requires:

`MEMBER_FAVORITES_RATE_LIMIT_MODE=enabled`

Default is disabled.

The HTTP mutation contract now requires the limiter to be available before calling the Favorite write executor.

If the rate limiter is:

- disabled
- missing its schema
- unable to write its counter
- unable to verify the counter after write

Favorite mutation fails closed.

## Invalid MEMORY attempts

After a signed Member session has passed the HTTP security boundary, an invalid MEMORY ID still consumes the Member-wide quota.

This prevents malformed IDs from bypassing broad request throttling.

The MEMORY-specific counter is used only for a valid bounded MEMORY ID.

## 429 response

When a limit is exceeded, the HTTP contract returns:

- HTTP 429
- `error=rate_limited`
- `Retry-After` header for the remaining fixed-window seconds

The Favorite write executor is not called for a rate-limited request.

## Independent activation gates

Future Favorite mutation requires all of the following:

1. Production route wiring
2. `MEMBER_FAVORITES_MUTATION_ROUTE_MODE=enabled`
3. rate-limit schema applied
4. `MEMBER_FAVORITES_RATE_LIMIT_MODE=enabled`
5. Favorite schema applied
6. `MEMBER_FAVORITES_WRITE_MODE=enabled`
7. signed Member session
8. same-origin POST
9. exact JSON body
10. current Family/MEMORY authorization

This phase does not enable any of them in Production.

## Current exclusions

Not included:

- Production route wiring
- Production rate-limit schema apply
- Production rate-limit counter writes
- Production Favorite writes
- IP-based throttling
- device fingerprinting
- CAPTCHA
- final MEMORIES UI
- `favorite_mutable=true`
- LINE send
- Production deploy

## Safety status

Current foundation provides:

- fixed 60-second windows
- Member-wide 20-attempt limit
- same-MEMORY 6-attempt limit
- exact Family + Customer scoping
- exact MEMORY scoping for valid IDs
- invalid MEMORY consumes broad quota
- counter write post-verification
- fail-closed limiter availability
- HTTP 429 + Retry-After support
- separate route / limiter / Favorite-write gates
- canonical CRM write = 0
- Favorite write by limiter = 0
- LINE send = 0
- Production route wiring = 0
