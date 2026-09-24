# MIZUNO PHOTO MEMBER — Favorites Read Model

Baseline: 2026-09-25 JST

## Purpose

This phase replaces the existing MEMORIES favorite placeholder with a source-only read foundation.

It does not yet allow the Member to mutate Favorite state.

## Ownership decision

Favorite is a **per-Member preference**, not one shared Favorite flag for the whole Family.

Canonical ownership is:

- exact Family ID
- exact canonical Customer ID
- exact MEMORY ID

This allows two authorized adults in the same Family to have different Favorites without changing Family history itself.

## Schema

Managed source migration:

`migrations_managed/20260924_member_memory_favorites_foundation.sql`

Table:

`member_memory_favorites`

Primary key:

`(family_id, customer_id, memory_id)`

No name, address, phone, email, LINE display name, or fuzzy identity field is used.

Production schema application remains separately Owner-gated.

## Visibility rule

A Favorite is returned only when its MEMORY is still:

- in the exact Family
- published
- not soft-deleted

A stale Favorite row cannot make a draft, deleted, or another-Family MEMORY visible.

## Member authorization

The public read model:

`readMemberFavoritesForSession`

requires:

- server-verified Member session
- canonical 8-digit Customer ID
- exact explicit Family link
- exact session Family match

Request query/body identity is not accepted.

## MEMORIES overlay

The existing MEMORIES reader now optionally overlays Favorite state.

When the Favorite schema is present:

- a matching exact Customer favorite returns `favorite=true`
- another Customer's favorite does not leak
- another Family's favorite does not leak

When the Favorite schema is not yet applied:

- MEMORIES still returns normally
- `favorites_available=false`
- every MEMORY falls back to `favorite=false`
- `favorite_mutable=false`

Favorite is therefore optional enrichment, not a new core dependency for Family history.

## Mutation boundary

This phase intentionally keeps:

- `favorite_mutable=false`
- favorite write route = 0
- INSERT = 0
- DELETE = 0

A later phase may add a guarded idempotent mutation executor after separate review.

## Conceptual API

Source-only endpoint:

`GET /api/internal/member/favorites`

Returns only this Member customer's visible Favorite MEMORY IDs.

The endpoint is not Production route-wired.

## Current exclusions

Not included:

- Favorite add
- Favorite remove
- toggle route
- optimistic frontend state
- final MEMORIES UI
- Production route wiring
- Production schema apply
- Production D1 write
- Member session issuance
- LINE send
- Production deploy

## Safety status

Current implementation is:

- source-only
- read-only
- exact Family authorization
- exact Customer preference isolation
- exact MEMORY join
- published MEMORY only
- deleted MEMORY hidden
- cross-Family fail-closed
- request identity override = 0
- Favorite mutation = 0
- canonical CRM write = 0
- LINE send = 0
- Production route wiring = 0
- Production write = 0
