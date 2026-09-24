# MIZUNO PHOTO MEMBER — FAMILY PASSPORT Read Model

Baseline: 2026-09-24 JST

## Purpose

FAMILY PASSPORT turns already-published Family MEMORIES into a small milestone record that can later be shown in HOME or MY.

Initial milestone set:

- お宮参り
- 1st Birthday
- 七五三
- 入学

This phase is read-only and source-only.

It does not create Passport rows, modify MEMORIES, wire a Production route, or render the final customer UI.

## Identity boundary

The reader accepts identity only from a server-verified Member session:

- canonical 8-digit Customer ID
- explicit Family ID

It revalidates the current Customer -> Family link before reading any MEMORY data.

Request-supplied Customer ID or Family ID is not accepted.

Cross-Family reads fail closed.

## Data source

Milestones are derived only from:

`member_memories`

and only rows where:

- exact `family_id` matches the Member session
- `published=1`
- `deleted_at` is empty

No reservation-name lookup, customer-name matching, phone matching, email matching, child-name matching, fuzzy matching, or CRM PII inference is used.

## Explicit genre matching

Passport matching is deliberately conservative.

A MEMORY genre must exactly match a known alias after Unicode normalization and case normalization.

There is no substring inference.

### お宮参り

Accepted:

- お宮参り

### 1st Birthday

Accepted explicit aliases include:

- 1歳
- 1歳バースデー
- 1歳誕生日
- ファーストバースデー
- 1st Birthday
- First Birthday

Not accepted as 1st Birthday:

- generic バースデー
- 2歳バースデー
- unrelated birthday strings

A generic birthday record is not enough evidence that the shoot was the first birthday.

### 七五三

Accepted:

- 七五三

### 入学

Accepted explicit aliases include:

- 入学
- 入学撮影
- 入学記念

The combined CRM genre:

- 入学・卒業

is deliberately not treated as definite 入学 evidence because the source label does not prove which school event occurred.

## Unknown genres

Unknown or ambiguous genres are returned as:

`unmatched_genres`

They are not silently converted into milestones.

This allows future source cleanup or explicit alias decisions without corrupting a customer's Passport history.

## Output

For each milestone the reader returns:

- milestone code
- customer-facing label
- achieved boolean
- evidence count
- first achieved date
- latest achieved date
- qualifying MEMORY IDs

The Passport also returns:

- achieved milestone count
- total milestone count
- completion ratio
- whether all current milestone definitions are achieved
- unmatched genres
- matching policy

## Multiple matching MEMORIES

A Family can have more than one MEMORY for the same milestone.

Example:

- 七五三 2024
- 七五三 2026

The Passport keeps both as evidence while showing the milestone itself as achieved.

This avoids destructive deduplication and preserves Family history.

## Family-level limitation

The current MEMORY schema does not contain a canonical child identity on each MEMORY.

Therefore this phase is explicitly:

`child_specific=false`

It does not claim which child completed a milestone.

A future child-specific Passport would first require an explicit canonical Family-child identity model and a deterministic MEMORY -> child link. Child identity must never be inferred from names alone.

## Display order vs recommendation

The milestone order is currently:

1. お宮参り
2. 1st Birthday
3. 七五三
4. 入学

This is a display definition only.

It is not a NEXT MEMORY recommendation engine and must not be used to infer a child's current age or next event.

NEXT MEMORY remains a separate future model that can use verified birthday/age context and past genres.

## Conceptual HTTP contract

Source-only endpoint:

`GET /api/internal/member/family-passport`

The endpoint:

- requires server Member session
- is GET-only
- does not accept Customer ID or Family ID from the request
- is not Production route-wired

## Current exclusions

Not included:

- Passport write table
- milestone manual override
- child-specific Passport
- fuzzy genre matching
- age inference
- NEXT MEMORY recommendation
- Passport UI
- Production route wiring
- Production D1 schema apply
- Production D1 write
- Customer/Family write
- LINE send
- Production deploy

## Safety status

Current implementation is:

- read-only
- exact Customer identity only
- explicit Family link only
- Family-scoped
- published MEMORY only
- deleted MEMORY hidden
- explicit genre aliases only
- generic birthday does not imply first birthday
- combined school genre does not imply entrance
- no child inference
- Production route wiring = 0
- Production write = 0
