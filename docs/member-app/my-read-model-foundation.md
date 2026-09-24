# MIZUNO PHOTO MEMBER — MY Read Model Foundation

Baseline: 2026-09-25 JST

## Purpose

MY is the Member app account/family summary surface.

This phase composes already-established Member sources instead of introducing a second source of truth.

Sources:

- Family identity
- Family Pass
- Family Passport
- Favorites
- Next Memory

No customer-facing UI is implemented in this phase.

## Core vs optional sections

Core:

- exact Customer -> Family identity
- Family Pass

Family Pass is core because it already owns the authoritative published MEMORY count and current tier calculation.

Optional/degraded sections:

- Family Passport
- Favorites
- Next Memory

Safe schema absence in those optional sections does not hide the rest of MY.

Identity ambiguity never degrades silently.

## Family summary

MY exposes only safe family presentation data:

- display name
- current Member relation
- access role
- linked member count

It does not expose the list of linked Customer IDs.

Family/profile editing remains disabled.

## Counts and membership

MEMORY count is not recomputed by a new MY query.

It is reused from:

`readMemberFamilyPassForSession`

This keeps the Family Pass definition:

**1 published MEMBER MEMORY = 1 qualifying shoot**

MY also exposes:

- current tier
- effective BLACK state
- Family Pass entitlement summary
- BLACK goods benefit contract

Benefit enforcement remains inactive.

## Family Passport

MY reuses the existing Family Passport reader.

It does not add new genre inference.

The existing explicit alias matching rules remain authoritative.

## Favorites

Favorite count is per-Member Customer preference, not shared Family state.

MY reuses the existing Favorites read model.

If the Favorites schema is absent:

- MY remains available
- favorite_count becomes null
- Favorites section is marked unavailable

Favorite mutation remains false in MY capabilities even though a separately guarded mutation foundation exists.

## Next Memory

MY reuses the existing Next Memory reader.

Managed child identity continues to require exact canonical child rows.

Safe missing child-profile schema degrades the section locally.

Ambiguous child identity fails MY closed.

LINE consultation remains:

- consultation intent only
- automatic_send=false

## Settings placeholders

MY currently exposes non-mutable placeholders for:

- family profile edit
- member profile edit
- notification settings
- LINE link settings
- account deletion

All remain false.

## Identity minimization

Internal component composition verifies exact:

- Customer ID
- Family ID

The customer-facing MY HTTP response deliberately does **not** expose:

- Customer ID
- Family ID
- linked member Customer IDs

## Conceptual endpoint

Source-only endpoint:

`GET /api/internal/member/my`

Requirements:

- server-supplied verified Member session
- exact current Customer -> Family authorization

Request query parameters cannot override:

- Customer identity
- Family identity
- deterministic clock controls

Production route wiring remains false.

## Failure policy

Fail closed:

- invalid Member session
- Family access denied
- missing Family identity schema
- ambiguous Family identity
- unlinked/inactive Family
- incomplete Family identity
- ambiguous child identity
- component identity mismatch
- unavailable Family Pass core

Section-local degradation:

- Family Passport non-security failure
- Favorites schema absence
- child profile schema absence for Next Memory

## Current exclusions

Not included:

- final MY UI
- profile editing
- family editing
- notification setting mutation
- LINE account/link mutation
- account deletion
- Favorite mutation activation from MY
- reservation creation
- automatic contact
- LINE send
- Production route wiring
- Production write
- Production deploy

## Safety status

Current MY foundation is:

- source-only
- read-only
- composed from existing SOT readers
- Family identity core required
- Family Pass core required
- MEMORY count reused from Family Pass
- Passport optional
- Favorites optional
- Next Memory optional when safe
- Customer ID public response = 0
- Family ID public response = 0
- linked Customer ID list public response = 0
- edit mutations = 0
- reservation creation = 0
- automatic contact = 0
- LINE send = 0
- Production route wiring = 0
- Production write = 0
