# MIZUNO PHOTO MEMBER — MY UI Foundation

Baseline: 2026-09-25 JST

## Purpose

This phase adds the source-only customer-facing MY screen and mounts it into the canonical Member shell.

The screen composes the existing MY read model instead of creating a second source of truth.

Design direction remains:

**Luxury Minimal × Family Story**

## Visual composition

MY is organized as:

1. Family summary
2. FAMILY PASS
3. FAMILY PASSPORT
4. FAVORITES
5. NEXT MEMORY
6. SETTINGS

The emphasis is "家族の物語を、ひとつの場所に。"

## Identity minimization

The UI receives the already-authorized MY response.

Its presentation model intentionally removes:

- Customer ID
- Family ID
- linked Member Customer IDs
- canonical child IDs
- Passport evidence MEMORY IDs

Allowed presentation data includes:

- Family display name
- current relation/access role
- linked member count
- child display name when already returned by the authorized Next Memory reader

No identity inference is added.

## Family Pass

Family Pass remains the authoritative source for:

- MEMORY count
- current tier
- progress
- effective BLACK state

The MY UI does not calculate a competing tier.

The BLACK benefit contract may be displayed as informational text:

**PHOTO GOODS 10% OFF**

but the current UI also states that enforcement is not active.

The shooting fee is not discounted.

## Family Passport

The UI renders only milestone label and achieved state.

It does not expose:

- qualifying MEMORY IDs
- child attribution
- unmatched evidence internals

The existing explicit genre matching policy remains authoritative.

## Favorites

MY can display the current per-Member Favorite count.

A local CTA may switch the canonical shell to MEMORIES.

Favorite mutation remains disabled.

No Favorite add/remove/toggle is introduced from MY.

## Next Memory

NEXT MEMORY remains candidate-only.

MY may show:

- label
- deterministic target date when available
- days until when available
- authorized child display name

It does not expose child IDs.

No LINE message is sent.

No reservation is created.

## Settings

Settings rows are visible as product placeholders:

- Family profile
- Member profile
- Notification settings
- LINE link
- Account deletion

All remain non-mutable until separately reviewed.

## Partial degradation

Optional components may be unavailable:

- Family Passport
- Favorites
- Next Memory

MY still renders its core Family + Family Pass experience and shows a quiet partial-state message.

Security/identity failures remain fail-closed in the existing MY read model before the UI receives data.

## Production boundary

Not included:

- Production Member route wiring
- profile/family editing
- Favorite mutation
- notification mutation
- LINE account/link mutation
- account deletion
- reservation creation
- automatic contact
- LINE send
- Production D1/schema changes
- Production deploy

## Safety status

- canonical MY shell mount = implemented
- Family summary = implemented
- Family Pass presentation = implemented
- Family Passport presentation = implemented
- Favorite count presentation = implemented
- Next Memory presentation = implemented
- Settings placeholders = implemented
- Customer ID exposure = 0
- Family ID exposure = 0
- linked Customer ID exposure = 0
- child ID exposure = 0
- profile/family mutation = 0
- Favorite mutation = 0
- settings mutation = 0
- reservation creation = 0
- automatic contact = 0
- LINE send = 0
- Production route wiring = 0
- Production write = 0
