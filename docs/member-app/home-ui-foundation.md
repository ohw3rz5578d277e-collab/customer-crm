# MIZUNO PHOTO MEMBER — HOME UI Foundation

Baseline: 2026-09-25 JST

## Purpose

This phase turns the existing Member HOME read model into a source-only customer-facing HOME UI and mounts it into the canonical Member shell.

Design direction remains:

**Luxury Minimal × Family Story**

The UI is photo-led and editorial, but it does not invent private image URLs while Production private-media delivery is inactive.

## HOME composition

The visual order is:

1. Brand hero
2. TODAY'S MEMORY
3. Recent MEMORIES
4. FAMILY PASS
5. NEXT MEMORY
6. CREATE pickup
7. FAMILY PASSPORT
8. SHOP pickup
9. NEWS

This mirrors the already-approved HOME composition rather than introducing a second source of truth.

## Private-photo boundary

Recent MEMORIES and TODAY'S MEMORY are allowed to show presentation metadata, but this phase does not fabricate a customer-photo URL.

Until the private media delivery dependency is activated separately, those slots render a neutral photo placeholder.

This avoids:

- storage key exposure
- signed provider URL exposure
- arbitrary image URL injection
- bypassing the private media grant/content contract

## Public asset boundary

Creative, Shop, and NEWS may render only already-resolved local public asset paths under:

`/member-assets/...`

External URLs, traversal and backslash paths are rejected.

## Identity boundary

The HOME UI consumes an already-authorized HOME result.

It intentionally removes:

- Customer ID
- Family ID
- child IDs
- storage/provider internals

from the browser presentation model.

Child display name may be presented when the server read model already approved it.

## Navigation boundary

HOME buttons can switch only among the five canonical Member shell tabs:

- HOME
- MEMORIES
- CREATE
- SHOP
- MY

The HOME UI does not create arbitrary external navigation.

NEXT MEMORY remains suggestion-only.

No LINE draft/send, automatic contact or reservation creation is introduced.

## Family Pass / BLACK

The HOME UI shows the Family Pass tier and progress already returned by the read model.

BLACK styling remains inherited from the Member shell's separately authorized BLACK state.

HOME itself cannot promote a Family to BLACK or enforce a discount.

## SHOP boundary

Shop pickup can show presentation content.

The UI preserves the source state when:

- pricing is not authoritative
- checkout is not ready

It does not create an order or payment flow.

## Partial degradation

Optional HOME sections may be unavailable.

The HOME UI keeps the authorized sections visible and shows a quiet "一部の情報は現在準備中です" message.

Identity/security failures still fail before this view model is produced by the HOME read model.

## Production boundary

Not included:

- Production Member route wiring
- Production private media activation
- Production D1/schema changes
- Member session activation
- Customer/Family writes
- Favorite mutation UI
- Shop checkout/payment
- BLACK discount enforcement
- LINE send
- automatic contact
- reservation creation
- Production deploy

## Safety status

- HOME source UI = implemented
- canonical HOME shell mount = implemented
- TODAY'S MEMORY presentation = implemented
- recent MEMORIES presentation = implemented
- FAMILY PASS presentation = implemented
- NEXT MEMORY presentation = implemented
- CREATE pickup = implemented
- FAMILY PASSPORT = implemented
- SHOP pickup = implemented
- NEWS = implemented
- private MEMORY binary URL invention = 0
- external public asset URL = 0
- Customer ID exposure = 0
- Family ID exposure = 0
- automatic fetch = 0
- automatic write = 0
- automatic contact = 0
- LINE send = 0
- reservation creation = 0
- Production route wiring = 0
- Production write = 0
