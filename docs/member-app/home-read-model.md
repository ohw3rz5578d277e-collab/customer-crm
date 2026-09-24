# MIZUNO PHOTO MEMBER — HOME Read Model Foundation

Baseline: 2026-09-24 JST

## Purpose

This phase adds a source-only read model for the future MIZUNO PHOTO MEMBER HOME screen.

It composes already-approved Member foundations instead of creating a second source of truth.

Current HOME composition uses:

- MEMORIES
- FAMILY PASS
- FAMILY PASSPORT
- TODAY'S MEMORY
- Creative
- NEXT MEMORY

Final visual UI remains a separate design task.

This phase does not wire a Production route, issue sessions, send LINE, create reservations, apply schema, or write D1.

## Why a HOME composition layer

Without a composition layer, the future customer frontend would need to coordinate multiple internal calls and independently decide:

- which Family identity is authoritative;
- how to handle one missing optional section;
- whether component identities disagree;
- which data can safely be shown together.

The HOME read model centralizes those decisions on the server side.

## Identity source

HOME accepts only the server-verified Member session:

- canonical 8-digit Customer ID
- explicit Family ID

Request query/body identity is not accepted.

Each underlying component still performs its own authorization.

The HOME layer additionally checks that every successful component agrees with the Member session Family and, when present, Customer ID.

A component identity mismatch fails closed as:

`component_identity_mismatch`

and requires review.

## Composed components

### MEMORIES

Source:

`readMemberMemoriesForSession`

HOME uses:

- newest published, non-deleted Family MEMORIES
- up to 3 recent MEMORY list cards
- the loaded visible MEMORY count

The count is intentionally named:

`visible_memory_count`

because the MEMORIES reader has a bounded list limit. HOME does not falsely present it as an unbounded database total.

MEMORIES is the current HOME core.

If the core MEMORY schema is unavailable, HOME does not present an otherwise-partial Family history as normal.

### FAMILY PASS

Source:

`readMemberFamilyPassForSession`

HOME can expose the already-defined:

- current tier
- progress
- durable BLACK state when available
- BLACK benefit contract metadata

No discount is enforced by HOME.

### FAMILY PASSPORT

Source:

`readMemberFamilyPassportForSession`

HOME can expose the Family-level milestone model already defined in its own contract.

HOME does not convert FAMILY PASSPORT into child-specific history.

### TODAY'S MEMORY

Source:

`buildTodaysMemory`

HOME derives TODAY'S MEMORY from the MEMORIES list that HOME has already loaded.

This intentionally avoids a second Member MEMORY database read inside the same HOME request.

HOME exposes the already-approved TODAY'S MEMORY contract:

- exact prior-year month/day anniversary when available
- honest `この日の思い出` label for exact matches
- same-month ±7-day fallback only when no exact match exists
- honest `この頃の思い出` label for fallback
- no current-year nostalgia match
- conservative leap-day behavior

If there is no qualifying past MEMORY, the TODAY'S MEMORY section remains available with `today_memory=null` rather than inventing content.

### Creative

Source:

`readMemberCreativeCatalogFromAuthorizedMemories`

HOME reuses the already-authorized MEMORIES result that was loaded for the HOME core. Creative therefore does not issue a second MEMORIES list read just to calculate template eligibility.

HOME exposes a bounded preview:

- up to 3 templates in Owner-defined catalog order
- total available template count
- eligible template count
- per-template eligibility including `memories_needed`
- `generation_ready=false`

Creative schema absence is optional degradation. If the catalog schema has not been applied in an environment, the rest of authorized HOME data can still render and HOME reports the Creative section as unavailable.

Source-level Creative activation does **not** activate rendering, photo upload, generated-output storage, asset delivery, or final CREATE/HOME UI.

### NEXT MEMORY

Source:

`readMemberNextMemoryForSession`

HOME exposes only:

- primary NEXT MEMORY candidate
- up to 3 candidate previews
- consultation CTA metadata
- explicit history-scope flags

NEXT MEMORY remains suggestion-only.

No automatic LINE contact or reservation creation occurs.

## Partial-degradation rule

Some HOME sections can be safely unavailable without hiding all other authorized Family data.

Example:

The managed child-profile schema may not yet be available.

In that case:

- MEMORIES can still render
- FAMILY PASS can still render
- FAMILY PASSPORT can still render
- NEXT MEMORY becomes unavailable
- HOME returns `partial=true`
- `unavailable_sections` records the reason

This is different from an identity/security failure.

## Fail-closed identity/security rule

The following statuses are treated as fatal for HOME composition:

- invalid Member session
- Family access denied
- Family identity schema unavailable
- ambiguous Family identity
- unlinked Customer
- inactive/missing Family
- incomplete Family identity
- ambiguous child identity

HOME does not mix safe-looking sections with identity ambiguity.

## Component consistency

Successful components must agree with the server Member session.

Examples of prohibited composition:

- MEMORIES from Family A + FAMILY PASS from Family B
- Member session Customer A + Passport response for Customer B

If such a condition appears, HOME fails closed instead of merging the responses.

## Response limits

Initial HOME limits:

- recent MEMORIES: 3
- NEXT MEMORY candidate previews: 3
- Creative template previews: 3

These are presentation read-model limits, not deletion or storage limits.

## Module activation state

TODAY'S MEMORY and Creative are now active in the source-level HOME composition.

This means only that the HOME read model can return its already-approved read-only data.

It does **not** mean:

- Production route activation
- final customer UI activation
- push notification activation
- LINE activation

The remaining future HOME modules are still inactive:

- Shop Pickup
- News

HOME does not invent data for modules whose backend contracts are not yet implemented.

## Conceptual HTTP contract

Source-only endpoint:

`GET /api/internal/member/home`

The endpoint:

- requires server Member session
- is GET-only
- ignores request-supplied Customer ID
- ignores request-supplied Family ID
- ignores client-controlled `as_of`
- is not Production route-wired

The source-level read function may accept deterministic `as_of` only for tests/internal verification of NEXT MEMORY and TODAY'S MEMORY.

## Privacy / exposure boundary

HOME does not newly expose:

- private media storage keys
- raw photo binaries
- CRM address
- phone
- email
- LINE user ID
- LINE messages
- payment data

It only composes fields already exposed by the approved Member read models.

## Current exclusions

Not included:

- final HOME UI
- hero visual implementation
- final TODAY'S MEMORY UI
- TODAY'S MEMORY push notification
- Creative rendering / generation
- Creative asset delivery
- Creative customer-photo upload
- Shop Pickup
- News feed
- favorites
- commerce discount enforcement
- LINE draft/send
- automatic contact
- reservation creation
- Production route wiring
- Production D1 schema apply
- Production D1 write
- Customer/Family write
- Member session issuance
- Production deploy

## Safety status

Current implementation is:

- read-only
- server Member session only
- exact Customer/Family identity
- component identity consistency required
- identity ambiguity fails closed
- MEMORIES core required
- optional NEXT MEMORY schema degradation supported
- TODAY'S MEMORY source composition active
- TODAY'S MEMORY derived from the already-loaded MEMORIES result
- no extra TODAY'S MEMORY database read inside HOME
- Creative source composition active
- Creative eligibility derived from the already-authorized MEMORIES result
- no extra Creative MEMORIES list read inside HOME
- Creative generation remains disabled
- no automatic contact
- LINE send = 0
- reservation creation = 0
- Production route wiring = 0
- Production write = 0
