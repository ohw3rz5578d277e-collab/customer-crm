# MIZUNO PHOTO MEMBER — Bootstrap Plan Contract

Baseline: 2026-09-24 JST

## Purpose

This phase composes the existing Member foundations into one read-only account bootstrap plan.

It answers:

- which canonical Customer ID belongs to the upstream-verified LINE identity;
- which explicit Family ID belongs to that Customer ID;
- whether the signed Member session foundation is ready to issue a session;
- which historical completed shoots would become MEMORIES;
- which MEMORIES are already synchronized;
- whether any identity or historical-memory conflict requires manual review.

It does not issue a Member session, write MEMORIES, send LINE messages, wire a Production route, or deploy Production.

## Required upstream trust

Input:

`verified_line_user_id`

The word `verified` is important.

This function does not validate a LINE OAuth token itself. A future trusted login layer must first validate LINE Login and obtain the verified LINE user ID.

Until that future layer is activated, this remains a source-only plan.

## Identity chain

```
verified LINE identity
        |
        v
exact line_user_id lookup
        |
        v
canonical 8-digit Customer ID
        |
        v
explicit active Customer -> Family link
        |
        v
Family ID
        |
        +--> signed session readiness
        |
        +--> historical MEMORY sync plan
```

No fallback is permitted using:

- customer name
- address
- phone
- email
- LINE display name
- child name
- fuzzy matching

## Output: ready state

A successful plan returns:

- canonical `customer_id`;
- public Family metadata;
- whether Member session secret/config is ready;
- historical MEMORY counts;
- whether historical MEMORY writes will eventually be required;
- zero actual writes.

Session metadata does not contain a token or cookie value.

The bootstrap plan intentionally does not call the session issuer.

## Historical MEMORY planning

The plan reuses the existing Phase 2 historical MEMORY logic.

Only exact-customer completed photography history is considered.

The summary includes:

- source eligible count;
- `to_create`;
- `already_synced`;
- `skipped`;
- `conflicts`.

Existing `source_system + source_reservation_id` idempotency rules remain unchanged.

A cross-family or cross-customer MEMORY conflict stops bootstrap and requires review.

## Fail-closed states

Examples:

### LINE identity stage

- `invalid_line_user_id`
- `unlinked`
- `ambiguous_line_identity`
- `noncanonical_customer_id`

### Family stage

- `unlinked`
- `ambiguous_family_identity`
- `family_inactive_or_missing`
- `schema_not_applied`

### MEMORY stage

- schema missing states
- `memory_sync_conflict`
- other source validation failures

No later stage proceeds after a failed earlier identity stage.

## Existing customer experience

This contract supports the intended existing-customer onboarding experience.

Example:

A customer has already completed:

- 2023 お宮参り
- 2024 1st Birthday
- 2025 七五三

When the customer later creates a Member account, the future activation flow can:

1. verify LINE identity;
2. resolve canonical Customer ID;
3. resolve explicit Family ID;
4. calculate the historical MEMORY plan;
5. separately perform approved idempotent MEMORY writes;
6. issue the Member session;
7. open HOME with the family's existing history.

The current bootstrap plan stops before steps 5 and 6.

## Session readiness

The bootstrap plan reads only the session foundation health/configuration state.

It reports:

- `ready_to_issue`
- cookie contract name
- session max age
- signing algorithm

It does not expose:

- the session secret;
- a session token;
- a cookie value.

It does not create a Production secret.

## Registration gaps

An unlinked LINE identity is not automatically converted into a new CRM customer.

An exact Customer ID with no Family link is not automatically assigned to a Family.

Those future registration/write workflows require separate design and Owner authorization.

This prevents account creation from weakening the exact-identity contract.

## Current exclusions

Not included:

- LINE Login OAuth verification
- LINE Login activation
- LINE callback route
- Member login route
- Member session issuance
- Family creation
- Family-link creation
- Customer creation
- Customer ID generation
- historical MEMORY writes
- private media fetch
- HOME UI
- Production route wiring
- Production schema apply
- Production D1 write
- LINE send
- Production deploy

## Safety

This plan performs read-only orchestration.

Expected side effects:

- D1 write: 0
- session issue: 0
- MEMORY write: 0
- LINE send: 0
- Production deploy: 0

## Next activation boundary

After this foundation is merged, the next account-flow work can be separated into independently gated pieces:

1. trusted LINE Login verification/callback contract;
2. registration handling for exact identities that are not yet linked;
3. approved historical MEMORY write executor;
4. approved Member session issuance route;
5. customer-facing HOME/MEMORIES route wiring.

None of these are activated by this bootstrap plan.
