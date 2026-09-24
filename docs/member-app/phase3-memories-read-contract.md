# MIZUNO PHOTO MEMBER — Phase 3 MEMORIES Read Contract

Baseline: 2026-09-24 JST

## Goal

Provide the read-only Member Core model needed by the future customer-facing MEMORIES timeline and MEMORY detail screen without applying the Member schema to Production, writing historical data, wiring a Production route, or accepting caller-supplied customer/family identity.

## Identity boundary

The read model accepts only a server-verified Member session containing:

- canonical 8-digit Customer ID
- explicit Family ID

The request URL, query string, and request body are not identity sources.

Before MEMORY data is read:

1. resolve the exact canonical Customer ID through the existing Phase 1 family identity contract;
2. require exactly one active Customer ID -> Family ID link;
3. require the linked Family ID to equal the server-side Member session Family ID;
4. otherwise fail closed.

Name, address, phone, email, LINE display name, child name, and fuzzy matching remain prohibited.

## Read surfaces

The source-only contract models two future endpoints:

- `GET /api/internal/member/memories`
- `GET /api/internal/member/memories/:memory_id`

The handler is not wired into the Production Worker in this phase.

Identity is injected by the server-side Member session. There is intentionally no `customer_id` or `family_id` request parameter.

## MEMORY list model

A list item exposes:

- `memory_id`
- `shoot_date`
- `genre`
- `title`
- cover media metadata
- preview count
- Amazon Photos availability
- favorite placeholder
- CREATE availability flag
- SHOP availability flag

Only rows meeting all of these rules are returned:

- exact authorized `family_id`
- `published = 1`
- not soft-deleted

The model is ordered by newest shoot date first.

## MEMORY detail model

A detail response exposes:

- MEMORY title/date/genre
- authorized preview media metadata
- exact stored Amazon Photos link when it is HTTPS
- favorite placeholder
- `next_memory = null` until that phase exists
- empty CREATE actions until CREATE is implemented
- SHOP unavailable until SHOP is implemented

Unpublished or deleted MEMORIES are not visible.

A MEMORY that exists under another Family ID is returned as `memory_not_found`, so its existence is not disclosed.

## Private media boundary

`member_memory_media.storage_key` is a private storage locator and is never returned by the customer-facing read model.

The read model returns only non-secret media metadata such as:

- `media_id`
- type
- role
- sort order
- dimensions
- `private_delivery_required = true`

A later private media delivery route must perform its own Member session authorization before resolving a storage key.

Guessable public object URLs are not introduced here.

## Amazon Photos

The Phase 2 synchronization foundation is responsible for exact reservation-level association.

Phase 3 additionally validates the stored URL as HTTPS before returning it.

No customer-level fallback is performed.

## Schema behavior

This phase expects source definitions for:

- `member_memories`
- `member_memory_media`

If the tables are not present in the runtime database, the read model fails with `member_memory_schema_not_applied`.

This is expected in Production until a separate Owner-approved schema gate is granted.

## Read-only guarantees

This phase performs:

- SELECT only
- zero INSERT
- zero UPDATE
- zero DELETE
- zero historical sync writes
- zero Family link writes
- zero LINE sends
- zero Production deploys

## Feature placeholders

Favorites, CREATE, SHOP, and Next Memory are represented conservatively:

- `favorite = false`
- `favorite_mutable = false`
- `create_available = false`
- `create_actions = []`
- `shop_available = false`
- `next_memory = null`

These fields must not imply a feature is active before its own phase is implemented.

## Cross-family tests

Regression coverage requires:

- valid Family A session can list Family A published MEMORIES;
- Family B session using Customer A fails closed;
- Family A requesting a Family B MEMORY ID receives not found;
- unpublished/deleted MEMORIES remain hidden;
- deleted/cross-family media remain hidden;
- request query parameters cannot override server-side identity;
- private `storage_key` is absent from responses;
- the schema-missing path performs no writes.

## Current phase exclusions

Not included:

- Production route wiring
- Production D1 schema apply
- Production D1 write
- historical MEMORY insertion
- private media binary delivery
- favorites persistence
- CREATE implementation
- SHOP implementation
- NEXT MEMORY implementation
- HOME / MEMORIES frontend
- LINE Login activation
- Google Sheets backup execution
- Production deploy

## Next gate

After CI and review pass, Phase 3 may be merged to main with Owner approval.

Production use still requires separate future authorization for:

1. Family schema apply;
2. Member MEMORY schema apply;
3. explicit Family link write path;
4. historical MEMORY write path;
5. customer-facing route/session wiring;
6. Production deploy.
