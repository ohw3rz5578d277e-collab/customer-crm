# MIZUNO PHOTO MEMBER — Private Media Access Foundation

Baseline: 2026-09-24 JST

## Purpose

Phase 3 intentionally does not expose `member_memory_media.storage_key` to the customer-facing MEMORIES read model.

This foundation defines the authorization layer required before a future private media delivery route may resolve a storage object.

It does not fetch media binaries and it is not wired into Production.

## Authorization chain

A private media access decision requires:

1. server-verified Member session;
2. canonical 8-digit Customer ID from that session;
3. exact active Customer ID -> Family ID link;
4. session Family ID must equal the linked Family ID;
5. requested `media_id` must belong to the same Family ID;
6. parent MEMORY must belong to the same Family ID;
7. parent MEMORY must be published and not deleted;
8. media row must not be deleted.

No customer name, address, phone, email, LINE display name, child name, or fuzzy identity matching is used.

## Cross-family behavior

A Family A session requesting a Family B `media_id` receives the same internal status as a missing media object.

The existence of Family B media is not disclosed.

## Storage key boundary

`storage_key` remains an internal storage locator.

It is returned only by the internal authorization function after authorization succeeds.

It is not a customer-facing response field.

The current contract rejects storage keys that are:

- empty;
- absolute HTTP/HTTPS URLs;
- absolute paths;
- path traversal using `..`;
- control-character-bearing;
- longer than the configured key limit.

This reduces the chance that a later delivery implementation accidentally treats an arbitrary URL or path as trusted storage input.

## Storage provider neutrality

This phase does not choose or activate a paid storage provider.

The authorization result is provider-neutral and only resolves an opaque private storage key.

A later phase may bind that key to an existing zero-cost-compatible private storage mechanism after Owner review.

## Function

`authorizeMemberPrivateMediaAccess(env, memberSession, mediaId)`

Success returns an internal descriptor containing:

- `media_id`
- `memory_id`
- `storage_key`
- `media_type`
- `role`
- dimensions

The descriptor is for a future trusted server-side delivery layer only.

## Current exclusions

Not included:

- public/private media HTTP route
- signed URL generation
- R2/S3/storage vendor activation
- binary fetch
- upload
- image transformation
- cache configuration for media bytes
- Production route wiring
- Production D1 schema apply
- Production D1 write
- LINE send
- Production deploy

## Safety guarantees

This phase performs SELECT-only authorization checks.

It introduces:

- zero INSERT
- zero UPDATE
- zero DELETE
- zero Production D1 write
- zero Production schema change
- zero LINE send
- zero Production deploy

## Required regressions

Tests cover:

- same-family authorization succeeds;
- session Family mismatch fails closed;
- cross-family media is indistinguishable from missing;
- deleted media is hidden;
- media under unpublished MEMORY is hidden;
- media under deleted MEMORY is hidden;
- missing Member schema is explicit;
- missing Family schema cannot be bypassed;
- untrusted storage-key forms fail closed;
- authorization path remains read-only.

## Next integration gate

A future media delivery implementation must not resolve or fetch `storage_key` before this authorization succeeds.

Any customer-facing delivery route, private storage binding, signed URL behavior, or Production deployment requires a separate implementation and gate.
