# MIZUNO PHOTO MEMBER — Public Asset Resolver Foundation

Baseline: 2026-09-25 JST

## Purpose

Creative, Shop Pickup, and NEWS currently expose logical asset IDs only.

This phase adds the trusted source-level mapping from those logical IDs to public Member asset paths.

It does **not** deliver private customer photos.

## Security boundary

The registry may resolve only to local paths under:

`/member-assets/`

Examples:

- `/member-assets/shop/album.webp`
- `/member-assets/news/autumn.jpg`
- `/member-assets/creative/calendar-preview.webp`

Rejected paths include:

- external HTTP/HTTPS URLs
- protocol-relative URLs
- paths outside `/member-assets/`
- literal traversal
- percent-encoded path material
- backslash paths
- query strings
- fragments
- control characters
- outer whitespace

## Schema

Managed source migration:

`migrations_managed/20260924_member_public_asset_registry_foundation.sql`

Table:

`member_public_assets`

Fields are limited to:

- logical asset ID
- asset kind: image or video
- trusted local public path
- known MIME type
- optional/required dimensions
- published state
- sort metadata
- timestamps / soft deletion

The schema deliberately has no:

- `storage_key`
- external URL
- signed URL
- customer/family ownership
- private photo reference

Production schema application remains separately Owner-gated.

## Supported media

Initial MIME allowlist:

- image/jpeg
- image/png
- image/webp
- video/mp4

Image rows require valid width and height.

Video dimensions may be absent but, when provided, must be valid.

## Read model

`resolveMemberPublicAssets(env, assetIds)`

Rules:

- 1–20 logical asset IDs per request
- duplicate IDs are deduplicated
- requested ordering is preserved for resolved results
- only published, non-deleted registry rows are considered
- malformed/unsafe registry rows are hidden
- missing logical IDs are returned as unresolved IDs
- zero writes

A partially resolved batch is allowed and explicitly reports `complete=false`.

## Conceptual HTTP contract

Source-only endpoint:

`POST /api/internal/member/assets/resolve`

Body:

```json
{
  "asset_ids": ["asset:shop:album", "asset:news:autumn"]
}
```

Only the exact `asset_ids` key is accepted.

The handler requires the established server-side Member session boundary.

The response does not echo Customer ID or Family ID.

Production route wiring remains disabled.

## Separation from private media

This resolver is for brand/template/catalog assets only.

It must never be used to expose:

- private MEMORY media storage keys
- customer-uploaded photos
- signed private photo URLs
- original representative photos
- delivery-provider credentials

Private customer photo delivery remains governed by the separate private-media authorization path.

## Next integration phase

After this foundation is reviewed and merged, Creative / Shop Pickup / NEWS can optionally enrich their logical asset references through this resolver.

That integration should degrade presentation assets locally if the public asset schema is absent; it must not weaken Family authorization or private media boundaries.

## Current exclusions

Not included:

- Production schema apply
- Production route wiring
- asset upload/admin UI
- binary asset hosting
- CDN/external URLs
- private customer media delivery
- signed private URLs
- Creative execution
- final customer UI
- Production deploy

## Safety status

Current foundation is:

- source-only
- read-only
- logical asset ID input only
- trusted `/member-assets/` output only
- published-only
- deleted hidden
- max 20 IDs/request
- storage key exposure = 0
- arbitrary external URL = 0
- signed URL exposure = 0
- private customer media = 0
- Production schema apply = 0
- Production route wiring = 0
- Production write = 0
