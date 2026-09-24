# MIZUNO PHOTO MEMBER — Public Asset Catalog Integration

Baseline: 2026-09-25 JST

## Purpose

This phase integrates the approved public asset resolver into:

- Creative catalog
- Shop Pickup catalog
- NEWS catalog

The existing logical asset IDs remain canonical presentation references.

When the public asset registry is available, the read models additionally attach a trusted local public asset descriptor.

## Enrichment contract

Creative:

- `asset_ref.asset_id` remains
- `asset_ref.preview_asset_id` remains
- optional `asset_ref.public_asset`
- optional `asset_ref.preview_public_asset`

Shop Pickup:

- `hero_asset_ref.asset_id` remains
- optional `hero_asset_ref.public_asset`

NEWS:

- `hero_asset_ref.asset_id` remains
- optional `hero_asset_ref.public_asset`

Resolved paths can only come from the public asset resolver and therefore remain under:

`/member-assets/`

## Optional degradation

The public asset registry is a presentation enrichment, not a core identity/history dependency.

If `member_public_assets` is not applied:

- Creative still returns its logical asset IDs
- Shop still returns its logical hero asset IDs
- NEWS still returns its logical hero asset IDs
- the parent section remains available
- resolved public asset fields become null
- `public_assets_available=false`

This does not trigger Family identity degradation and does not hide otherwise valid catalog content.

## Internal batching

The public HTTP resolver remains limited to 20 asset IDs per request.

Catalog integration does not weaken that boundary.

The internal helper batches trusted catalog IDs in groups of 20.

The internal catalog ceiling is 240 unique logical asset IDs, which safely covers the current bounded catalogs:

- Creative: up to 100 templates, up to 2 logical assets each
- Shop: up to 100 products
- NEWS: up to 100 items

## Privacy boundary

Enrichment never exposes:

- private MEMORY media `storage_key`
- signed private photo URL
- external arbitrary URL
- customer photo binary
- CRM identity fields

The public asset resolver remains separate from private customer media authorization.

## HOME effect

HOME already consumes Creative, Shop Pickup, and NEWS read-model results.

Therefore HOME can inherit the optional enriched public asset descriptors through those components without a new HOME database query.

No new HOME identity decision is introduced.

## Current exclusions

Not included:

- Production schema apply
- Production route wiring
- binary public asset hosting
- upload/admin asset management
- CDN/external asset URLs
- private customer photo delivery
- signed private photo URLs
- Creative generation
- final customer UI
- Production deploy

## Safety status

Current integration is:

- source-only
- read-only
- logical asset IDs preserved
- optional public path enrichment
- trusted local `/member-assets/` paths only
- registry absence degrades presentation locally
- private customer media boundary unchanged
- storage key exposure = 0
- signed URL exposure = 0
- arbitrary external URL = 0
- extra HOME database read = 0
- Production schema apply = 0
- Production route wiring = 0
- Production write = 0
