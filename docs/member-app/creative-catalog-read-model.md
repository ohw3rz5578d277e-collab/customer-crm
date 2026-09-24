# MIZUNO PHOTO MEMBER — Creative Catalog Read Model

Baseline: 2026-09-24 JST

## Purpose

This phase adds the read-only foundation for the future CREATE experience.

The product direction is zero-cost-first:

1. reusable template is prepared once;
2. Owner reviews / approves it;
3. Member selects one of their existing photos;
4. composition should preferably happen in the browser/device;
5. no paid image-generation SaaS is required for each customer use.

This phase implements only the safe template catalog and Family eligibility read contract.

It does **not** generate an image, upload a template, upload a customer photo, or save a generated result.

## Initial Creative families

The schema allows these roadmap Creative types:

- Wallpaper
- Calendar
- Collage
- Then & Now
- Family Card
- Memory Movie

The catalog does not imply that every type is already executable.

Every returned template includes:

`composition.execution_ready=false`

until a separately reviewed composition engine exists.

## Template metadata schema

Managed source migration:

`migrations_managed/20260924_member_creative_catalog_foundation.sql`

Table:

`member_creative_templates`

Metadata includes:

- template ID
- Creative type
- title / short description
- season tag
- optional start/end date
- photo slot count
- composition mode
- canvas width / height
- output MIME
- logical template asset ID
- logical preview asset ID
- minimum Family MEMORY count
- published state
- sort order

The migration is additive.

Production application requires a separate Owner authorization.

## Asset safety boundary

The catalog stores and exposes only logical asset identifiers.

Examples:

`asset:creative:wallpaper:01`

It does not expose:

- R2 storage keys
- filesystem paths
- signed storage credentials
- arbitrary public URLs
- arbitrary HTML
- arbitrary JavaScript
- executable template code

Actual asset delivery is intentionally a separate future layer.

That layer can map an authorized logical asset ID to a safe private/public delivery mechanism without leaking the underlying storage implementation.

## Family authorization

The Creative catalog requires a server-verified Member session.

It reuses the existing safe MEMORIES read model to verify:

- canonical Customer ID
- explicit Family ID
- current Family access

The request cannot supply or override Customer ID or Family ID.

Cross-Family access fails closed.

## Family eligibility

A template may specify:

`minimum_memory_count`

between 1 and 12.

The catalog returns the template even when the Family has not yet reached that threshold.

Example:

- Then & Now template requires 2 MEMORIES
- Family currently has 1 MEMORY

Response:

- template remains visible
- `eligible=false`
- `memories_needed=1`

This allows a future CREATE UI to explain why a Creative becomes available later rather than silently hiding it.

Eligibility is not a discount, rank, or purchase authorization.

## MEMORY count boundary

Eligibility uses the approved Member MEMORIES reader.

That reader is bounded.

The template schema deliberately restricts:

`minimum_memory_count <= 12`

which is far below the current MEMORIES read limit.

The catalog therefore does not need an additional Family history count query for initial eligibility.

## Publication rule

Only rows where:

- `published=1`
- `deleted_at` is empty

are queried.

Unpublished or soft-deleted templates are never returned.

## Schedule rule

Templates may be:

- evergreen
- start-date constrained
- end-date constrained
- start/end constrained

A scheduled template is returned only when the server-side current date is within the valid window.

Malformed dates or reversed ranges fail closed and are hidden.

The customer request cannot control the production clock through query parameters.

A deterministic internal `as_of` option exists only for tests and verification.

## Composition modes

Allowed metadata modes:

- `single_photo`
- `multi_photo`
- `pair_photo`
- `sequence`

These are declarative metadata only.

They do not execute arbitrary code.

## Output formats

Initial allowed output metadata:

- `image/jpeg`
- `image/png`
- `image/webp`
- `video/mp4`

Allowing a MIME in catalog metadata does not mean the generation engine exists.

For example, Memory Movie can be listed as a future template while:

`execution_ready=false`

remains true.

## Conceptual API

Source-only endpoint:

`GET /api/internal/member/creative/templates`

Requirements:

- server Member session
- GET only
- published template rows only
- deleted rows hidden
- logical asset references only
- Family eligibility derived from authorized MEMORIES
- no request-supplied identity
- no Production route wiring

## HOME boundary

This phase does not activate Creative in HOME.

HOME remains:

`creative_active=false`

A later source-only composition PR may add a curated Creative card to HOME after the catalog foundation is accepted.

## CREATE tab boundary

This phase is backend/read-contract only.

It does not implement final CREATE UI.

A later final UI can consume the approved catalog contract without deciding identity/security rules itself.

## Current exclusions

Not included:

- final CREATE UI
- HOME Creative activation
- template upload endpoint
- template publication endpoint
- asset binary delivery
- private asset signing
- customer photo upload
- customer photo persistence
- browser composition implementation
- generated output saving
- generated output sharing
- Memory Movie rendering
- Production route wiring
- Production schema apply
- Production D1 write
- LINE send
- automatic contact
- reservation creation
- Member session issuance
- Production deploy

## Safety status

Current implementation is:

- read-only catalog
- source-only additive migration
- server Member session only
- exact Family authorization
- published-only templates
- soft-deleted templates hidden
- strict template metadata allowlists
- malformed/inactive templates hidden
- arbitrary HTML = 0
- arbitrary JavaScript = 0
- arbitrary external URL = 0
- storage key exposure = 0
- logical asset IDs only
- browser-side composition preferred
- generation ready = false
- customer photo write = 0
- Production route wiring = 0
- Production write = 0
