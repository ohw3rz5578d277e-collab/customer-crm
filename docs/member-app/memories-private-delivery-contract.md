# MIZUNO PHOTO MEMBER — MEMORIES Private Delivery Contract Integration

Baseline: 2026-09-25 JST

## Purpose

MEMORIES already exposes authorized media descriptors without private storage keys.

This phase adds a source-level delivery contract to each exposed media descriptor so a future customer UI can know the approved grant/content flow without inventing endpoints or transport rules.

This phase does not activate delivery in Production.

## Media descriptor

Each MEMORIES media descriptor keeps:

- media_id
- media_type
- role
- sort_order
- dimensions
- private_delivery_required=true

and now adds:

`delivery`

The delivery contract contains only source-level transport metadata.

## Grant contract

`POST /api/internal/member/media/grant`

Body keys:

- media_id

Requirements recorded by the contract:

- signed Member session
- same Origin
- 120-second grant TTL
- Production route not wired

## Content contract

`POST /api/internal/member/media/content`

Body keys:

- media_id
- grant

Requirements recorded by the contract:

- signed Member session
- same Origin
- valid grant
- Production route not wired
- Production storage binding absent
- Production storage fetch absent

## Readiness semantics

The contract explicitly distinguishes source readiness from live availability:

- `source_contract_ready=true`
- `delivery_ready=false`

This prevents a future UI from treating the current source foundation as an active Production media service.

## Privacy boundary

The MEMORIES response does not expose:

- storage_key
- signed storage URL
- provider URL
- bucket name
- storage provider identifier
- Customer ID inside the media descriptor
- Family ID inside the media descriptor

The delivery contract also records:

- storage_key_exposed=false
- signed_storage_url_exposed=false
- external_redirect=false

## List and detail

The contract is applied consistently to:

- MEMORY list cover media
- MEMORY detail media rows

No extra database query is introduced.

The media row already loaded by MEMORIES remains the only presentation source.

## Why storage binding is not added here

Fresh repository inspection found no existing R2/S3/private-object binding configuration that can be safely treated as canonical.

This phase therefore does not invent:

- a bucket name
- an environment binding name
- an R2/S3 account
- a storage provider

A future storage-binding phase must use an actually configured and Owner-reviewed private storage source.

## Production status

Still inactive:

- private media grant Production route
- private media content Production route
- private storage binding
- private storage fetch
- binary delivery
- signed provider URL
- Production deploy

## Safety status

Current integration is:

- source-only
- read-only
- exact existing Member/Family read authorization unchanged
- delivery source contract exposed
- delivery_ready=false
- storage key exposure = 0
- signed storage URL exposure = 0
- external redirect = 0
- Production route wiring = 0
- Production storage binding = 0
- Production storage fetch = 0
- Production write = 0
