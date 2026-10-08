# MIZUNO PHOTO MEMBER — Production Entry Default-Off Source Manifest

State: source-only private-media runtime wiring stage, 2026-10-08 JST

## Current canonical source state

The Member Production entry is still **default-off**, while the approved read-only private-media storage adapter is now wired in source.

- Production entry path: `src/production-index-crm-customer360-entry.js`
- historical pre-wiring blob: `9791174a5be2aa8861134f6881e2cee451f50966`
- historical default-off blob: `9b325cb61c3fe67006cef2ecc03d5769d7a693a4`
- current stage identity: structural source inspection + fresh exact main SHA + fresh exact observed/expected entry blob equality

The historical blobs above are provenance only. They are **not** asserted as the current Production entry blob.

This is a source-state statement only. It does **not** mean that the Worker carrying this source has been deployed to Production.

## Default-off source invariants

The canonical source inspection requires:

- `MEMBER_PRODUCTION_OWNER_APPROVED=false`
- Member route mode remains a separate `MEMBER_PRODUCTION_ROUTE_MODE` gate
- private-media content route mode remains separately gated and not enabled by this stage
- LINE Login external exchange approval remains `false`
- public asset adapter remains `null`
- private media storage adapter is explicitly wired as the read-only/get-only adapter created from `env.MEMBER_PRIVATE_MEDIA_BUCKET`
- adapter construction performs no R2 object access
- route OFF causes no R2 object fetch
- LINE callback boundary exception remains exact GET only
- callback exception requires both Owner flag and route mode
- Member dispatch remains after the global Production request boundary
- existing CRM dispatch remains the fallback

## Manifest behavior

The manifest now validates the current source stage with fail-closed exact evidence:

1. a fresh observed main SHA must be supplied;
2. it must exactly match the expected main SHA;
3. a valid 40-character observed Production-entry blob SHA must be supplied;
4. a valid 40-character expected Production-entry blob SHA must be supplied;
5. the observed and expected entry blob SHAs must match exactly;
6. only after those exact-baseline checks pass is the source structurally inspected for the wired-but-off Member state.

Missing blob evidence returns `entry_blob_sha_required`.
A blob mismatch returns `entry_blob_mismatch`.
A structurally invalid source fails closed.

Historical pre-wiring/default-off blobs remain recorded only as provenance for earlier stages.

## Separate Production gates

This source stage does not authorize or imply:

- Production deploy
- Worker activation or Production traffic change
- Member route activation
- private-media content route activation
- `MEMBER_PRODUCTION_OWNER_APPROVED` activation
- `MEMBER_PRODUCTION_ROUTE_MODE` enablement
- LINE Login Production activation
- R2 object read/write/list/delete
- Production storage fetch
- public asset runtime wiring
- Member schema apply
- Production D1 read/write
- CRM write
- LINE send
- Customer ID generation/update/delete/merge
- secret/token change
- security policy change
- checkout/payment/commerce
- paid spend

## Current safety state

- source-only private-media adapter wiring applied = yes
- adapter mode = read-only / get-only
- adapter-construction R2 access = 0
- route-OFF R2 object fetch = 0
- public asset adapter = null
- `MEMBER_PRODUCTION_OWNER_APPROVED` = false
- Production runtime deployed by this manifest = no
- Production route activation = no
- Production schema apply = no
- Production D1 write = no
- CRM write = no
- LINE send = no
