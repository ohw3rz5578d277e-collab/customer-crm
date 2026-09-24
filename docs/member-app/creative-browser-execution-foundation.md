# MIZUNO PHOTO MEMBER — Creative Browser Execution Contract Foundation

Baseline: 2026-09-25 JST

## Purpose

The Creative catalog and composition planner already define:

- eligible Creative templates
- exact photo-slot count
- exact selected private media
- canvas dimensions
- output MIME
- logical/public template asset references

This phase converts that safe composition plan into a browser-side execution contract.

It does **not** render an image yet.

## Why browser-side

The intended first Creative workflow is:

1. Member selects an eligible Creative template.
2. Server authorizes the selected private MEMORY media.
3. Server returns a safe composition plan.
4. Browser obtains each authorized private image through the existing grant/content contract.
5. Browser composes the final image locally.
6. Browser offers a local download.

The generated image is not uploaded back to the server in this phase.

Benefits:

- zero server-side image rendering cost
- no generated-output storage requirement
- private photos do not need a new generated-output write path
- local download can remain user-initiated

## Initial output scope

Supported:

- image/jpeg
- image/png
- image/webp

Not supported:

- video/mp4
- memory_movie execution

Video requires separate timing, audio, codec, range-delivery, memory, and export review.

## Fixed layout recipes

Arbitrary layout JSON is not introduced.

The contract maps the existing composition modes to built-in recipes:

- single_photo -> single_full_bleed_v1
- pair_photo -> pair_split_v1
- multi_photo -> balanced_grid_v1
- sequence -> sequence_grid_v1

Photo drawing defaults:

- fit = cover
- focal point = center
- rotation = 0
- opacity = 1

Pair layout is deterministic:

- portrait canvas -> top/bottom
- landscape/square canvas -> left/right

Multi-photo and sequence layouts use a deterministic bounded grid.

## Template layer

The template layer may use only a previously resolved trusted public Member asset:

`/member-assets/...`

Rejected:

- external URLs
- protocol-relative URLs
- path traversal
- percent-encoded path material
- backslashes
- query strings
- fragments
- non-image assets

The template layer covers the full output canvas.

## Private photo layers

Each selected photo carries only the existing public delivery contract:

Grant:

`POST /api/internal/member/media/grant`

Content:

`POST /api/internal/member/media/content`

The browser execution contract does not contain:

- an actual grant value
- storage_key
- signed provider URL
- external storage URL

## Readiness semantics

This phase deliberately distinguishes contract readiness from runtime readiness.

Current state:

- source_contract_ready = true
- browser_engine_contract_ready = true
- browser_renderer_implemented = true
- runtime_ready = false while private media Production delivery remains inactive
- local_download.ready = false while runtime dependencies remain inactive

Current blockers include:

- private_media_delivery_not_active
The renderer-source blocker has been removed by the browser renderer foundation.

A missing public template asset adds:

- template_public_asset_unavailable

This prevents the UI from claiming Creative generation works before the remaining runtime pieces are actually implemented and activated.

## Local download contract

Planned local output mechanism:

`browser_blob_object_url`

Filename is sanitized from the Creative title and receives the MIME-appropriate extension.

Examples:

- .jpg
- .png
- .webp

The contract records:

- server_upload = false
- generated_output_persistence = false

## Metadata/privacy

Initial image processing does not copy:

- EXIF metadata
- source metadata

The execution contract also forbids:

- arbitrary HTML
- arbitrary CSS
- arbitrary JavaScript
- arbitrary layout JSON
- arbitrary external URL

No customer photo server write is introduced.

No generated output server write is introduced.

## Composition planner integration

The existing Creative composition plan now:

- keeps the trusted resolved public template asset when available
- attaches the private media delivery source contract to each selected media descriptor
- attaches the browser execution contract
- reports browser_execution_contract_ready=true when the contract is valid
- keeps browser_composition_implemented=false
- keeps execution.ready=false

## Current exclusions

Not included:

- final CREATE UI wiring
- user-triggered download button wiring
- generated image persistence
- server rendering
- video/movie generation
- fonts/text overlays
- arbitrary filters
- manual crop/focal-point editor
- Production route wiring
- Production private media delivery activation
- Production write
- paid processing

## Safety status

Current foundation is:

- source-only
- read-only
- browser-side execution contract
- renderer source implemented = 1
- renderer auto-execution = 0
- server rendering = 0
- image output only
- fixed built-in layouts
- same-origin template assets only
- private media grant/content contract only
- storage key exposure = 0
- signed storage URL exposure = 0
- arbitrary external URL = 0
- arbitrary HTML/CSS/JS = 0
- customer photo server write = 0
- generated output server write = 0
- Production route wiring = 0
- Production write = 0
