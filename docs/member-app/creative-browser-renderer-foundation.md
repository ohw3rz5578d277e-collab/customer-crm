# MIZUNO PHOTO MEMBER — Creative Browser Renderer Foundation

Baseline: 2026-09-25 JST

## Purpose

The previous Creative browser execution contract defined exactly what a safe browser render is allowed to do.

This phase implements that renderer as a source-only browser module.

It is still not wired into the final customer UI and cannot become runtime-ready while private media Production delivery remains inactive.

## Renderer flow

When supplied with a runtime-ready Creative execution contract, the renderer:

1. validates the complete execution contract;
2. creates a browser canvas;
3. loads each authorized private photo through the grant/content contract;
4. draws private photos using center-cropped `cover`;
5. loads the trusted same-origin template asset;
6. draws the template last as the top overlay;
7. exports JPEG / PNG / WebP to a Blob;
8. creates a local Blob object URL;
9. returns the local output descriptor.

It does not automatically click or download anything.

A user gesture remains expected for the final save action.

## Draw order

Fixed order:

**photos -> template overlay**

Layer roles are now explicit:

- photo layer: `role=photo`
- template layer: `role=overlay`

The template layer is fixed at z-index 1000.

Photo layers use deterministic ascending z-index values.

## Fit behavior

Only:

`cover + center`

is supported.

The renderer computes a source crop rectangle and uses canvas `drawImage`.

Not supported:

- arbitrary crop data
- manual focal point
- rotation
- arbitrary opacity
- arbitrary filters
- arbitrary CSS transforms

## Contract validation

Before rendering, the browser module revalidates:

- execution environment = browser
- server rendering = false
- canvas dimensions bounded 320–8192
- JPEG / PNG / WebP output only
- renderer implementation flag = true
- runtime_ready = true
- local_download.ready = true
- trusted `/member-assets/` template path
- exact template overlay role/z-index
- exact private grant/content routes
- same-origin/session requirements
- fixed photo roles/indexes/z-order
- no arbitrary layout JSON
- no arbitrary HTML/CSS/JavaScript
- no filters/metadata copy
- Blob/object-URL download only
- no server upload/persistence

A modified client contract therefore does not automatically become executable.

## Default browser runtime

A default same-origin runtime adapter is included.

Public template asset fetch:

- relative `/member-assets/...` only
- credentials = same-origin
- cache = no-store
- redirects = error

Private photo fetch:

1. POST `/api/internal/member/media/grant` with `media_id`
2. receive short-lived grant
3. POST `/api/internal/member/media/content` with `media_id + grant`
4. decode returned image Blob

The grant is never placed in:

- URL path
- query string
- fragment

## Image input safety

Allowed MIME types:

- image/jpeg
- image/png
- image/webp

Maximum fetched image size:

**50 MiB**

The runtime validates:

- HTTP success
- Content-Type allowlist
- Content-Length when present
- final Blob size
- final Blob MIME consistency

Fetch redirects are disabled.

## Canvas runtime portability

The runtime prefers:

- OffscreenCanvas when available

and otherwise supports:

- document-created HTMLCanvasElement

The core renderer receives an injected runtime boundary, which makes security and rendering behavior testable without a real browser.

## Output

The renderer returns:

- Blob
- local Blob object URL
- safe filename
- MIME
- width
- height
- `revoke_object_url_required=true`

The caller must eventually revoke the object URL.

The renderer does not auto-download.

## Current readiness

Source status after this phase:

- execution contract: implemented
- browser renderer: implemented
- default same-origin browser runtime: implemented
- final customer UI wiring: false
- Production private media delivery: false
- integrated Creative runtime_ready: false
- integrated local_download.ready: false

The remaining runtime blocker is the inactive private media delivery/storage route, not renderer source code.

## Current exclusions

Not included:

- final CREATE UI
- user-triggered download button wiring
- Production private storage binding
- Production private media route activation
- server-generated outputs
- output persistence/history
- font/text overlays
- crop editor
- memory movie/video renderer
- paid processing

## Safety status

Current foundation is:

- source-only
- renderer implemented
- no auto execution
- no final UI wiring
- same-origin fetch only
- redirects disabled
- grant never in URL
- no-store fetching
- fixed 50 MiB input cap
- fixed photo -> overlay draw order
- cover/center only
- JPEG/PNG/WebP only
- local Blob/object URL only
- automatic download = 0
- customer photo server write = 0
- generated output server write = 0
- Production private media delivery activation = 0
- Production route wiring = 0
- Production write = 0
