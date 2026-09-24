# MIZUNO PHOTO MEMBER — Creative CREATE UI Foundation

Baseline: 2026-09-25 JST

## Purpose

This phase adds the source-only browser UI foundation for the Member **CREATE** tab.

The visual direction follows:

**Luxury Minimal × Family Story**

The screen is intentionally photo-led, quiet, and premium rather than looking like a generic points or utility application.

This change does not activate a Production Member route.

## Customer flow

The source UI supports the following explicit customer actions:

1. choose an eligible Creative template;
2. choose the exact number of authorized MEMORY photos required by that template;
3. request a preview;
4. when the existing browser execution contract becomes runtime-ready, render locally in the browser;
5. save the generated local image through an explicit user gesture.

Nothing auto-executes.

Nothing auto-downloads.

## Template picker

The template picker consumes the existing Creative catalog read result.

It accepts image-output templates only:

- JPEG
- PNG
- WebP

Memory Movie / MP4 remains outside the initial CREATE image UI.

Locked templates remain visible but disabled and can show the remaining MEMORY count needed.

Only trusted local Member asset paths under:

`/member-assets/`

may appear as template preview paths.

Arbitrary external asset URLs are rejected.

## Media picker

The media picker consumes already-authorized Member MEMORY detail results.

It displays image media only.

It never needs:

- Customer ID
- Family ID
- storage key
- R2 path
- signed storage URL
- arbitrary external media URL

The selected media list is represented only by opaque media IDs and their safe MEMORY display metadata.

## Planning boundary

When the selection is valid, the client can build exactly:

```json
{
  "template_id": "...",
  "media_ids": ["..."]
}
```

No identity field or rendering override is included.

The existing server-side Creative planner remains authoritative and must reauthorize every selected media item.

For pair-photo / Then & Now, selected images must come from distinct MEMORY IDs.

## Preview boundary

The preview helper calls an injected composition planner first.

It does not call the renderer unless the returned browser execution contract reports:

`readiness.runtime_ready=true`

Today the integrated runtime is still expected to remain blocked while Production private media delivery is inactive.

Therefore this source UI can be reviewed and tested without activating private-media Production delivery.

## Save boundary

The save helper accepts only:

- local `blob:` object URL
- safe filename
- JPEG / PNG / WebP MIME

The helper runs only when explicitly called from a user action.

It does not:

- auto-download
- upload generated output
- persist generated output
- call a server write endpoint

The caller remains responsible for revoking the local Blob object URL after the download lifecycle completes.

## UI tone

Source CSS defines:

- warm ivory paper background
- charcoal text
- deep green accent
- large editorial heading
- restrained rounded cards
- photo/template-led layout
- sticky preview/save action bar
- mobile-first layout with wider responsive grid

The style remains source-only and is not injected into Production.

## Current runtime state

After this phase:

- Creative catalog: implemented
- composition planner: implemented
- browser execution contract: implemented
- browser renderer source: implemented
- CREATE UI source: implemented
- final Production CREATE route: not wired
- Production private media delivery: inactive
- integrated runtime_ready: false until dependencies are activated
- generated output persistence: disabled

## Safety status

- source-only UI implementation = 1
- Production route wiring = 0
- Production deploy = 0
- Production D1 write = 0
- Production private media activation = 0
- Customer ID input = 0
- Family ID input = 0
- private storage key exposure = 0
- arbitrary external media URL = 0
- auto preview = 0
- auto download = 0
- generated output server write = 0
- LINE send = 0
- paid processing = 0
