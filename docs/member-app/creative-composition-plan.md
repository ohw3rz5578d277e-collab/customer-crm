# MIZUNO PHOTO MEMBER — Creative Composition Plan

Baseline: 2026-09-24 JST

## Purpose

This phase defines the safe planning step between:

1. a Member choosing a published Creative template; and
2. a future browser-side composition engine.

It validates the chosen template and chosen Member MEMORY media without generating, uploading, saving, or sharing any output.

The plan is deliberately non-executable.

## Inputs

The planning contract accepts only:

- `template_id`
- `media_ids[]`

The request cannot provide or override:

- Customer ID
- Family ID
- Member identity
- server date / `as_of`
- storage keys
- template asset URLs
- arbitrary composition instructions

Identity comes only from the server-verified Member session.

## Template authorization

The planner reuses:

`readMemberCreativeCatalogForSession`

Therefore the chosen template must already be:

- published
- not deleted
- inside its valid schedule
- valid against the Creative metadata allowlists
- visible to the authorized Family context

The planner uses exact `template_id` equality.

There is no fallback to a similar template.

If the template is not currently available:

`creative_template_not_available`

is returned.

## Eligibility

A visible template may still be locked because the Family has not reached its required MEMORY count.

Example:

- Then & Now requires 2 MEMORIES
- Family has 1 MEMORY

The catalog may show that template as locked, but the composition planner returns:

`creative_template_not_eligible`

No generation plan is created.

## Media authorization

Every selected `media_id` is individually reauthorized through:

`authorizeMemberPrivateMediaAccess`

That authorization requires:

- server Member session
- explicit Family link
- exact Family match
- published parent MEMORY
- non-deleted MEMORY
- non-deleted media
- valid private storage descriptor

A media ID from another Family is not revealed as belonging elsewhere.

It is surfaced to the Creative layer only as:

`creative_media_not_available`

## Private storage boundary

The existing private-media authorization layer must inspect the internal private `storage_key` to validate that the media descriptor is safe.

The Creative plan deliberately strips that field before producing its response.

The plan exposes only:

- media ID
- MEMORY ID
- media type
- role
- width
- height

It does not expose:

- storage key
- R2 object path
- signed asset URL
- raw binary
- public source URL
- filesystem path

Future asset/media delivery must have its own authorization layer.

## Photo slot rule

The number of selected media items must exactly equal:

`template.composition.photo_slots`

Examples:

- Wallpaper / single photo: exactly 1
- Then & Now / pair photo: exactly 2
- 4-photo collage: exactly 4

The initial contract does not silently fill missing slots, duplicate a photo, or discard extra photos.

## Duplicate media

The same `media_id` cannot be selected twice in one plan.

Duplicate selection returns:

`duplicate_media_id`

before composition execution.

## Initial media type

Initial composition planning accepts image media only.

A selected media row with:

`media_type='video'`

returns:

`unsupported_creative_media_type`

This avoids implying that video rendering / transcoding already exists.

Memory Movie remains a roadmap Creative type, but execution is still disabled.

## Pair-photo / Then & Now rule

For `composition.mode='pair_photo'`:

- two authorized images are required; and
- they must come from two distinct MEMORY IDs.

Selecting two different photos from the same MEMORY does not qualify as Then & Now.

It returns:

`pair_photo_requires_distinct_memories`

This rule is based on explicit MEMORY IDs, never photo similarity or date inference.

## Output plan

A successful plan may include:

- exact template ID
- Creative type
- title
- logical template asset IDs
- composition mode
- exact photo slot count
- canvas dimensions
- output MIME
- safe selected media metadata
- selected distinct MEMORY count

It also explicitly reports that execution is not ready.

## Execution state

A successful plan still returns:

- `execution.ready=false`
- `template_asset_delivery_ready=false`
- `private_media_delivery_ready=false`
- `browser_composition_implemented=false`
- `generated_output_persistence_ready=false`

This prevents a read/validation foundation from being mistaken for a functioning generator.

## Conceptual HTTP contract

Source-only endpoint:

`POST /api/internal/member/creative/plan`

Allowed JSON fields:

- `template_id`
- `media_ids`

Any extra field is rejected.

This specifically prevents client injection of:

- `customer_id`
- `family_id`
- `as_of`
- storage descriptors
- arbitrary rendering configuration

The request body is also bounded to a small planning payload.

## Browser composition direction

The product architecture still prefers browser/device-side composition for the first release because it:

- keeps recurring compute cost low;
- avoids sending customer photos to a new third-party generation SaaS;
- fits the zero-cost-first architecture;
- allows reusable Owner-approved templates.

However, this phase does not implement browser rendering.

A future phase must separately define:

1. safe template asset delivery;
2. safe authorized photo delivery;
3. local canvas/render implementation;
4. export handling;
5. optional generated-output persistence.

## No write path

The composition planner performs no:

- D1 insert
- D1 update
- D1 delete
- customer photo upload
- generated output save
- template publication
- Family update
- CRM update
- reservation creation
- LINE send

Every currently used dependency is read-only.

## Final UI boundary

This phase does not implement:

- final CREATE UI
- final template picker
- final media picker
- preview canvas
- download/export UI
- HOME Creative activation

Those can consume this plan later after their own review.

## Production boundary

This source change does not activate Production behavior.

No Production route is wired.

No Production schema is applied.

No Production write is executed.

## Safety status

Current implementation is:

- source-only
- server Member session only
- exact template ID
- exact media IDs
- duplicate media rejected
- exact photo slot count required
- each media individually reauthorized
- cross-Family media hidden
- image input only
- pair-photo requires distinct MEMORY IDs
- private storage key exposure = 0
- arbitrary external URL exposure = 0
- raw photo binary in plan = 0
- generation execution = 0
- browser composition implementation = 0
- customer photo write = 0
- generated output write = 0
- Production route wiring = 0
- Production write = 0
