# MIZUNO PHOTO MEMBER — App Shell UI Foundation

Baseline: 2026-09-25 JST

## Purpose

This phase adds the source-only visual shell for the Member app.

The canonical customer navigation is:

- HOME
- MEMORIES
- CREATE
- SHOP
- MY

The shell follows the adopted **Luxury Minimal × Family Story** direction.

It is intentionally different from the owner CRM UI and does not reuse the old Today-oriented CRM navigation.

## Responsive navigation

Mobile:

- fixed bottom navigation
- five equal destinations
- safe-area aware
- compact labels
- no horizontal overflow dependency

Desktop / tablet:

- persistent left navigation
- Member brand block
- workspace on the right
- responsive width, not a fixed smartphone canvas

The source breakpoint is 820px.

## Panel contract

The shell renders one trusted mount point for every canonical tab:

- `data-member-mount="home"`
- `data-member-mount="memories"`
- `data-member-mount="create"`
- `data-member-mount="shop"`
- `data-member-mount="my"`

The shell itself does not fetch data and does not inject arbitrary customer HTML.

Feature-specific UI modules can mount into those fixed destinations in later phases.

The already-implemented Creative CREATE UI is expected to mount into the CREATE destination in a later integration phase.

## BLACK theme boundary

BLACK styling is not selected by a client-supplied tier string.

The shell activates BLACK styling only when an already-authorized Family Pass read result has:

`status="ok"`

and:

`family_pass.effective_black=true`

The shell does not receive or expose Customer ID or Family ID.

BLACK styling remains presentation only.

It does not activate benefit enforcement, discount writes, commerce changes, or any entitlement mutation.

## Navigation behavior

The browser helper accepts only the five fixed tab IDs.

Unknown values fail closed to HOME.

Changing tabs only:

- updates active navigation state
- updates ARIA selection state
- hides/shows the fixed panel mount
- optionally emits a local `member:tab-change` event

It does not:

- fetch an endpoint
- change CRM data
- send LINE
- create a reservation
- change a Family Pass
- navigate to an arbitrary external URL

## Production boundary

This is a source-only Member UI foundation.

Not included:

- Production Member route wiring
- Production authentication activation
- Production schema apply
- Production private media activation
- HOME final UI
- MEMORIES final UI
- SHOP final UI
- MY final UI
- live CREATE mounting
- any write activation

## Safety status

- five canonical Member tabs = implemented
- mobile bottom navigation = implemented
- desktop side navigation = implemented
- responsive app shell = implemented
- BLACK visual theme boundary = implemented
- client tier override = 0
- Customer ID exposure = 0
- Family ID exposure = 0
- arbitrary external navigation = 0
- automatic fetch = 0
- automatic write = 0
- LINE send = 0
- Production route wiring = 0
- Production write = 0
