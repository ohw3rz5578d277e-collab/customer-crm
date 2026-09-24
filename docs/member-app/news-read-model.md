# MIZUNO PHOTO MEMBER — NEWS Read Model

Baseline: 2026-09-25 JST

## Purpose

This phase adds the read-only foundation for MEMBER NEWS and the HOME NEWS section.

NEWS is an Owner-authored announcement catalog.

Initial types:

- News
- Campaign
- Service
- Maintenance

This phase handles display only.

It does not send notifications.

## Plain-text contract

NEWS stores and returns:

- title
- short summary
- plain-text body

It deliberately does not support:

- arbitrary HTML
- arbitrary JavaScript
- embedded scripts
- iframe content

This keeps the initial Member content surface simple and avoids turning announcement content into an executable CMS surface.

## Schema

Managed source migration:

`migrations_managed/20260924_member_news_catalog_foundation.sql`

Table:

`member_news_items`

Presentation metadata includes:

- news ID
- news type
- title
- summary
- body text
- optional logical hero asset ID
- optional local navigation path
- publication start/end
- publication date
- HOME featured flag
- published state
- sort order

Production schema application requires separate Owner authorization.

## Asset boundary

Hero images are referenced only by logical asset ID.

NEWS does not expose:

- storage keys
- filesystem paths
- signed storage credentials
- arbitrary image URLs

Asset binary delivery remains a separate future layer.

## Navigation boundary

NEWS may optionally expose only a local path beginning with `/`.

Examples:

- `/news/autumn/`
- `/shop/album/`

External and protocol-relative URLs are rejected.

## Identity

NEWS still requires a server-verified Member session and exact Family authorization.

The content itself is currently global rather than Family-personalized, but access follows the same Member boundary as HOME and other Member read models.

The request cannot override:

- Customer ID
- Family ID
- server date

## Publication

Only:

- `published=1`
- non-deleted rows
- rows inside their valid date window

are returned.

Malformed schedule dates fail closed.

## HOME NEWS

Rows can be marked:

`featured_home=1`

HOME preview is limited to 3 items, preserving Owner-defined order.

This is a display rule only.

## No delivery side effects

A NEWS item being visible does not mean it was sent anywhere.

The read model explicitly reports:

- push delivery ready = false
- LINE delivery ready = false
- automatic contact = false

Future notification delivery requires a separate design, consent rule, and Owner Gate.

## Conceptual API

Source-only endpoint:

`GET /api/internal/member/news`

Requirements:

- server Member session
- GET only
- published/non-deleted rows only
- plain text only
- logical asset IDs only
- local navigation only
- no Production route wiring

## Current exclusions

Not included:

- final NEWS UI
- HOME NEWS activation
- NEWS editor/admin UI
- push notifications
- LINE draft/send
- email send
- automatic campaign delivery
- personalized segmentation
- arbitrary HTML
- external URL redirects
- asset binary delivery
- Production route wiring
- Production schema apply
- Production write
- Member session issuance
- Production deploy

## Safety status

Current implementation is:

- source-only
- read-only
- server Member session only
- exact Family authorization
- plain text only
- published-only
- deleted rows hidden
- schedule fail-closed
- logical asset IDs only
- local navigation only
- arbitrary HTML = 0
- JavaScript = 0
- arbitrary external URL = 0
- storage key exposure = 0
- HOME preview limit = 3
- push delivery = 0
- LINE delivery = 0
- automatic contact = 0
- Production route wiring = 0
- Production write = 0
