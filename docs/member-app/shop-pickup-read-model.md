# MIZUNO PHOTO MEMBER — Shop Pickup Read Model

Baseline: 2026-09-24 JST

## Purpose

This phase adds the read-only foundation for the future MEMBER SHOP and HOME Shop Pickup sections.

The first goal is product discovery:

**turn your photo into an object**

Initial presentation categories are:

- Album
- Canvas
- Frame
- Print
- KOTOBUKI

This phase intentionally does not become a commerce source of truth.

## Presentation catalog only

Managed source migration:

`migrations_managed/20260924_member_shop_catalog_foundation.sql`

Table:

`member_shop_products`

The table contains only presentation metadata:

- product ID
- product type
- title / description
- season tag
- optional schedule
- logical hero asset ID
- local Shop path
- CTA label
- HOME featured flag
- publication state
- sort order

It does not contain:

- authoritative price
- inventory / stock
- cart
- order
- payment state
- Square transaction data
- WooCommerce order data
- discount enforcement

Those belong to a future commerce integration contract.

## Why price is intentionally absent

The long-term Shop will likely use WordPress/WooCommerce and/or Square commerce data.

Duplicating price into Member Core before a canonical commerce source is connected would create stale-price risk.

Therefore this foundation explicitly returns:

- `pricing.authoritative=false`
- `pricing.amount=null`
- `checkout.ready=false`

The UI must not invent or infer a price from this catalog.

## Family Pass BLACK boundary

BLACK's confirmed future benefit is:

**PHOTO GOODS 10% OFF FOREVER**

This read model does not apply that discount.

Every product reports:

- discount enforcement ready = false
- discount amount = null

A future commerce layer must decide product eligibility and apply the benefit against an authoritative price/order source.

HOME/SHOP may later display benefit messaging only after that contract is reviewed.

## Asset boundary

Products expose only logical asset IDs.

Example:

`asset:shop:album:01`

The catalog does not expose:

- storage keys
- filesystem paths
- signed storage credentials
- arbitrary external image URLs

Actual image asset delivery remains a separate future layer.

## Navigation boundary

A product may expose only a local path beginning with `/`.

Examples:

- `/shop/album/`
- `/寿-kotobuki/`

Rejected examples:

- `https://example.com/product`
- `//evil.example/product`

This keeps redirect/commerce-provider trust decisions out of the presentation catalog.

A future router can map approved local paths to the actual WordPress/WooCommerce page.

## Identity

The MEMBER Shop catalog requires a server-verified Member session.

It revalidates:

- canonical Customer ID
- explicit Family link
- exact Family ID

The request cannot override Customer ID or Family ID.

The catalog currently does not personalize product availability by Family data, but authorization remains consistent with the rest of MEMBER.

## Publication and schedule

Only rows where:

- `published=1`
- `deleted_at` is empty

are queried.

Optional start/end dates allow seasonal product presentation.

Malformed dates or reversed date ranges fail closed and are hidden.

## HOME Pickup

Products may set:

`featured_home=1`

The read model returns at most 3 HOME pickup products, preserving Owner-defined `sort_order`.

This is a presentation limit only.

It does not imply inventory, availability, sale status, or recommendation ranking.

## Conceptual API

Source-only endpoint:

`GET /api/internal/member/shop/products`

Requirements:

- server Member session
- GET only
- published/non-deleted catalog rows only
- local navigation paths only
- logical asset IDs only
- no client-controlled identity
- no client-controlled server date
- no Production route wiring

## Current exclusions

Not included:

- final SHOP UI
- HOME Shop Pickup activation
- authoritative pricing
- inventory
- cart
- checkout
- Square integration
- WooCommerce integration
- WordPress product synchronization
- order creation
- payment execution
- BLACK benefit enforcement
- product asset binary delivery
- Production route wiring
- Production schema apply
- Production write
- LINE send
- automatic contact
- Member session issuance
- Production deploy

## Safety status

Current implementation is:

- source-only
- read-only
- presentation catalog only
- server Member session only
- exact Family authorization
- published-only products
- deleted products hidden
- malformed/inactive products hidden
- logical asset IDs only
- local Shop paths only
- arbitrary external URLs = 0
- private storage keys = 0
- authoritative price claim = 0
- inventory claim = 0
- checkout = 0
- order creation = 0
- payment execution = 0
- Family Pass discount enforcement = 0
- HOME Pickup limit = 3
- Production route wiring = 0
- Production write = 0
