# MIZUNO PHOTO MEMBER — SHOP UI Foundation

Baseline: 2026-09-25 JST

## Purpose

This phase adds the source-only customer-facing SHOP catalog and product-detail presentation.

It mounts into the canonical Member shell:

`[data-member-mount="shop"]`

The design remains **Luxury Minimal × Family Story**.

The customer goal is discovery:

**写真を、暮らしの中へ。**

## Product categories

The UI reflects the existing approved presentation types:

- ALBUM
- CANVAS
- FRAME
- PRINT
- KOTOBUKI

Category filtering is local browser presentation state only.

It does not issue new requests or change product visibility on the server.

## Product detail

Selecting one already-visible catalog product opens a source-only product detail view.

The detail can show:

- product title
- description
- product type
- season tag
- approved local public image asset

The detail does not create a cart or navigate to a commerce provider.

## Public asset boundary

Only already-resolved local public assets under:

`/member-assets/...`

may be rendered.

Rejected:

- external URLs
- traversal paths
- storage keys
- credentials

## Price boundary

The existing Shop read model is presentation-only.

The UI must not infer a price.

Until an authoritative commerce source is connected:

- no price is displayed
- no inventory claim is displayed
- checkout is disabled
- order creation is disabled
- payment is disabled

A product-side numeric field cannot override the top-level commerce-disabled state.

## Local Shop path boundary

The read model may carry an approved same-origin local Shop path such as:

- `/shop/album/`
- `/寿-kotobuki/`

This source UI preserves that value as safe presentation metadata but does not navigate to it yet.

Navigation becomes a later integration gate after the canonical commerce/page router is decided.

## Family Pass BLACK boundary

The confirmed future BLACK benefit is PHOTO GOODS 10% OFF FOREVER.

This phase does not enforce it.

The UI explicitly keeps:

- benefit enforcement = false
- discount amount application = 0

A later commerce integration must apply benefits against authoritative product/order pricing.

## Identity boundary

The SHOP UI consumes an already-authorized read result.

It strips Customer ID and Family ID from the browser presentation model.

## Production boundary

Not included:

- Production SHOP route wiring
- Production Shop schema apply
- WordPress/WooCommerce synchronization
- Square integration
- authoritative pricing
- inventory
- cart
- checkout
- order creation
- payment
- BLACK discount enforcement
- local Shop page routing activation
- CRM write
- LINE send
- Production deploy

## Safety status

- canonical SHOP shell mount = implemented
- product discovery grid = implemented
- category filter = implemented
- source-only product detail = implemented
- approved local public assets = implemented
- external asset URL = 0
- authoritative price display = 0
- inventory claim = 0
- cart = 0
- checkout = 0
- order creation = 0
- payment = 0
- Family Pass discount enforcement = 0
- local Shop navigation = 0
- Customer ID exposure = 0
- Family ID exposure = 0
- Production route wiring = 0
- Production write = 0
