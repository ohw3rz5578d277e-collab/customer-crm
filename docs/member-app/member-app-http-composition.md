# MIZUNO PHOTO MEMBER — App HTTP Composition

Baseline: 2026-09-25 JST

## Purpose

This source-only composition layer provides one Member HTTP entry contract over the already-reviewed source routers.

It composes:

1. LINE Login HTTP contract;
2. private-media HTTP router;
3. read-only Member HTTP router.

The composition is **not imported or wired by the Production Worker entry** in this phase.

## Entry contract

Source function:

`handleMemberAppHttpRequest(request, env, options)`

The composition only considers paths under:

`/api/member/`

All non-Member paths return `null` immediately.

For Member API paths, routing order is deterministic:

1. LINE Login
2. private media
3. read-only Member API

A subrouter that owns the request returns a Response. A subrouter that does not own the request returns `null`, allowing the next source router to inspect it.

Unknown Member API paths fall through as `null`; this source layer does not invent a Production 404 policy.

## LINE Login activation boundary

The composition defaults:

`line_login_approved=false`

It does not infer Owner approval from:

- source presence;
- merge state;
- CI;
- Preview;
- environment configuration;
- prior authorization.

A future trusted Production composition caller would have to pass explicit approval for the already-guarded external exchange executor.

This source phase does not do so.

## Private media storage boundary

The composition accepts:

`storage_adapter`

only as an explicit dependency and passes it to the reviewed private-media router.

It does not:

- discover R2/KV/D1 storage from `env`;
- infer a Production storage binding;
- fetch a Production private object by itself;
- expose storage keys;
- create signed storage URLs;
- redirect to external storage.

## Read-only boundary

The composed read router remains GET-only and signed-session protected.

Its source routes cover:

- HOME
- MEMORIES list/detail
- MY
- SHOP presentation catalog
- NEWS
- CREATE templates
- Favorites read
- Family Pass
- Family Passport
- next memory
- today's memory

Mutation routes remain excluded.

## UI source relationship

The composition health also checks the existing canonical five-tab Member UI source foundation:

- HOME
- MEMORIES
- CREATE
- SHOP
- MY

This is a source-readiness relationship only. The composition does not serve or inject the UI and does not activate a Production page route.

## Explicitly excluded

This phase does not perform or authorize:

- Production Worker entry modification;
- Production route activation;
- Production deploy;
- Production schema apply;
- D1 write;
- historical MEMORY write;
- Favorite write;
- BLACK entitlement write;
- Customer creation;
- Customer ID generation;
- Family creation or auto-link;
- canonical CRM write;
- LINE send;
- private-media Production storage binding/fetch;
- checkout/payment;
- commerce activation.

## Production Readiness Preflight

The Production Readiness Preflight should include this composition as explicit source evidence.

Auth, read-only, and private-media source readiness should each require the corresponding composed source layer to be present.

Production activation remains separately evidence-gated and Owner-gated.
