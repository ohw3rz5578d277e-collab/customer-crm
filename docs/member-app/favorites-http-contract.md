# MIZUNO PHOTO MEMBER — Favorite Mutation HTTP Contract

Baseline: 2026-09-25 JST

## Purpose

This phase defines the future browser-to-server HTTP contract for adding or removing a Favorite.

It does **not** wire the contract into the Production Worker.

## Conceptual route

`POST /api/internal/member/favorites/mutate`

The source handler exists for review and regression testing only.

Production route wiring remains false.

## Route gate

The handler is available only when:

`MEMBER_FAVORITES_MUTATION_ROUTE_MODE=enabled`

Default is disabled.

If disabled, the conceptual path returns a generic not-found response.

This route gate is separate from the executor write gate:

`MEMBER_FAVORITES_WRITE_MODE=enabled`

Both must be enabled before a write can occur.

The write executor also still requires its internal `approved=true` argument, which the HTTP handler supplies only after the server-side route gate and request security checks have passed.

The client cannot send `approved`.

## Signed Member session

The mutation handler does not accept a caller-supplied session object.

It verifies the existing signed:

`__Host-mizuno_member_session`

Cookie through `verifyMemberSessionRequest`.

The Favorite mutation plan then rechecks the current explicit Customer -> Family link immediately before any write.

A valid but stale session therefore cannot bypass current Family authorization.

## CSRF / origin boundary

Mutation requires an exact same-origin `Origin` header.

Rejected:

- missing Origin
- `Origin: null`
- another origin

The Member session cookie is already `SameSite=Lax`; exact Origin validation adds a second browser-side request boundary for state-changing POST requests.

## JSON contract

Content type must be:

`application/json`

Maximum body size:

`2048 bytes`

The body must contain exactly two keys:

```json
{
  "memory_id": "mem_...",
  "desired_favorite": true
}
```

or:

```json
{
  "memory_id": "mem_...",
  "desired_favorite": false
}
```

Rejected extra fields include:

- `customer_id`
- `family_id`
- `approved`
- any other property

The Customer and Family identity always come from the verified signed Member session.

## Declarative desired state

The browser does not send "toggle".

It sends the desired final state.

This preserves the idempotent semantics from the executor:

- false -> true = add
- true -> false = remove
- true -> true = no-op
- false -> false = no-op

## Response boundary

Successful response contains only:

- MEMORY ID
- resulting Favorite boolean
- whether state changed
- whether operation was an idempotent no-op

It does not expose:

- Customer ID
- Family ID
- storage keys
- executor approval state
- internal database metadata

## Current exclusions

Not included:

- Production route wiring
- setting either route/write env mode in Production
- Production Favorite write
- `favorite_mutable=true`
- final MEMORIES UI
- optimistic browser state
- rate limiting
- abuse throttling
- Production schema apply
- LINE send
- Production deploy

Rate limiting / abuse controls should be reviewed before actual customer-facing activation.

## Safety status

Current contract provides:

- POST only
- JSON only
- 2048-byte body limit
- exact request key allowlist
- client identity injection = 0
- client approval injection = 0
- signed Member session Cookie verification
- current Family reauthorization before write
- exact same-Origin requirement
- SameSite=Lax session cookie defense
- separate route mode gate
- separate executor write mode gate
- declarative desired state
- idempotent semantics
- Customer/Family IDs omitted from success response
- Production route wiring = 0
- Production write activation = 0
