# MIZUNO PHOTO MEMBER — Production Runtime Secret Stage Gate

## Purpose

This gate prepares the four Member runtime secrets in a new Cloudflare Worker **version** without sending that version to Production traffic.

It is intentionally split from activation. Creating the staged version does not authorize or perform a Production deployment, Member route activation, LINE Login activation, private-media activation, D1 write, CRM write, or LINE send.

## Why staging is separate from deployment

Cloudflare Worker versions and deployments are separate concepts. The staging gate uses `wrangler versions secret bulk`, which creates a new version containing the secret bindings. It does not use `wrangler secret put`, `wrangler secret bulk`, `wrangler deploy`, or `wrangler versions deploy`.

The workflow snapshots `wrangler deployments status --json` before and after staging and fails if the active Production deployment changes.

## Exact Owner command

After this gate is merged, staging is allowed only through release-gate issue #26:

`/member-runtime-secret-stage sha=<exact-current-main-sha> readiness_run=<successful-readiness-run-id> confirm=STAGE_MEMBER_RUNTIME_SECRETS`

The issue bridge is restricted to the Owner actor and exact command format.

## Required prior receipt

The supplied readiness run must be:

- completed;
- successful;
- a `workflow_dispatch` run;
- from `.github/workflows/member-production-runtime-readiness.yml`;
- on the exact authorized current-main SHA.

Current main is checked again before any Cloudflare mutation.

## Secret material

The staged Worker version contains exactly these Member runtime secret bindings:

- `MEMBER_SESSION_SECRET`
- `MEMBER_LINE_LOGIN_TRANSACTION_SECRET`
- `MEMBER_LINE_LOGIN_CHANNEL_SECRET`
- `MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET`

Three values are generated inside the workflow with Node.js `crypto.randomBytes(48)` and are never printed:

- `MEMBER_SESSION_SECRET`
- `MEMBER_LINE_LOGIN_TRANSACTION_SECRET`
- `MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET`

`MEMBER_LINE_LOGIN_CHANNEL_SECRET` must come from the GitHub Actions secret of the same name because it must match the LINE Login channel configuration. The workflow fails closed if it is missing, shorter than 32 characters, or contains surrounding whitespace.

All four values are masked before use. The temporary JSON secret file is mode `0600` and is removed in an `always()` cleanup step.

## Version and traffic verification

Before staging, the workflow snapshots:

- current Production deployment status;
- current Worker version list.

After `wrangler versions secret bulk`:

1. exactly one new Worker version must exist;
2. the new version must expose all four Member secret **names** in version metadata;
3. existing required CRM/Owner secret binding names must remain present;
4. required DB/service/rate-limit binding names must remain present;
5. active Production deployment status must remain semantically identical after canonical JSON comparison;
6. Production traffic change remains zero.

The staged version ID is safe to record and becomes the only candidate for a later, separately Owner-approved promotion gate.

## Canonical default-off invariant

Before staging, the gate verifies:

- `MEMBER_PRODUCTION_OWNER_APPROVED=false`;
- `MEMBER_PRODUCTION_ROUTE_MODE` is not enabled;
- `MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE` is not enabled.

Staging secret material does not change these source gates.

## Explicitly excluded

This gate does not perform:

- Production Worker deployment;
- `wrangler deploy`;
- `wrangler versions deploy`;
- normal `wrangler secret put/bulk` that immediately changes the deployed Worker;
- `MEMBER_PRODUCTION_OWNER_APPROVED` activation;
- `MEMBER_PRODUCTION_ROUTE_MODE` enablement;
- Member session Production activation;
- LINE Login Production activation or external exchange enablement;
- LINE callback boundary exception activation;
- private/public media route or storage binding activation;
- Member schema apply;
- Production D1 write;
- scheduled runtime D1 write;
- MEMORY/Favorite/BLACK data write;
- CRM customer data write;
- LINE send;
- Customer ID generation;
- checkout/payment/commerce activation;
- paid spend.

Promotion of the staged version to Production traffic requires a new exact-SHA, exact-version, exact-scope Owner authorization.
