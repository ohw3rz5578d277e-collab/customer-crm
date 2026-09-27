# MIZUNO PHOTO MEMBER — Production Runtime Version Promotion Gate

## Purpose

This gate promotes one exact, previously staged Cloudflare Worker version to 100% of `customer-crm-api` Production traffic.

It exists only to separate the traffic-changing action from secret staging. Secret staging remains non-deployed. Promotion requires a new exact-SHA, exact-version, exact-stage-run, exact-scope Owner authorization.

## Cloudflare deployment model

Cloudflare Worker versions and deployments are separate. A version can exist without receiving Production traffic. Promotion uses the documented non-interactive Wrangler versions deployment form:

`wrangler versions deploy <version-id>@100% --name customer-crm-api -y`

The workflow contains exactly one traffic mutation command: `wrangler versions deploy`. It does not use `wrangler deploy`, secret mutation commands, D1 mutation commands, route activation commands, or storage mutation commands.

## Exact Owner command

After this gate is merged, promotion is allowed only through release-gate issue #26:

`/member-runtime-version-promote sha=<exact-current-main-sha> version=<exact-staged-version-id> stage_run=<successful-stage-run-id> confirm=PROMOTE_MEMBER_RUNTIME_VERSION`

The issue bridge is restricted to the Owner actor and exact command format.

## Required prior receipt

The supplied stage run must be:

- completed;
- successful;
- a `workflow_dispatch` run;
- from `.github/workflows/member-production-runtime-secret-stage.yml`;
- on the exact authorized current-main SHA.

Current main is checked again before any Cloudflare traffic mutation.

The promotion gate also reads the exact successful staging job log and requires all of the following receipts from that same run:

- `MEMBER_RUNTIME_SECRET_STAGED_VERSION_ID=<exact-authorized-version-id>`;
- `MEMBER_RUNTIME_SECRET_STAGE=PASS`;
- `PRODUCTION_DEPLOYMENT_UNCHANGED=PASS`.

This binds the exact version candidate to the exact successful staging run rather than validating them independently.

## Exact staged-version verification

Before promotion, the workflow fetches the exact authorized version with `wrangler versions view` and verifies:

- the version message contains the exact authorized main SHA from the staging workflow;
- all four Member runtime secret binding names exist;
- existing required CRM/Owner secret binding names remain present;
- required DB/service/rate-limit binding names remain present;
- secret values are never printed.

## Production baseline fail-closed rules

Immediately before promotion, the workflow snapshots `wrangler deployments status --json`.

It refuses promotion unless:

1. Production currently has exactly one active version;
2. that version receives exactly 100% of traffic;
3. the staged version is not already active.

This prevents promotion from overwriting an unexpected gradual or split deployment state.

## Promotion and post-promotion verification

The only traffic mutation is the exact authorized staged version promoted to 100%.

Immediately after promotion, the workflow verifies:

- Production again has exactly one active version;
- its version ID exactly equals the authorized staged version ID;
- its traffic percentage is exactly 100%;
- Production `/health` returns HTTP 200;
- the release SHA in the health response body and header matches the exact authorized current-main SHA.

## Canonical Member activation remains off

Promotion only places the secret-bearing version into Production traffic. It does not authorize Member functionality.

The workflow verifies before promotion that:

- `MEMBER_PRODUCTION_OWNER_APPROVED=false`;
- `MEMBER_PRODUCTION_ROUTE_MODE` is not enabled;
- `MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE` is not enabled.

The final declaration also records zero activation for Member routes, sessions, LINE Login, and private/public media bindings.

## Explicitly excluded

This gate does not perform:

- `wrangler deploy`;
- secret creation, replacement, or deletion;
- Member secret staging;
- Member source activation;
- Member route activation;
- Member session Production activation;
- LINE Login Production activation;
- LINE callback boundary exception activation;
- private/public media route or storage binding activation;
- Production D1 migration apply;
- Production D1 data write;
- scheduled runtime D1 write;
- MEMORY/Favorite/BLACK data write;
- CRM customer data write;
- LINE send;
- Customer ID generation;
- checkout/payment/commerce activation;
- paid spend.

A future Member feature activation still requires a separate Owner-gated source/config change and release process.
