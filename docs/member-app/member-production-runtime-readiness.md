# MIZUNO PHOTO MEMBER — Production Runtime Readiness Receipt

## Purpose

This gate observes the current Production prerequisites before any customer-facing Member route/runtime activation.

It does not deploy a Worker, enable Member routes, activate Member sessions or LINE Login, change storage bindings, or write customer data.

Before any Cloudflare or Production D1 observation, the workflow now requires the completed Member backend sequence Steps 1–9 to pass an exact-SHA source gate on the same current-main release SHA.

## Exact Owner command

After this workflow is merged:

`/member-runtime-readiness sha=<exact-current-main-sha>`

The bridge is pinned to release-gate issue #26 and the Owner actor.

## Exact-SHA backend sequence prerequisite

The workflow checks the authorized SHA out exactly and rejects current-main drift before any Production observation.

It then runs:

- `member-runtime-step9-final-gate.test.mjs`;
- `member-production-backend-sequence-readiness.test.mjs`;
- `member-production-activation-readiness-assembly.test.mjs`;
- the exact-SHA backend readiness classifier with the authorized release SHA used as both `release_sha` and `step9_verified_sha`.

The backend sequence gate requires:

- exact same release / Step 9 SHA;
- Step 9 final gate PASS;
- Node major exactly 22;
- exact-head checkout evidence;
- workflow exact-head contract evidence;
- all-Member-tests workflow contract evidence;
- Steps 1–8 representative matrix evidence;
- lifecycle cross-contract security evidence;
- Production-default-off evidence.

Malformed or stale SHA evidence, a different Step 9 SHA, missing evidence, or a non-22 Node major fails closed.

This prerequisite executes before Cloudflare authentication, secret-name observation, or Production D1 read-only inspection. A failed backend sequence gate therefore prevents later Production observation steps from starting.

Passing it is technical readiness only. It explicitly keeps Owner Production activation authorization and Production action permission false.

## Read-only evidence

After the exact-SHA backend sequence prerequisite passes, the workflow records:

- exact current main SHA;
- canonical Member source remains default-off;
- `MEMBER_PRODUCTION_OWNER_APPROVED=false`;
- canonical `MEMBER_PRODUCTION_ROUTE_MODE` remains not enabled;
- canonical private-media route mode remains not enabled;
- latest successful gated Production release receipt visible in GitHub Actions;
- whether an exact-main successful Production release receipt exists;
- Production Worker **secret names only** for required Member runtime secrets;
- Member Production D1 migration pending list;
- exact Member schema tables and migration tracking.

No secret value is printed.

## Required Member secret names

Readiness reports presence/missing status for:

- `MEMBER_SESSION_SECRET`
- `MEMBER_LINE_LOGIN_TRANSACTION_SECRET`
- `MEMBER_LINE_LOGIN_CHANNEL_SECRET`
- `MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET`

Missing names are readiness blockers, not permission to create them.

## Schema receipt

The read-only runtime receipt requires:

- zero pending managed migrations;
- all eleven Member/Family tables present;
- all nine Member migrations tracked;
- classification equivalent to `ALREADY_APPLIED_CONFIRMED`.

## Important limitation

A successful backend sequence prerequisite proves only that Steps 1–9 are source/integration-ready on the exact authorized current-main SHA. It does not authorize Production activation or any Production mutation.

A successful GitHub Production deploy receipt proves only that the canonical gated deploy workflow completed successfully for a SHA. It does not authorize route activation and it must not be used to infer secret values or hidden storage configuration.

If no exact-current-main successful deploy receipt exists, the readiness result remains blocked for Production runtime parity.

## Explicitly excluded

This workflow performs none of the following:

- Production Worker deploy;
- `MEMBER_PRODUCTION_OWNER_APPROVED` activation;
- `MEMBER_PRODUCTION_ROUTE_MODE` enablement;
- Member session Production activation;
- LINE Login Production activation or external exchange enablement;
- LINE callback boundary activation;
- public/private storage binding change;
- public/private storage fetch;
- D1 write or delete;
- migration/schema apply;
- CRM customer data write;
- LINE send;
- Google/GAS network send;
- Customer ID or Family ID generation;
- BLACK or MEMORY write/backfill;
- checkout/payment/commerce activation;
- paid spend.

A later activation must remain a fresh exact-SHA, exact-scope Owner Gate.
