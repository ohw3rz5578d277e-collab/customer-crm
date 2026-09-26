# MIZUNO PHOTO MEMBER — Production Runtime Readiness Receipt

## Purpose

This gate observes the current Production prerequisites before any customer-facing Member route/runtime activation.

It does not deploy a Worker, enable Member routes, activate Member sessions or LINE Login, change storage bindings, or write customer data.

## Exact Owner command

After this workflow is merged:

`/member-runtime-readiness sha=<exact-current-main-sha>`

The bridge is pinned to release-gate issue #26 and the Owner actor.

## Read-only evidence

The workflow records:

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
- D1 write;
- CRM customer data write;
- LINE send;
- Customer ID generation;
- checkout/payment/commerce activation;
- paid spend.

A later activation must remain a fresh exact-SHA, exact-scope Owner Gate.
