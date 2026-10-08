# MIZUNO PHOTO MEMBER — Production Runtime Readiness Receipt

## Purpose

This gate observes the current Production prerequisites before any customer-facing Member route/runtime activation.

It does not deploy a Worker, enable Member routes, activate Member sessions or LINE Login, change storage bindings, or write customer data.

The source-level Member backend sequence Steps 1 through 9 must already pass on the exact authorized SHA before this workflow is allowed to observe any Production prerequisite.

In addition to the Step 9 final integration/security gate, the workflow requires the exact-SHA backend sequence readiness classifier on that same authorized current-main SHA before any Production observation.

## Authorization path

Runtime readiness is **bridge-only**. A direct Actions UI `workflow_dispatch` is not an authorized path.

The only authorized path is an exact Owner comment on release-gate issue #26:

`/member-runtime-readiness sha=<exact-current-main-sha>`

The issue-comment bridge must:

1. verify issue #26;
2. verify both the GitHub actor and comment author are the Owner;
3. verify the exact command shape;
4. verify the supplied SHA is the exact current `main` SHA;
5. pass both the exact SHA and exact Owner comment ID into the readiness workflow.

The readiness workflow itself then independently requires the bridge-dispatch bot actor/triggering actor and fetches the exact comment ID from GitHub. It fails closed unless the comment ID, author, issue URL and exact command body all match the expected Owner authorization receipt.

This prevents a direct/manual workflow dispatch or an unrelated workflow dispatch from being accepted as authorized readiness evidence.

## Source prerequisite

Before any GitHub Production release-receipt lookup, Cloudflare authentication, Production secret-name read, or Production D1 read, the workflow must:

1. verify the exact Issue #26 Owner authorization receipt;
2. check out the exact Owner-authorized current-main SHA;
3. confirm that current `main` is still that exact SHA;
4. use Node 22;
5. execute `member_tests/member-runtime-step9-final-gate.test.mjs` successfully;
6. execute `member_tests/member-production-backend-sequence-readiness.test.mjs` successfully;
7. execute `member_tests/member-production-activation-readiness-assembly.test.mjs` successfully;
8. classify the exact authorized SHA with `buildMemberProductionBackendSequenceReadiness(...)`.

The Step 9 final gate re-verifies the source-level Steps 1 through 8 representative security/regression contracts, the exact-PR-HEAD/all-Member-tests CI contract, cross-contract security, and Production-default-off behavior.

The exact-SHA backend sequence classifier uses the same authorized SHA for both `release_sha` and `step9_verified_sha`. It requires all of the following explicit evidence:

- exact release / Step 9 SHA equality;
- Step 9 final gate PASS;
- Node major exactly 22;
- exact-head checkout verified;
- workflow exact-head contract verified;
- all-Member-tests workflow contract verified;
- Steps 1 through 8 representative matrix verified;
- lifecycle cross-contract security verified;
- Production-default-off verified.

Malformed, stale, uppercase, missing or mismatched SHA evidence, missing boolean evidence, a different Step 9 SHA, or a non-22 Node major fails closed.

The backend readiness result must also keep `production_action_allowed=false` and Owner Production activation authorization false. Technical readiness never implies Owner authorization.

If the Owner receipt, Step 9 final gate, backend readiness tests, or exact-SHA backend classifier fails, the workflow stops before any Production observation.

Both PR checkout and exact authorized-SHA checkout use `persist-credentials: false`.

## Exact Owner command

After this workflow hardening is merged, a later **fresh explicit Owner authorization** is still required before dispatching through issue #26:

`/member-runtime-readiness sha=<exact-current-main-sha>`

Merging this source/test/docs hardening does **not** authorize the command or the workflow dispatch.

## Read-only evidence

After the authorization and source prerequisites pass, the workflow records:

- exact current main SHA;
- exact Owner authorization comment receipt;
- exact-SHA backend sequence technical readiness;
- canonical Member source remains default-off;
- `MEMBER_PRODUCTION_OWNER_APPROVED=false`;
- canonical `MEMBER_PRODUCTION_ROUTE_MODE` remains not enabled;
- canonical private-media route mode remains not enabled;
- latest successful gated Production release receipt visible in GitHub Actions;
- whether an exact-main successful Production release receipt exists;
- Production Worker **secret names only** for required Member runtime secrets;
- Member Production D1 migration pending list;
- exact Member schema tables and migration tracking.

No secret value or Owner comment body is printed.

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

A successful backend sequence prerequisite proves only that Steps 1 through 9 are technically source/integration-ready on the exact authorized current-main SHA. It does not authorize Production activation, Production mutation, or Production observation by itself.

A successful GitHub Production deploy receipt proves only that the canonical gated deploy workflow completed successfully for a SHA. It does not authorize route activation and it must not be used to infer secret values or hidden storage configuration.

If no exact-current-main successful deploy receipt exists, the readiness result remains blocked for Production runtime parity.

The runtime-readiness workflow itself performs Production **reads** of secret names and D1 schema/migration state. Those reads require a separate fresh Owner authorization to dispatch even though they do not mutate Production.

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
- CRM customer data write or mutation;
- LINE send;
- Google/GAS network send;
- Customer ID generation/update/delete/merge;
- Family ID generation/update/delete/merge;
- BLACK award/write/backfill;
- MEMORY write/backfill;
- checkout/payment/commerce activation;
- paid spend;
- UI change.

A later activation must remain a fresh exact-SHA, exact-scope Owner Gate with its own rollback and health checks.
