# MIZUNO PHOTO MEMBER — Production Schema Apply Gate

This gate exists only to apply the nine source-managed MIZUNO PHOTO MEMBER migrations after a fresh exact-SHA read-only Production schema preflight has succeeded.

It is intentionally separate from the normal CRM Production deploy workflow.

## Preconditions

Every apply requires all of the following:

1. the exact authorized SHA is still the current `main`;
2. the referenced `Member Production schema preflight` run is `completed/success`, is a `workflow_dispatch`, and has the same exact SHA;
3. the Owner authorization is an exact comment on release-gate issue #26;
4. the exact confirmation token is `APPLY_MEMBER_SCHEMA`;
5. immediately before apply, all nine Member migrations are pending;
6. no unrelated migration is pending;
7. none of the eleven expected Member/Family tables exists yet;
8. none of the nine Member migrations is tracked yet.

Any mismatch fails closed before the schema write.

## Exact Owner command

After this workflow is merged and a new exact-current-main read-only preflight succeeds:

`/member-schema-apply sha=<exact-current-main-sha> preflight_run=<successful-preflight-run-id> confirm=APPLY_MEMBER_SCHEMA`

The bridge is pinned to issue #26 and Owner actor `ohw3rz5578d277e-collab`.

## Authorized write scope

The workflow performs exactly one managed D1 migration apply after the pre-apply gates pass:

`d1 migrations apply customer-crm-db --remote`

Because the gate requires the pending set to be exactly the nine known Member migrations, the apply is restricted to those migrations.

The nine migrations remain contract-checked as additive CREATE TABLE/INDEX IF NOT EXISTS only and must not contain ALTER, DML, DROP, REPLACE, TRUNCATE, FOREIGN KEY, or REFERENCES.

## Post-apply verification

After apply, the workflow requires:

- zero pending managed migrations;
- all eleven expected Member/Family tables present;
- all nine Member migrations tracked;
- classification `ALREADY_APPLIED_CONFIRMED`.

## Explicitly not included

This gate does not:

- deploy the Worker;
- activate Member routes;
- activate Member sessions;
- activate LINE Login;
- enable external LINE token exchange;
- bind or fetch private/public media storage;
- write CRM customer data;
- send LINE messages;
- generate Customer IDs;
- activate checkout/payment/commerce;
- authorize paid spend.

Schema apply remains a separate Owner-authorized Production write.
