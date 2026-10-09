# MIZUNO PHOTO MEMBER — Production Lifecycle Schema Apply Gate

This gate may apply **only the two lifecycle migrations** that follow the already-applied original 2026-09-24 Member schema.

It is intentionally separate from Worker deploy, route activation, secret changes, LINE Login, R2 operations, CRM mutation, BLACK/MEMORY writes, and commerce activation.

## Preconditions

Every apply requires all of the following:

1. the exact authorized SHA is still the current `main`;
2. the referenced `Member Production schema preflight` run is `completed/success`, is a `workflow_dispatch`, and has the same exact SHA;
3. the Owner authorization is an exact comment on release-gate issue #26;
4. the exact confirmation token is `APPLY_MEMBER_LIFECYCLE_SCHEMA`;
5. immediately before apply, the original nine Member migrations are already tracked and not pending;
6. all original eleven Member/Family tables exist as real SQLite tables;
7. exactly these two lifecycle migrations are pending and nothing else is pending:
   - `20261007_member_identity_prospect_foundation.sql`
   - `20261009_member_registration_consent_event_foundation.sql`
8. none of the lifecycle schema objects exists yet and neither lifecycle migration is tracked.

Lifecycle schema objects means all six tables, seven named indexes, and four append-only triggers introduced by the two migrations. A same-named object with the wrong SQLite `type` or wrong `tbl_name` blocks the apply.

Any mismatch fails closed **before** the D1 schema write.

## Exact Owner command

After a new exact-current-main lifecycle schema preflight succeeds:

`/member-schema-apply sha=<exact-current-main-sha> preflight_run=<successful-preflight-run-id> confirm=APPLY_MEMBER_LIFECYCLE_SCHEMA`

The older broad confirmation token `APPLY_MEMBER_SCHEMA` is intentionally rejected.

## Authorized write scope

After all gates pass, the workflow contains exactly one managed D1 apply command:

`d1 migrations apply customer-crm-db --remote`

The immediately preceding classifier guarantees that the pending set is exactly the two lifecycle migrations above. Unknown migration files, old Member migrations becoming pending again, a one-of-two pending state, partial lifecycle schema, wrong-type/wrong-table index or trigger collisions, or inconsistent tracking all block the apply.

The contract also statically audits both migration files as schema-only: after removing the exact intended append-only `CREATE TRIGGER ... SELECT RAISE(...)` blocks, top-level `INSERT`, `UPDATE`, `DELETE`, `REPLACE`, `TRUNCATE`, `ALTER`, `DROP`, or any non-`CREATE TABLE/INDEX` statement is rejected.

The workflow never deploys a Worker or activates a route.

## Post-apply verification

After the single managed migration apply, the workflow re-reads pending migrations and schema receipts and requires:

- no pending migration;
- all 17 required Member tables present as actual SQLite tables;
- all seven lifecycle indexes present with their exact table binding;
- all four append-only triggers present with their exact table binding;
- all 11 Member migration receipts tracked;
- lifecycle schema status `LIFECYCLE_APPLY_CONFIRMED`.

If post-apply verification fails, the workflow reports failure and does not perform any additional mutation.

## Lifecycle schema contents

The two authorized migrations add:

- `member_identities`
- `member_prospects`
- `member_customer_invitations`
- `member_profile_change_review_queue`
- `member_registration_events`
- `member_consent_evidence`

They also add seven named indexes. The consent migration creates four append-only UPDATE/DELETE rejection triggers whose bodies are restricted to `SELECT RAISE(ABORT, ...)`.

No canonical Customer ID generation, customer merge/delete, Customer/Family linkage, raw profile PII storage, LINE send, Google/GAS write, or R2 side effect is included.

## Concurrency and authorization

The Issue #26 bridge rejects the apply rather than queueing behind another Production mutation. The apply workflow also validates the exact Owner comment, exact current-main SHA, successful exact-SHA preflight receipt, and exact confirmation token.

## Explicitly not included

This gate does not authorize:

- Worker deploy or activation;
- route activation;
- LINE Login Production activation;
- secret creation/change;
- R2 bucket/object operation;
- Customer/Family/Member/Prospect data mutation beyond schema creation;
- Customer ID or Family ID generation/change/delete/merge;
- BLACK award/write/backfill;
- MEMORY write;
- cache refresh or PII purge execution;
- commerce/discount enforcement;
- security policy changes;
- paid spend;
- UI changes.

Schema apply remains a separate, one-time, exact-SHA Owner-authorized Production write.
