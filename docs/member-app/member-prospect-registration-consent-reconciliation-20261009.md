# Member Prospect registration / consent reconciliation

Baseline canonical main: `b20c2c60cb38557b0e65fb4a8535b4ac2628cfd1`

Status: source/test/schema-candidate only. Not Production-ready.

## Why this reconciliation exists

Two independent step-3 source branches were created from the same prior main. PR #221 reached main first. PR #220 therefore became divergent and was not force-merged after main drift.

The reconciliation rule is:

- keep the already-integrated PR #221 planner as canonical;
- do not replace it with the older divergent planner from PR #220;
- recover only the missing source-level persistence safety that remains compatible with the PR #221 contract;
- do not perform any Production mutation while reconciling.

## Canonical planner retained from PR #221

`src/member-prospect-registration-consent-plan.mjs` remains authoritative for step 3. It already provides:

- server-generated Member Identity and Prospect identity evidence;
- Prospect isolation from Customer and Family identity;
- strict minimum profile validation;
- SHA-256 profile digest with no raw profile in planner output;
- current Terms / Privacy exact binding and explicit acceptance;
- server-recorded acceptance time;
- deterministic registration and consent event IDs;
- exact duplicate-count scopes;
- explicit persisted replay evidence for idempotent replay;
- atomic registration transaction requirement;
- `write_allowed:false`, `execute:false`, and `production_write_authorized:false`.

This reconciliation does not weaken or replace those rules.

## Missing persistence evidence recovered

The canonical planner requires future atomic operations named:

1. `create_member_identity`
2. `create_prospect`
3. `append_registration_event`
4. `append_consent_event`

It also requires future reads of persisted registration and consent event evidence for exact replay verification.

`migrations_managed/20261009_member_registration_consent_event_foundation.sql` therefore adds repository-only schema candidates for:

- `member_registration_events`
- `member_consent_evidence`

The registration-event candidate stores only the exact immutable evidence needed by the planner contract, including the idempotency key, Member/Prospect IDs, profile digest, document identities, and accepted time.

The consent-event candidate binds the consent event to exactly one registration event and carries the same Member/Prospect and document/time evidence.

Neither table stores raw profile name, phone, address, or email.

## Append-only enforcement

Both event tables have source-level `BEFORE UPDATE` and `BEFORE DELETE` abort triggers. Historical registration and consent evidence is therefore modeled as immutable. Re-registration or re-consent must create a new event rather than rewrite old evidence.

This is a schema candidate only. No migration is applied by this PR.

## CI safety scope

`member-app-foundation.yml` is updated only to:

- trigger on this exact new migration candidate;
- allow this exact migration path through the Member-only scope guard;
- include this exact migration candidate in the existing destructive/customer-mutation safety scan.

The allowlist is not broadened to arbitrary migrations.

## Authorization boundary

This reconciliation does **not** authorize:

- Production deploy or Worker activation;
- Production D1 read/write;
- migration/schema apply;
- Production Member or Prospect creation;
- Production registration/consent event writes;
- CRM write or Customer mutation;
- Customer ID generation/update/delete/merge;
- Family ID generation/update/delete/merge;
- Prospect promotion;
- profile PII persistence;
- Google network send or Customer/Prospect Master mutation;
- LINE send or LINE Login activation;
- R2 object access;
- route activation;
- secret/token changes;
- security-policy changes;
- commerce activation;
- BLACK automatic award;
- paid spend.

Every Production mutation remains separately Owner-gated by exact SHA.
