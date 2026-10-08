# Member Prospect registration / consent reconciliation

Baseline canonical main: `f0b48e64d27d0eaa833a234c174ef06e197dbe33`

Status: source/test/schema-candidate only. Not Production-ready.

## Why this reconciliation exists

The canonical Prospect registration / consent planner from PR #221 reached main before the older divergent PR #220. A first reconciliation PR (#222) was not merged after main drift. PR #223 then advanced main with the source-only Profile Review Queue, and PR #225 later advanced main again with the source-only Google sync delivery gate before PR #224 could be merged under its exact-main authorization.

The reconciliation rule is:

- keep the already-integrated PR #221 planner canonical;
- preserve the merged PR #223 Profile Review Queue unchanged;
- preserve the merged PR #225 Google sync delivery source gate unchanged;
- recover only the missing source-level registration/consent persistence safety compatible with those canonical steps;
- do not perform any Production mutation while reconciling.

## Canonical planner retained

`src/member-prospect-registration-consent-plan.mjs` remains authoritative for step 3. It already provides server-generated Member/Prospect identity evidence, Prospect isolation from Customer/Family identity, SHA-256 profile digest handling, current Terms/Privacy binding, server-recorded acceptance time, deterministic registration/consent event IDs, exact duplicate-count scopes, persisted replay evidence, and an atomic registration transaction requirement while keeping `write_allowed:false`, `execute:false`, and `production_write_authorized:false`.

This reconciliation does not weaken or replace those rules.

## Missing persistence evidence recovered

The canonical planner requires future atomic operations named:

1. `create_member_identity`
2. `create_prospect`
3. `append_registration_event`
4. `append_consent_event`

It also requires persisted registration and consent event evidence for exact replay verification.

`migrations_managed/20261009_member_registration_consent_event_foundation.sql` therefore adds repository-only schema candidates for `member_registration_events` and `member_consent_evidence`.

The registration-event candidate stores immutable evidence including the Member/Prospect IDs, idempotency key, profile digest, document identities, and accepted time. The consent-event candidate binds one consent event to one registration event and carries matching Member/Prospect and document/time evidence.

Neither table stores raw profile name, phone, address, or email.

## Append-only enforcement

Both event tables have source-level `BEFORE UPDATE` and `BEFORE DELETE` abort triggers. Re-registration or future re-consent must append new evidence rather than rewrite historical evidence. This is a schema candidate only; no migration is applied by this PR.

## CI safety scope

`member-app-foundation.yml` is updated only to:

- trigger on this exact new migration candidate;
- allow this exact migration path through the Member-only scope guard;
- include this exact migration candidate in the existing destructive/customer-mutation safety scan.

The allowlist is not broadened to arbitrary migrations.

## Current-main preservation

PR #223 changed only Profile Review Queue source/test/docs files. PR #225 changed only Google sync delivery / server-adapter source/test/docs files. Neither overlaps the four files in this reconciliation. Their merged code remains inherited unchanged from canonical main `f0b48e64d27d0eaa833a234c174ef06e197dbe33`.

## Authorization boundary

This reconciliation does **not** authorize Production deploy or Worker activation, Production D1 read/write, migration/schema apply, Production Member/Prospect/registration-event/consent-event writes, CRM or Customer/Family mutation, Customer ID generation/update/delete/merge, Family ID generation/update/delete/merge, Prospect promotion, profile PII persistence, Google/GAS network send or Master mutation, LINE send, R2 access, route activation, secret/token changes, security-policy changes, commerce activation, BLACK automatic award, or paid spend.

Every Production mutation remains separately Owner-gated by exact SHA.
