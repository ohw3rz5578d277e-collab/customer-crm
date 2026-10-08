# Member Prospect registration + append-only consent source contract

Baseline main: `ca00fee2247bbb69f1c4963e950bb3fca691008a`

Status: source/test/schema-candidate only. Not Production-ready.

## Purpose

Implement backend sequence step 3 from `member-runtime-release-gates.md` without enabling runtime writes:

- create a new Member Identity + Prospect pair only from server-generated IDs;
- keep Customer and Family scope empty during Prospect registration;
- record explicit current Terms and Privacy acceptance as append-only evidence attached to stable Member Identity;
- describe an all-or-nothing registration transaction without executing it.

## Registration identity contract

The planner requires:

- canonical server-generated `member_identity_id` and `prospect_id`;
- literal `ids_server_generated_verified === true`;
- literal `registration_request_server_verified === true`;
- exact zero-collision evidence for Member Identity, Prospect ID and consent event ID;
- each collision count scoped to the exact corresponding ID;
- no requested canonical Customer ID, Family ID or promoted Customer ID.

Malformed/non-scalar IDs, cross-subject count evidence, duplicate IDs and unsafe count values fail closed.

The source plan does not generate Customer ID or Family ID and performs no fuzzy identity linking.

## Consent evidence contract

Registration is ready only when:

- Terms and Privacy document identities are strict scalar version + SHA-256 values;
- the server has verified both document identities;
- `terms_accepted` and `privacy_accepted` are literal boolean `true`;
- `accepted_at` is a canonical millisecond UTC timestamp;
- the server confirms that `accepted_at` was recorded server-side;
- the existing Member consent foundation accepts the same evidence as append-only consent.

The consent evidence is attached to stable Member Identity and also records the Prospect ID as immutable registration context. Prospect -> Customer promotion must preserve this historical row rather than rewrite it.

## Append-only schema candidate

`migrations_managed/20261009_member_consent_evidence_foundation.sql` adds only the candidate `member_consent_evidence` table plus indexes and two protection triggers.

The triggers abort every `UPDATE` and `DELETE` against `member_consent_evidence`. New consent or re-consent therefore appends a new event row instead of changing historical evidence.

This migration file is repository source only. It is **not** authorization to apply any schema change in Production.

## All-or-nothing registration contract

A ready source plan describes three inserts:

1. `member_identities` — Member Identity linked to the new Prospect, with canonical Customer ID `NULL`;
2. `member_prospects` — Prospect linked to the same Member Identity, with promoted Customer ID `NULL`;
3. `member_consent_evidence` — immutable Terms/Privacy acceptance evidence.

The future executor must run all three inside one transaction and require exactly one affected row for each statement. Any failure must roll back the whole registration. Blind retry without re-reading collision state is not allowed.

The planner itself returns:

- `write_allowed:false`
- `execute:false`
- `production_write_authorized:false`
- `execution_requires_separate_gate:true`

## Identity and side-effect boundary

This stage does not:

- create/update/delete/merge a CRM Customer;
- generate or mutate Customer ID;
- generate or mutate Family ID;
- promote a Prospect;
- rewrite prior consent evidence;
- write Customer Master;
- send LINE;
- call Google;
- access R2;
- grant FAMILY PASS / BLACK / MEMORIES or other entitlement;
- activate Member Production routes.

## Authorization boundary

This source-only stage does **not** authorize:

- Production deploy or Worker activation;
- Production D1 read/write;
- migration apply;
- Production Prospect/Member/consent writes;
- CRM write/mutation;
- Customer ID / Family ID generation or mutation;
- LINE send or LINE Login activation;
- Google network send or Customer Master mutation;
- R2 object access;
- route activation;
- secret/token changes;
- security-policy changes;
- commerce activation or paid spend.
