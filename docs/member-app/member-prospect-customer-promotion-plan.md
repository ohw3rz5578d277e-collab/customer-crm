# Prospect to Customer promotion plan

Baseline: 2026-10-09 JST

## Roadmap phase

Member backend sequence **step 7** from `member-runtime-release-gates.md`.

## Goal

Promote one Prospect lifecycle to an already-existing canonical CRM Customer while preserving the same Member Identity, the historical Prospect reference, and append-only history.

Member/FAMILY PASS never creates the canonical Customer ID. Promotion is planning only and does not perform a Production mutation.

## Required exact evidence

A first-time promotion candidate must verify all of the following:

- valid Prospect ID and stable Member Identity;
- exact persisted Prospect ID <-> Member Identity binding;
- the persisted Member Identity record is verified, remains `active`, and still references that exact Prospect;
- the persisted Prospect record is verified and belongs to that exact Member Identity;
- the active Member -> Prospect binding count is exactly `1` and scoped to that exact Prospect;
- the Member Identity has no current canonical Customer binding: persisted Customer state must be explicit `null`, not omitted;
- the Prospect has no prior promoted Customer: `promoted_customer_id` must be explicit `null`, not omitted;
- canonical Customer ID exists in Customer CRM and its persisted source is `customer_crm`;
- canonical Customer match count is exactly `1` and scoped to that exact Customer ID;
- a canonical CRM promotion trigger is verified and bound to the exact Prospect + Member + Customer tuple;
- Prospect status evidence is persisted and bound to the exact Prospect + Member;
- Prospect is still in `prospect` state;
- the target Customer currently has zero Member bindings, with the count scoped to that exact Customer;
- the exact Prospect + Member + Customer promotion-event count is zero;
- consent-history continuity is verified against the exact Member + Prospect;
- acquisition-history continuity is verified against the exact Member + Prospect;
- signup-benefit count is scoped to the exact Member + Prospect and is either zero or one;
- when one signup benefit exists, its exact entitlement ID, Member, Prospect, and current state are verified before carry-forward.

Caller-supplied booleans such as “history preserved” without persisted subject evidence are not sufficient.

Count evidence used by the Step-7 promotion gate must be a non-negative JavaScript safe-integer number. Numeric strings are not coerced into trusted cardinality evidence.

## Deterministic promotion event

The planner derives one deterministic `promotion_event_id` from:

- Prospect ID;
- Member Identity ID;
- canonical CRM Customer ID.

The same tuple therefore produces the same event ID on retry.

## Idempotent completed replay

A completed retry is recognized only when all of these are exact:

- Prospect status is `promoted` and its status evidence is bound to the same Prospect + Member;
- `promoted_customer_id` equals the same canonical Customer ID;
- the Member Identity's current canonical Customer binding equals the same canonical Customer ID;
- the target Customer has exactly one Member binding;
- that persisted Customer binding points to the same Member;
- exactly one promotion event exists for the same Prospect + Member + Customer tuple;
- the persisted promotion event ID and all three subjects match the deterministic event.

When this exact completed state is observed, the planner returns `promotion_already_recorded` with `idempotent_replay=true` and does not plan a second mutation.

Any crossed Member, Customer, Prospect, event, count scope, partial state, or collision is fail closed and routes to review.

## Signup-benefit continuity

Promotion must not reissue or reset the one-time signup benefit.

If no entitlement exists, carry-forward is `null`.

If one entitlement exists, promotion preserves:

- the same entitlement ID;
- the same Member Identity;
- the originating Prospect ID;
- the same current state;
- `state_reset=false`;
- `reissue=false`.

Promotion only associates that existing state with the canonical Customer context. Automatic redemption or commerce write remains disabled.

## Planned transaction shape

A ready plan describes one atomic future transaction with these conceptual operations:

1. bind the stable Member Identity to the existing canonical CRM Customer while retaining the historical Prospect reference;
2. mark the Prospect promoted and bind the same canonical Customer ID;
3. append the deterministic promotion event;
4. preserve consent, acquisition, and signup-benefit history links.

The contract requires:

- `transaction_required=true`;
- `partial_commit_allowed=false`;
- `rollback_on_failure=true`.

This source-only stage still returns `promotion_allowed=false`, `write_allowed=false`, `execute=false`, and `production_write_authorized=false`. A future executor requires a separate exact-SHA Owner authorization.

## Prohibited behavior

- no fuzzy matching by name, date, phone, email, address, LINE display name, or profile data;
- no canonical Customer ID generation outside Customer CRM;
- no second Member account;
- no automatic merge across ambiguous identities;
- no Customer Master PII mutation;
- no Family ID generation or Family mutation;
- no signup-benefit reissue or state reset;
- no Production Prospect/Customer/Member mutation;
- no Production D1 read/write/delete;
- no Google/GAS network operation or Master write;
- no LINE send;
- no R2/private-media operation;
- no Commerce activation;
- no BLACK/MEMORY write.

## Authorization boundary

Source/test/docs planning only. A future promotion executor requires a separate Owner-approved exact-SHA gate, bounded target evidence, atomicity/idempotency verification, audit receipt, rollback/repair path, and post-write verification.
