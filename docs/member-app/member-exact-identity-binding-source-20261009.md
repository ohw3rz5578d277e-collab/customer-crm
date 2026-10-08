# Member exact identity binding source gate — 2026-10-09

## Baseline

Canonical main at construction:

`7499bc726a34e7c8f736dbdb4896cedcfbf975c4`

This is backend sequence step 1 from `member-runtime-release-gates.md`.

## Evidence shape

All textual identity evidence (Member ID, Prospect ID, Customer ID, Family ID, status, and source fields) must arrive as scalar strings. Arrays, objects, boxed/string-like values, or other non-string shapes are not normalized into accepted identity evidence.

## Customer binding

A Customer Member binding is source-ready only when all of the following are exact and persisted:

- valid Member Identity
- the persisted Member Identity record is explicitly verified and has status `active`
- canonical 8-digit Customer ID
- Customer ID source is `customer_crm`
- canonical Customer record is explicitly verified
- persisted Member Identity exactly matches the submitted Member Identity
- persisted Customer ID exactly matches the submitted canonical Customer ID
- exactly one active Member <-> Customer binding exists for that canonical Customer ID
- the binding-count evidence is explicitly scoped to that same canonical Customer ID
- an explicit non-empty Family ID is present
- the persisted Family group record is explicitly verified and has status `active`
- persisted Family ID and Family Customer ID exactly match
- the exact Customer <-> Family link is explicitly verified as active/non-deleted
- exactly one active Customer <-> Family link exists
- the active-family count evidence is explicitly scoped to both the same canonical Customer ID and the same Family ID

This prevents a soft-deleted historical Family link from being paired with the active-link count of a different Family.

Duplicate, inactive, missing, malformed-shape, or mismatched identity evidence fails closed. Binding-count scope mismatches route to review. No name/address/phone/email/LINE display-name or other fuzzy identity matching is permitted.

## Prospect binding

A Prospect Member binding is source-ready only when:

- valid Member Identity
- the persisted Member Identity record is explicitly verified and has status `active`
- valid Prospect ID
- Prospect status is exactly `prospect`
- persisted Member Identity and Prospect ID exactly match
- exactly one active Member <-> Prospect binding exists
- binding-count evidence is explicitly scoped to that same Prospect ID
- persisted canonical Customer association is explicitly supplied as literal `null`
- persisted Family association is explicitly supplied as literal `null`
- persisted `promoted_customer_id` association is explicitly supplied as literal `null`

Missing scope fields are not interpreted as evidence of absence. Undefined or omitted Customer/Family/promotion evidence fails closed as `missing_prospect_scope_evidence`. Any non-null Customer, Family, or promoted-Customer association before explicit promotion is a scope violation and routes to review. Disabled or review-required Member Identity records also fail closed.

## Safety

This phase only produces normalized source evidence. It performs no mutation.

- Production deploy: no
- Production Worker activation: no
- Production D1 read/write: no
- migration apply: no
- Customer ID generation: no
- Family ID generation: no
- CRM mutation: no
- LINE send: no
- Google network send / Customer Master mutation: no
- R2 access: no
- route or secret activation/change: no
- automatic merge: no
- fuzzy identity linking: no
- commerce activation / paid spend: no

All returned ready plans keep `write_allowed=false` and `execution_requires_separate_gate=true`.
