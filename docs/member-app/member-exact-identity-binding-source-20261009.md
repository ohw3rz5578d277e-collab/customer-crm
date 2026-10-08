# Member exact identity binding source gate — 2026-10-09

## Baseline

Canonical main at construction:

`7499bc726a34e7c8f736dbdb4896cedcfbf975c4`

This is backend sequence step 1 from `member-runtime-release-gates.md`.

## Customer binding

A Customer Member binding is source-ready only when all of the following are exact and persisted:

- valid Member Identity
- canonical 8-digit Customer ID
- Customer ID source is `customer_crm`
- canonical Customer record is explicitly verified
- persisted Member Identity exactly matches the submitted Member Identity
- persisted Customer ID exactly matches the submitted canonical Customer ID
- exactly one active Member <-> Customer binding exists for that canonical Customer ID
- an explicit non-empty Family ID is present
- persisted Family ID and Family Customer ID exactly match
- exactly one active Customer <-> Family link exists for that canonical Customer ID

Duplicate or mismatched bindings fail closed and route to review. No name/address/phone/email/LINE display-name or other fuzzy identity matching is permitted.

## Prospect binding

A Prospect Member binding is source-ready only when:

- valid Member Identity
- valid Prospect ID
- Prospect status is exactly `prospect`
- persisted Member Identity and Prospect ID exactly match
- exactly one active Member <-> Prospect binding exists
- no canonical Customer ID is attached
- no Family ID is attached

Any Customer or Family identity present on a Prospect before explicit promotion is treated as a scope violation and fails closed.

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
