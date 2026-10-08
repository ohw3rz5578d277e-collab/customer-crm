# Member profile change review queue contract

Baseline main for runtime step 4 hardening: `b20c2c60cb38557b0e65fb4a8535b4ac2628cfd1`

Status: source/test/docs only. Not Production-ready.

## Purpose

Backend sequence step 4 requires every ambiguous profile change to pass through a review queue **before any Customer/Prospect Master update**.

Identity, version, binding, or Prospect-promotion ambiguity must never overwrite Customer/Prospect Master automatically. The customer-facing response may acknowledge receipt without exposing internal identity details, while the candidate change remains blocked pending administrator review.

## Review reasons

The source gate accepts only:

- `identity_mismatch`
- `version_conflict`
- `promotion_collision`
- `binding_mismatch`

Unknown, non-scalar, or whitespace-coerced reason evidence fails closed.

## Exact identity and subject binding

A new review plan requires:

- a valid exact Member Identity ID;
- literal `member_identity_authenticated:true`;
- the exact authenticated Member Identity ID matching the requested Member Identity;
- exactly one review subject:
  - canonical Customer ID, or
  - Prospect ID.

Customer and Prospect subjects cannot be supplied together. Missing, malformed, or mixed subject evidence fails closed.

The submitted profile must be an object. The durable candidate shape stores only a canonical SHA-256 payload digest. Raw submitted profile content remains ephemeral and no durable raw-PII storage is authorized by this stage.

## Deterministic queue identity and idempotency

Each queue request requires a canonical queue idempotency key.

The deterministic review ID is derived from the exact tuple:

- queue idempotency key;
- Member Identity ID;
- reason code;
- subject type;
- subject ID;
- submitted-profile payload digest.

Before a new review can be planned, exact-scoped existing-review count evidence must be bound to the same Member Identity, idempotency key, and payload digest.

- count `0`: a new source-only review candidate may be planned;
- count `1`: the request is treated as an idempotent replay only if persisted review evidence exactly matches review ID, Member, reason, subject, payload digest, and `pending` status;
- any partial, mismatched, duplicate, malformed, or ambiguous state fails closed and is review-required.

Queue persistence itself still requires a separate execution/Production gate.

## Administrator decision binding

An approve/reject plan requires exact persisted pending-review evidence for:

- review ID;
- Member Identity ID;
- reason code;
- subject type and subject ID;
- payload digest;
- pending status.

The administrator actor must be explicitly verified and supplied as a strict scalar actor ID. Every decision requires an idempotency key and derives a deterministic audit decision event ID from the review, decision, actor, and payload digest.

### Reject

Rejection:

- leaves Master unchanged;
- requires audit history;
- does not permit Google send;
- requires a separately authorized executor to persist the decision.

### Approve

Approval additionally requires:

- literal identity re-verification bound to the exact Member Identity;
- literal latest-version verification bound to the exact Customer/Prospect subject;
- a valid current profile-version evidence value.

Even after approval, this step **does not write Master directly**. Approval requires a brand-new step-5 Google sync event. The uncertain or previously blocked write is never replayed directly.

## Master-write boundary

For both queue creation and administrator decision planning:

- `master_write_allowed:false`
- `google_send_allowed:false`
- `queue_write_allowed:false`
- `production_write_authorized:false`

A queue entry or approval is not permission to mutate Customer/Prospect Master.

The normal administrator surface remains Customer CRM. Direct Google Sheet editing is not the review workflow.

## Authorization boundary

This stage does **not** authorize:

- Production queue write;
- Customer/Prospect Master write;
- Google network send;
- CRM mutation;
- Customer ID generation/update/delete/merge;
- Prospect/Member Production mutation;
- LINE send;
- Production D1 read/write;
- migration/schema apply;
- R2 object access;
- route activation;
- secret/token change;
- security-policy change;
- Commerce activation;
- BLACK automatic award;
- paid spend;
- UI changes.

All Production mutations remain separately Owner-gated.
