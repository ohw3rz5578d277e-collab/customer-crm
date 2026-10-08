# Member Google sync delivery source gate — 2026-10-09

Status: source-only planning. Not Production authorized.

## Purpose

Implement backend sequence step 5 without activating Google access: server-side sync identity, idempotency, bounded retry planning, and reconciliation.

## Entry condition

A Google sync candidate is accepted only after a persisted step-4 profile-review decision is verified as `approve_ready` and is bound exactly to:

- `decision_event_id`
- Member Identity
- Customer or Prospect subject type + exact subject ID
- approved profile payload digest
- verified current profile version

The next profile version must be exactly current + 1.

## Stable sync event

`sync_event_id` is deterministically derived from the verified review decision event, Member Identity, exact subject, next profile version, and payload digest.

A retry must reuse the same sync event. A retry must never generate a replacement event for the same accepted mutation.

Existing-event evidence is scoped to exact Member / subject / version / digest. One existing event is accepted only when the persisted event tuple exactly matches. Multiple candidate events are a conflict.

## Payload handling

The ephemeral profile payload is canonicalized and hashed before planning. Its SHA-256 digest must equal the digest carried by the verified review decision.

The planner does not authorize durable raw-profile storage and does not authorize a Google network request.

## Server-only delivery boundary

The delivery planner requires a verified server execution context and the fixed logical destination contract `member_google_master_v1`.

Browser direct Google access remains prohibited.

The HMAC envelope uses only:

- POST
- `/member/profile-sync`
- safe-integer timestamp
- nonce 22–128 URL-safe characters
- `sync_event_id`
- canonical body digest
- managed server secret of at least 32 characters

Maximum accepted clock skew is five minutes. The adapter remains request-construction/verification only; it performs no network send.

## Bounded retries

Total attempts are capped at 4.

Retryable result classes:

- timeout
- network_error
- google_429
- google_5xx

Backoff after attempts 1–3 is fixed at 60 seconds, 5 minutes, and 30 minutes.

Attempt identity must match the persisted sync event and persisted attempt number. After the retry budget is exhausted, the record becomes review-required rather than retrying indefinitely.

Authentication, binding, version, digest, and invalid-request failures are permanent review-required failures, not automatic retries.

## Reconciliation

Scheduled reconciliation compares the persisted expected sync event against exact remote evidence:

- subject type
- subject ID
- profile version
- payload digest

Exact version + digest means `in_sync`.

Remote version ahead, same-version digest conflict, subject mismatch, or malformed remote evidence fails closed to review.

Missing or older remote state may request another attempt only while the same 4-attempt budget remains. Reconciliation never creates a new sync event for the same accepted mutation.

## Authorization boundary

All source outputs keep `send_allowed=false`, `execute=false`, `google_write_allowed=false`, and `production_write_authorized=false`.

This step does not authorize Google/GAS network calls, Customer/Prospect Master writes, Customer History writes, credentials/secrets, Production D1 read/write, migrations, CRM mutations, Customer ID changes, Prospect/Member Production mutation, LINE sends, R2 access, route activation, deploy, commerce activation, BLACK/MEMORY writes, paid spend, or UI changes.
