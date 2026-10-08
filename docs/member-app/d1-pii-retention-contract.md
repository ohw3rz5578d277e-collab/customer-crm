# D1 PII retention contract

Baseline: 2026-10-09 JST

## Roadmap phase

Member backend sequence step 6 follows the source-only Google sync delivery gate.

## Goal

Cloudflare D1 may temporarily cache profile PII for operational continuity, but the cache has two independent limits:

- verified profile-cache freshness TTL: **7 days**;
- absolute D1 PII maximum retention: **30 days from the original PII write time**.

The 7-day cache TTL is not a retention extension. Reading the cache, refreshing a Member session, receiving a later Google read, or running reconciliation must never reset `pii_written_at_ms` or move the original 30-day deadline.

All retention/cache timestamps are strict non-negative JavaScript safe-integer millisecond values. Numeric strings, NaN, infinity, fractional values, future evidence, and timestamp arithmetic that overflows the safe-integer range fail closed.

## Cache read contract

A cached profile may be read only when all are true:

- `now_ms`, `cached_at_ms`, and `pii_written_at_ms` are valid strict timestamp evidence;
- the absolute 30-day retention deadline has not been reached;
- `cached_at_ms >= pii_written_at_ms`;
- Member/subject identity correspondence is verified;
- profile version/digest correspondence is verified;
- the effective cache expiry has not been reached.

The effective cache expiry is:

`min(cached_at_ms + 7 days, pii_written_at_ms + 30 days)`

The exact expiry boundary is closed: at `now_ms >= cache_expires_at_ms`, cached PII is not readable.

A temporary Google outage does not invalidate a still-fresh verified cache. It also does not permit stale or expired cache access.

## Cache refresh contract

A refresh candidate may be planned only from verified server-side Google evidence. The planner remains default-off and does not itself write D1.

A verified refresh may set cache freshness to the current server time, but its effective expiry is:

`min(now_ms + 7 days, pii_written_at_ms + 30 days)`

A refresh must preserve the original `pii_written_at_ms`. It must not reset the retention clock.

At or after the absolute 30-day deadline, refresh is prohibited and purge/recovery takes precedence even if Google is available and all identity/version evidence otherwise verifies.

## State machine

PENDING_SYNC -> SYNCED -> VERIFIED/PURGE_ELIGIBLE -> PII_PURGED

If the hard deadline arrives before full verification:

PENDING_SYNC/SYNCED -> DEADLINE_RECOVERY

DEADLINE_RECOVERY means:

- cached PII is immediately inaccessible;
- purge/removal or cryptographic destruction is required;
- only non-PII sync/retry/audit metadata may remain;
- reconciliation may continue without restoring the expired PII cache;
- human review is required if durable Google recovery cannot be completed safely.

## Normal early purge

Before day 30, PII becomes purge-eligible immediately after all are true:

- durable Google sync confirmed;
- exact canonical Customer/Prospect subject correspondence verified;
- latest profile version/digest verified.

Failure of any check prevents early purge but never extends accessibility beyond day 30.

## Hard deadline precedence

The exact absolute deadline is:

`pii_written_at_ms + 30 days`

At `now_ms >= hard_purge_deadline_ms`:

- PII access is denied;
- purge is required;
- cache freshness, Google availability, identity-verification failure, missing cache timestamps, or retry state cannot extend the deadline;
- if durable synchronization evidence is incomplete, only non-PII retry/audit metadata may remain.

One millisecond before the absolute deadline remains pre-deadline; the exact boundary itself is expired.

## Preserved runtime identity

PII retention does not automatically remove the runtime identity/control records required for:

- canonical Customer ID reference;
- Member Identity;
- Prospect promotion status;
- Family ID/link;
- FAMILY PASS / BLACK state;
- MEMORIES references and access-control metadata;
- consent evidence;
- sync event/version/status;
- audit/review state.

Whether a specific field is PII must be classified explicitly before Production retention code is enabled.

## Fail closed

After the deadline, a stale cache must never be served merely because Google is unavailable.

No identity inference, fallback to another customer, or name/phone/email matching is permitted.

Malformed or ambiguous timing evidence never grants access or refresh eligibility.

## Executor boundary

This step defines source-only decisions and dry-run planning. A future purge executor requires a separate Owner-approved exact-SHA write gate, dry-run inventory, bounded target set, audit receipt, and post-write verification.

No automatic Production purge is authorized here.

## Authorization boundary

No Production D1 read/write/delete, migration/schema apply, Google/GAS network operation, Customer/Prospect Master mutation, CRM mutation, Customer ID generation/update/delete/merge, Family ID mutation, Prospect promotion, LINE send, deploy, route activation, secret/token change, R2 operation, UI change, BLACK/MEMORY write, commerce activation, or paid spend is authorized.
