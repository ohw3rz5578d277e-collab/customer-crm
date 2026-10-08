# D1 PII retention contract

Baseline: 2026-10-09 JST

## Roadmap phase

Member backend sequence step 6 follows the source-only Google sync delivery gate.

## Goal

Cloudflare D1 may temporarily cache customer PII for operational continuity, but:

- a readable cache entry has a maximum freshness TTL of **7 days**;
- PII for one D1 write cycle has a hard absolute retention maximum of **30 days**;
- cache refresh, session refresh, retry, reconciliation, or Google outage must never silently extend the 30-day clock.

The 30-day clock is based only on the original `pii_written_at_ms` for that D1 PII write cycle. A new post-purge cache cycle requires a separately authorized write and a new write-time evidence value.

## Read boundary

The Member cache read gate remains fail closed:

- `now_ms`, `cached_at_ms`, and `pii_written_at_ms` must be safe non-negative integer timestamps;
- cache verification older than the PII write cannot prove the current profile and is rejected;
- exact identity/version verification is required;
- age `>= 7 days` from `cached_at_ms` is not readable;
- a verified cache younger than 7 days may be used during a temporary Google outage;
- age `>= 30 days` from `pii_written_at_ms` always disables PII access and requires purge, before softer cache or outage checks are considered.

## Refresh boundary

A cache refresh plan requires all of:

- Google is available;
- exact identity verification;
- exact profile-version verification;
- Google version equals the expected version;
- Google payload digest equals the expected digest;
- Google synchronization evidence is not older than the local PII write.

A successful refresh may move the **7-day cache freshness** window forward, but the resulting `cache_expires_at_ms` is capped at the original 30-day retention deadline. Refresh does not change `pii_written_at_ms` and does not reset the hard deadline.

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

Before day 30, PII becomes purge-eligible only after all are exactly verified:

- durable Google sync confirmed;
- exact canonical identity correspondence verified;
- latest `profile_version` equality verified;
- latest payload digest equality verified.

Malformed or ambiguous verification evidence cannot authorize early purge.

## Hard 30-day deadline precedence

At or after the hard deadline, purge planning must **not** depend on healthy synchronization/verification metadata.

If `pii_written_at_ms` is valid and the record has reached 30 days:

- only explicit `pii_present === false` proves no purge is needed;
- missing, malformed, stale, or contradictory sync/identity/version/digest evidence cannot suppress purge planning;
- unverified records enter `DEADLINE_RECOVERY` and retain only non-PII retry/audit state;
- audit preview and reconciliation carry the exact `retention_deadline_ms` so overdue work is explicit without copying PII into receipts.

This precedence is required because corrupt control metadata must never turn a 30-day hard maximum into indefinite retention.

## Preserved runtime identity

PII retention does not automatically remove the runtime identity/control records required for:

- canonical Customer ID reference
- Member Identity
- Prospect promotion status
- Family ID/link
- FAMILY PASS / BLACK state
- MEMORIES references and access-control metadata
- consent evidence
- sync event/version/status
- audit/review state

Whether a specific field is PII must be classified explicitly before Production retention code is enabled.

## Fail closed

After the deadline, a stale cache must never be served merely because Google is unavailable.

No identity inference, fallback to another customer, or name/phone/email matching is permitted.

## Executor boundary

This step defines source-only planning, tests, dry-run preview shape, and reconciliation intent. It does **not** execute a D1 mutation.

A future purge executor requires a separate Owner-approved exact-SHA write gate, dry-run inventory, bounded target set, audit receipt, and post-write verification.

No automatic Production purge is authorized here.

## Authorization boundary

No Production D1 read/write/delete, migration/schema apply, Google operation, CRM mutation, Customer ID generation/update/delete/merge, Prospect/Member Production mutation, LINE send, deploy, route activation, secret change, R2 operation, UI change, BLACK/MEMORY write, commerce activation, or paid spend is authorized.
