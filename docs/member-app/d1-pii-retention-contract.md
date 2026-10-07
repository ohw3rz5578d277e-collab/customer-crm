# D1 PII retention contract

Baseline: 2026-10-07 JST

## Roadmap phase

Phase 4 follows Google Customer Master sync design.

## Goal

Cloudflare D1 may temporarily cache customer PII for operational continuity, but PII must not remain accessible beyond a hard 30-day deadline.

The 30-day clock is based on the PII record/version write time. Reading the cache, refreshing a Member session, or running reconciliation must not reset that clock.

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

- durable Google sync confirmed
- exact canonical Customer ID correspondence verified
- latest profile_version/digest verified

Failure of any check prevents early purge but never extends accessibility beyond day 30.

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

This phase defines planning only. A future purge executor requires a separate Owner-approved exact-SHA write gate, dry-run inventory, bounded target set, audit receipt, and post-write verification.

No automatic Production purge is authorized here.

## Authorization boundary

No Production D1 write/delete, migration apply, Google operation, CRM mutation, Customer ID generation/update/delete/merge, LINE send, deploy, route activation, secret change, R2 operation, UI change, BLACK/MEMORY write, commerce activation, or paid spend is authorized.
