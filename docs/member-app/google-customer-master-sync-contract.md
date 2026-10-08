# Google Customer Master sync contract

Baseline: 2026-10-07 JST

## Roadmap phase

Phase 3 follows the Member Identity / Prospect foundation. This document is source-only and does not deploy GAS, create a spreadsheet, or access Google.

## Roles

- Customer CRM remains the sole issuer/owner of canonical Customer ID.
- Google Customer Master is the target long-term canonical PII profile store.
- Customer History is append-only audit history.
- Cloudflare remains the identity/runtime/sync-control layer and may hold only bounded PII cache under the retention contract.
- The customer browser never talks directly to Google Sheets or GAS.

## Customer Master

One current row per canonical Customer ID.

Minimum control fields:

- customer_id
- profile_version
- updated_at
- last_sync_event_id
- source

Profile fields may include name, phone, postal/address, email, and other explicitly approved customer profile fields.

Customer ID is the only update key. Name, phone, email, address, LINE display name, or fuzzy similarity must never select the row to overwrite.

## Customer History

Every accepted profile change appends an immutable history record containing at least:

- history_event_id
- customer_id
- profile_version
- changed_at
- source
- changed_fields
- before_digest / after_digest or field-level before/after values according to the later privacy design
- actor/reference metadata that does not expose credentials

History rows are append-only. Updating Customer Master must never delete or rewrite prior history.

## Server-to-server boundary

The browser authenticates to the Member service. The server derives Member Identity and canonical Customer ID from the authenticated session/binding.

Only the server may call the Google integration endpoint.

The browser must never receive:

- Spreadsheet ID
- GAS deployment secret
- service-account credential
- HMAC/shared secret
- Google API credential
- another customer's Customer ID as an authorization selector

A future GAS endpoint must authenticate server-to-server requests. Replay resistance must include timestamp plus unique event/nonce semantics. Secrets belong in managed secret storage, not in the spreadsheet.

## Sync event

Each accepted profile mutation receives a unique sync_event_id and monotonic profile_version for that customer.

The Google side must be idempotent:

- same sync_event_id replay -> no duplicate history row
- older profile_version -> reject/no overwrite
- same profile_version with different payload digest -> conflict/review
- next valid profile_version -> append history then update master
- wrong/missing canonical Customer ID -> reject

The operation must use locking/serialization appropriate to GAS/Google so concurrent requests cannot silently overwrite each other.

## Delivery target and reconciliation

Primary target: event-driven synchronization shortly after an accepted change.

Safety net: scheduled daily reconciliation checks unsynchronized/out-of-date records and retries safely by sync_event_id/version.

"Within one day" is the maximum normal synchronization objective, not the normal latency target.

## Read path

Member profile read:

browser -> Cloudflare Member server -> authenticated session -> Member Identity -> exact canonical Customer ID -> server-to-server Google read -> current Customer Master row.

A bounded seven-day cache may serve profile reads. Cache invalidates/refreshes immediately after a confirmed edit. Background refresh is preferred before expiry.

If Google is unavailable and no valid cache exists, fail closed with profile unavailable. Never guess identity and never return another customer's row.

## Review queue

Identity/version conflicts do not overwrite Customer Master. They create or retain a review-required state for administrator handling in Customer CRM.

Review approval must itself produce an auditable sync event; direct spreadsheet editing is not the normal administrative write path.

## Independent backup

Customer History in the same spreadsheet is audit history, not an independent backup.

A later backup phase must create a physically/logically separate backup artifact (for example a versioned export or separate restricted Drive file). Loss of the primary workbook must not destroy both current state and its only backup.

## Authorization boundary

This contract does not authorize Google Sheet creation, GAS deployment, Google API calls, credentials/secrets, Production D1 read/write, CRM customer mutation, Customer ID generation, LINE send, Production deploy, route activation, R2 access, UI changes, BLACK/MEMORY writes, commerce activation, or paid spend.
