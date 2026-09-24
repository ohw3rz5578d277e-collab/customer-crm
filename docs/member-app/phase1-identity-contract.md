# MIZUNO PHOTO MEMBER — Phase 1 Identity Contract

Baseline: 2026-09-24 JST

## Purpose

Define the CRM-side family/household identity contract needed by the future MIZUNO PHOTO MEMBER app without changing Production data or interfering with the current LINE-history / sales reconciliation work.

The Member app is a separate customer-facing product. Customer CRM remains the canonical source of customer identity.

## Cost principle

The Member app must be designed zero-cost-first.

- prefer existing Cloudflare free-tier services and the existing WordPress / LINE / Amazon Photos stack;
- do not add a paid SaaS dependency when an existing or free-tier component can provide the same core function;
- browser-side composition is preferred for Creative generation from uploaded templates and customer photos;
- native App Store / Google Play distribution is not required for the first release; PWA/Web is the default;
- new infrastructure that can generate recurring charges requires an explicit cost review before adoption.

## Identity chain

```
LINE Login / Member session
        |
        v
exact LINE identity verification
        |
        v
canonical Customer ID
        |
        v
explicit Customer <-> Family link
        |
        v
Family ID
        |
        +--> Member Core
        +--> MEMORIES
        +--> CREATE
        +--> FAMILY PASS
        +--> SHOP
```

## Non-negotiable safety rules

1. A Family ID is never inferred from name, address, phone, email, LINE display name, child name, or fuzzy similarity.
2. Customer ID remains the existing canonical CRM identity.
3. Existing `customer_family_members` rows describe profile/family-member information and do not themselves establish household identity.
4. Two customers share a Member family only after an explicit link exists.
5. One canonical customer may have at most one active Member-family link at a time.
6. Historical family links use soft delete; identity history is not silently overwritten.
7. The initial foundation is read-only. No family creation/link endpoint is wired to Production.
8. Production D1 schema application and writes require separate Owner authorization.

## Additive schema

- `customer_family_groups`
  - stable `family_id`
  - display name
  - active/inactive state
- `customer_family_customer_links`
  - exact `customer_id`
  - `family_id`
  - relationship/access role
  - soft delete

No change is made to the `customers` table.

## Initial API contract

A future internal Member API may call:

`GET /api/internal/member-family/customer/:customer_id`

The current module implements this read model but is intentionally not wired into the Production entry point.

Expected states:

- `linked`
- `unlinked`
- `schema_not_applied`
- invalid Customer ID -> rejected

## Relationship to the customer-facing app

The customer-facing app should never request a Family ID by name or by a user-supplied Customer ID.

Expected flow:

1. Verify LINE/member session server-side.
2. Resolve the canonical CRM Customer ID.
3. Resolve the explicit Family ID through the internal contract.
4. Issue a Member session bound to that Family ID.
5. Every family-scoped resource verifies the session Family ID before returning data.

This provides the authorization boundary for personalized HOME, MEMORIES, CREATE, SHOP, MY and later BLACK experiences.

## Existing-customer historical shoot sync

A later Member phase must support existing customers who have already completed photography sessions before creating a Member account.

Expected account-creation flow:

1. Verify the Member identity and resolve the canonical Customer ID.
2. Resolve the explicit Family ID.
3. Read historical completed reservations already linked to that canonical Customer ID.
4. Create or reconcile one Member MEMORY per eligible completed shoot.
5. Use a stable source identifier such as `reservation_id` to make the import idempotent and prevent duplicates.
6. Attach existing delivery metadata / Amazon Photos delivery URL when canonical CRM evidence exists.
7. If preview photos or delivery links are missing, create the MEMORY history entry without inventing media; OWNER may enrich it later.
8. Re-running sync must not create duplicate MEMORY rows.

Identity safety:

- only reservations already attached to the canonical Customer ID are eligible for automatic sync;
- name-only / fuzzy / address-only historical rows remain review-only until CRM identity reconciliation establishes an exact customer link;
- account creation must never auto-claim ambiguous legacy sales rows.

Result: an existing customer can create an account and immediately see the past shooting history that is already safely linked in Customer CRM.

## Spreadsheet backup strategy

Cloudflare D1 remains the canonical operational database. Google Sheets is a disaster-recovery / human-readable backup target, not a second source of truth.

Required model:

```
Customer CRM / D1  (canonical)
        |
        +--> backup outbox / audit event
                  |
                  v
          scheduled backup worker
                  |
                  v
           Google Spreadsheet
```

Recommended spreadsheet structure:

- `Customers_Current`
  - latest recoverable customer snapshot;
- `Customers_Backup_Log`
  - append-only changes / snapshots;
- `Family_Current`
  - recoverable Family ID mappings when activated;
- `Backup_Status`
  - last successful backup time, row counts, failure state.

Safety requirements:

1. Backup is one-way D1 -> Google Sheets. Spreadsheet edits do not automatically overwrite CRM.
2. A CRM write should create a durable backup-outbox/audit record in D1.
3. A scheduled Worker retries pending backup rows so a temporary Google API failure does not lose the backup event.
4. Spreadsheet sync failures must never roll back or block the canonical CRM transaction.
5. Backup jobs must be idempotent.
6. Do not store passwords, session cookies, API secrets, LINE access tokens, or authentication tokens in the spreadsheet.
7. Do not copy full delivered photo binaries into Sheets.
8. Spreadsheet sharing must remain private and least-privilege.
9. Sensitive customer columns should be limited to what is actually needed for disaster recovery.
10. A restore operation is always explicit / Owner-reviewed; Sheets never performs automatic Production restoration.

This design protects against accidental D1 deletion while avoiding a dangerous bidirectional dual-master database.

## Backup recovery principle

A mirror alone is not sufficient because an accidental deletion could also propagate to the mirror.

Therefore the backup design should combine:

- a current-state snapshot for quick human inspection; and
- an append-only history / change log for point-in-time reconstruction.

Cloudflare-native recovery features may also be used, but the Google Sheet backup is intended as an independent operational copy.

## Current scope

Included:
- managed migration source
- read-only family resolver
- regression tests
- contract documentation
- zero-cost-first architecture requirement
- future historical shoot sync contract
- future Google Sheets backup contract

Not included:
- Production schema apply
- Production D1 write
- family auto-creation
- LINE Login implementation
- Member Core DB
- historical MEMORY import execution
- Google Sheets backup execution
- photo storage
- customer-facing UI
- deploy

## Production gates

Before activation:

1. current CRM identity-reconciliation work must remain unaffected;
2. migration review and local verification must pass;
3. Member login contract must be implemented;
4. cross-family access tests must prove family A cannot access family B;
5. historical sync must be idempotent and exact-Customer-ID-only;
6. backup recovery and retry behavior must be tested without exposing secrets;
7. fresh Owner approval is required for Production schema apply;
8. fresh Owner approval is required for any Production write path.
