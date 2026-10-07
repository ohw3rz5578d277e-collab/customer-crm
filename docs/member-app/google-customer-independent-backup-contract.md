# Google customer data independent backup contract

## Roadmap phase

Independent backup follows Customer/Prospect Master sync and Review Queue safety.

## Principle

Customer History inside the same primary workbook is audit history, not disaster-recovery backup.

A backup must be a physically/logically separate, access-restricted Drive artifact or equivalent approved Google storage object whose loss domain is not the primary workbook alone.

## Backup scope

A versioned snapshot may include the minimum approved durable records needed to reconstruct:

- Customer Master
- Customer History
- Prospect Master
- Prospect History
- consent evidence
- promotion/link audit metadata
- synchronization version/event metadata

Authentication secrets, session tokens, invitation raw tokens, HMAC secrets, service-account credentials, and unrelated runtime secrets must never be exported.

## Snapshot identity

Each snapshot requires:

- backup_id
- created_at
- source logical version/watermark
- record counts by logical dataset
- deterministic manifest digest
- previous backup reference when available
- completion status

A snapshot is not considered valid until the artifact and manifest are both durably created and verification succeeds.

## Verification

Verification is read-only and must check:

- expected logical datasets are present
- manifest digest matches
- record counts match the manifest
- snapshot is readable by the backup service identity
- snapshot is not publicly shared
- no forbidden secret classes are present

A failed verification must not delete or replace the last verified backup.

## Retention

Backup retention must be separately documented before activation. PII retention in backup must be consistent with the published Privacy Policy and legal/business requirements; this contract does not authorize indefinite retention.

Deletion of expired backups is a separate write operation and must be auditable and bounded.

## Restore

Restore is never automatic.

A restore requires:

- explicit Owner authorization
- exact backup_id
- read-only inspection first
- identity/version conflict detection
- dry-run reconstruction report
- separately authorized write phase

Restore must not silently overwrite newer Customer/Prospect Master state.

## Scheduling

A future scheduled backup may run after successful daily reconciliation. Scheduling itself does not authorize Drive writes until the backup executor and credentials are separately approved.

## Authorization boundary

This source-only contract does not create Drive files, read/write Google data, change sharing, create credentials, change secrets, deploy GAS/Workers, write Production D1, mutate CRM customers, generate Customer IDs, send LINE, change routes/UI/R2, write BLACK/MEMORY state, activate commerce, or spend money.
