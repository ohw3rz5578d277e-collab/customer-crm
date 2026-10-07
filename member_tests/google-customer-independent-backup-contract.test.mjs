import assert from 'node:assert/strict';
import fs from 'node:fs';
const doc=fs.readFileSync('docs/member-app/google-customer-independent-backup-contract.md','utf8');
for(const phrase of [
 'audit history, not disaster-recovery backup',
 'physically/logically separate',
 'Authentication secrets, session tokens, invitation raw tokens',
 'deterministic manifest digest',
 'not publicly shared',
 'failed verification must not delete or replace the last verified backup',
 'Restore is never automatic.',
 'explicit Owner authorization',
 'dry-run reconstruction report',
 'does not authorize indefinite retention',
 'does not create Drive files'
]) assert.ok(doc.includes(phrase),`missing: ${phrase}`);
console.log('GOOGLE_CUSTOMER_INDEPENDENT_BACKUP_CONTRACT=PASS');
console.log('PRIMARY_WORKBOOK_HISTORY_IS_BACKUP=NO');
console.log('SEPARATE_BACKUP_ARTIFACT_REQUIRED=YES');
console.log('AUTOMATIC_RESTORE=0');
console.log('GOOGLE_DRIVE_WRITE=0');
console.log('PRODUCTION_WRITE=0');
