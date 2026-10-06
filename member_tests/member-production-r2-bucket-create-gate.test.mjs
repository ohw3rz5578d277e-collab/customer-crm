import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-r2-bucket-create.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-production-r2-bucket-create-from-issue.yml','utf8');
const doc=fs.readFileSync('docs/member-app/member-production-r2-bucket-create.md','utf8');
const bucket='customer-crm-member-private-media';
const digest='6f1f7fa25143081a302fcd148a52ae660195b2a31cf93a55f9ed39cf2e25f388';

assert.equal(crypto.createHash('sha256').update(bucket).digest('hex'),digest);
assert.ok(workflow.includes(bucket));
assert.ok(workflow.includes(digest));
assert.ok(workflow.includes("cf-r2-jurisdiction: default"));
assert.ok(workflow.includes('BUCKET_CREATE_AUTHORIZED_COUNT=1'));
assert.ok(workflow.includes('BUCKET_UPDATE_AUTHORIZED=0'));
assert.ok(workflow.includes('BUCKET_DELETE_AUTHORIZED=0'));
assert.equal((workflow.match(/--request POST/g)||[]).length,1);
assert.doesNotMatch(workflow,/--request\s+(PATCH|DELETE|PUT)/i);
assert.doesNotMatch(workflow,/\/objects(?:[/?"'])/i);
assert.ok(bridge.includes("github.event.issue.number == 26"));
assert.ok(bridge.includes("/member-production-r2-bucket-create "));
assert.ok(bridge.includes(digest));
assert.ok(doc.includes('Merging this source does not authorize a Production mutation.'));
assert.ok(doc.includes('must not delete, recreate, patch, rename, or retry the bucket automatically'));

console.log('MEMBER_R2_BUCKET_CREATE_GATE_CONTRACT=PASS');
console.log(`CANONICAL_BUCKET_SHA256=${digest}`);
