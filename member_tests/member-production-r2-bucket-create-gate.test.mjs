import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';

const WORKFLOW='.github/workflows/member-production-r2-bucket-create.yml';
const BRIDGE='.github/workflows/dispatch-member-production-r2-bucket-create-from-issue.yml';
const FOUNDATION='.github/workflows/member-app-foundation.yml';
const DOC='docs/member-app/member-production-r2-bucket-create.md';

const BUCKET='customer-crm-member-private-media';
const DIGEST='6f1f7fa25143081a302fcd148a52ae660195b2a31cf93a55f9ed39cf2e25f388';
const calculated=crypto.createHash('sha256').update(BUCKET).digest('hex');
assert.equal(calculated,DIGEST);

const workflow=fs.readFileSync(WORKFLOW,'utf8');
const bridge=fs.readFileSync(BRIDGE,'utf8');
const foundation=fs.readFileSync(FOUNDATION,'utf8');
const doc=fs.readFileSync(DOC,'utf8');

for(const marker of [
  'workflow_dispatch:',
  'expected_sha:',
  'candidate_bucket_sha256:',
  'jurisdiction:',
  'owner_ack:',
  "github.actor == 'ohw3rz5578d277e-collab'",
  `CANONICAL_BUCKET_NAME: ${BUCKET}`,
  `CANONICAL_BUCKET_SHA256: ${DIGEST}`,
  'BUCKET_CREATE_AUTHORIZED_COUNT=1',
  'BUCKET_UPDATE_AUTHORIZED=0',
  'BUCKET_DELETE_AUTHORIZED=0',
  'R2_OBJECT_READ=0',
  'R2_OBJECT_WRITE=0',
  'R2_OBJECT_LIST=0',
  'R2_OBJECT_DELETE=0',
  'PRODUCTION_STORAGE_BINDING_CHANGE=0',
  'PRODUCTION_STORAGE_FETCH=0',
  'PRODUCTION_DEPLOY=0',
  'PRODUCTION_TRAFFIC_CHANGE=0',
  'PRODUCTION_D1_WRITE=0'
])assert.ok(workflow.includes(marker),`workflow missing ${marker}`);

assert.ok(workflow.includes(`expected_ack="AUTHORIZE_MEMBER_R2_BUCKET_CREATE:\${expected_sha}:\${candidate_sha}:default"`));
assert.ok(workflow.includes("test \"$jurisdiction\" = 'default'"));
assert.ok(workflow.includes("-H 'cf-r2-jurisdiction: default'"));
assert.ok(workflow.includes(`name:\"${BUCKET}\"`));
assert.ok(workflow.includes('storageClass:"Standard"'));
assert.ok(workflow.includes('R2_CANONICAL_BUCKET_ABSENT=PASS'));
assert.ok(workflow.includes('R2_BUCKET_POSTCHECK_EXACT_ONE_MATCH=PASS'));
assert.ok(workflow.includes('ACTIVE_PRODUCTION_VERSION_DRIFT=NO'));
assert.ok(workflow.includes('MAIN_DRIFT=NO'));
assert.ok(workflow.includes('RAW_BUCKET_NAME_PRINTED=NO'));
assert.ok(workflow.includes('RAW_BUCKET_NAMES_PRINTED=NO'));

const cloudflareCreateEndpoint='https://api.cloudflare.com/client/v4/accounts/$CLOUDFLARE_ACCOUNT_ID/r2/buckets';
const endpointOccurrences=workflow.split(cloudflareCreateEndpoint).length-1;
assert.equal(endpointOccurrences,3,'workflow must use exactly two metadata-list calls and one create call');
const postOccurrences=(workflow.match(/--request POST/g)||[]).length;
assert.equal(postOccurrences,1,'workflow must issue exactly one POST');
assert.doesNotMatch(workflow,/--request\s+(PATCH|DELETE|PUT)/i);
assert.doesNotMatch(workflow,/wrangler\s+r2\s+(bucket\s+delete|object|bucket\s+update)/i);
assert.doesNotMatch(workflow,/\/objects(?:[/?"'])/i);
assert.doesNotMatch(workflow,/r2_buckets\s*[:=]\s*\[/);
assert.doesNotMatch(workflow,/wrangler\s+secret|gh\s+secret/i);

for(const marker of [
  'github.event.issue.number == 26',
  "github.actor == 'ohw3rz5578d277e-collab'",
  "github.event.comment.user.login == 'ohw3rz5578d277e-collab'",
  "startsWith(github.event.comment.body, '/member-production-r2-bucket-create ')",
  `canonical_bucket_sha256='${DIGEST}'`,
  'OWNER_ACK_MISMATCH',
  'MAIN_DRIFT',
  'member-production-r2-bucket-create.yml/dispatches',
  'BUCKET_CREATE_DISPATCH_COUNT=1',
  'BUCKET_UPDATE_COUNT=0',
  'BUCKET_DELETE_COUNT=0'
])assert.ok(bridge.includes(marker),`bridge missing ${marker}`);

assert.match(bridge,/command_re='\^\/member-production-r2-bucket-create sha=\(\[0-9a-f\]\{40\}\) bucket_sha256=\(\[0-9a-f\]\{64\}\) jurisdiction=\(default\) owner_ack=/);
assert.doesNotMatch(bridge,/bucket_name=/);
assert.doesNotMatch(bridge,/fedramp|jurisdiction=eu|jurisdiction=us/);

for(const path of [WORKFLOW,BRIDGE]){
  assert.ok(foundation.includes(`- '${path}'`),`foundation paths missing ${path}`);
  assert.ok(foundation.includes(path),`foundation scope guard missing ${path}`);
}

for(const marker of [
  `bucket name: \`${BUCKET}\``,
  `bucket-name SHA-256: \`${DIGEST}\``,
  'jurisdiction: `default`',
  'storage class: `Standard`',
  'Merging this source does not authorize a Production mutation.',
  'exactly one `POST /accounts/{account_id}/r2/buckets` request',
  'The workflow must not delete, recreate, patch, rename, or retry the bucket automatically.',
  'fresh read-only digest index under separate exact-SHA Owner authorization',
  '`MEMBER_PRIVATE_MEDIA_BUCKET` binding'
])assert.ok(doc.includes(marker),`doc missing ${marker}`);

console.log('MEMBER_R2_BUCKET_CREATE_GATE_CONTRACT=PASS');
console.log(`CANONICAL_BUCKET_SHA256=${DIGEST}`);
console.log('BUCKET_CREATE_MAX_COUNT=1');
console.log('BUCKET_UPDATE=0');
console.log('BUCKET_DELETE=0');
console.log('R2_OBJECT_ACCESS=0');
console.log('PRODUCTION_BINDING_CHANGE=0');
console.log('PRODUCTION_DEPLOY=0');
