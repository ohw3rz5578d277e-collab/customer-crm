import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';

const DOC='docs/member-app/member-production-r2-binding-proposal.md';
const WRANGLER='wrangler.jsonc';
const ENTRY='src/production-index-crm-customer360-entry.js';
const ADAPTER='src/member-production-storage-adapter.mjs';

const BUCKET='customer-crm-member-private-media';
const BINDING='MEMBER_PRIVATE_MEDIA_BUCKET';
const BUCKET_SHA256='6f1f7fa25143081a302fcd148a52ae660195b2a31cf93a55f9ed39cf2e25f388';
const VERIFIED_MAIN='660620477f1d2ef9f9d1a49183c58957cf13e7eb';
const READ_ONLY_RUN='37560368471';
const STORAGE_IMPORT="import { createMemberPrivateMediaStorageAdapter } from './member-production-storage-adapter.mjs';";
const PRIVATE_WIRING='private_media_storage_adapter:createMemberPrivateMediaStorageAdapter(env?.MEMBER_PRIVATE_MEDIA_BUCKET)';

const doc=fs.readFileSync(DOC,'utf8');
const cfg=JSON.parse(fs.readFileSync(WRANGLER,'utf8'));
const entry=fs.readFileSync(ENTRY,'utf8');
const adapter=fs.readFileSync(ADAPTER,'utf8');

assert.equal(crypto.createHash('sha256').update(BUCKET).digest('hex'),BUCKET_SHA256);
assert.ok(doc.includes(`bucket name: \`${BUCKET}\``));
assert.ok(doc.includes(`bucket-name SHA-256: \`${BUCKET_SHA256}\``));
assert.ok(doc.includes('observed jurisdiction: `default`'));
assert.ok(doc.includes('exact-one-match verification: PASS'));
assert.ok(doc.includes(`read-only verification run: \`${READ_ONLY_RUN}\``));
assert.ok(doc.includes(`inspected main SHA: \`${VERIFIED_MAIN}\``));
assert.ok(doc.includes(`"binding": "${BINDING}"`));
assert.ok(doc.includes(`"bucket_name": "${BUCKET}"`));
assert.ok(doc.includes('Public Member assets remain unbound.'));
assert.ok(doc.includes('runtime wiring is source-only and does not authorize storage fetch'));
assert.ok(doc.includes('does **not** authorize'));

assert.deepEqual(
  cfg.r2_buckets,
  [{binding:BINDING,bucket_name:BUCKET}],
  'default Wrangler scope must declare exactly one canonical private-media R2 binding'
);

const scopes=[
  ['default',cfg],
  ...Object.entries(cfg?.env||{}).map(([name,value])=>[`env.${name}`,value])
];
for(const [scopeName,scope] of scopes){
  assert.notEqual(
    String(scope?.vars?.MEMBER_PRODUCTION_ROUTE_MODE||'').trim().toLowerCase(),
    'enabled',
    `${scopeName} Member Production route must remain disabled`
  );
  assert.notEqual(
    String(scope?.vars?.MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE||'').trim().toLowerCase(),
    'enabled',
    `${scopeName} private-media route must remain disabled`
  );
}
for(const [scopeName,scope] of Object.entries(cfg?.env||{})){
  assert.ok(
    !Array.isArray(scope?.r2_buckets)||scope.r2_buckets.length===0,
    `env.${scopeName} must not add another R2 binding`
  );
}

assert.ok(entry.includes('const MEMBER_PRODUCTION_OWNER_APPROVED=false;'));
assert.ok(entry.includes(STORAGE_IMPORT));
assert.ok(entry.includes('public_asset_adapter:null'));
assert.ok(entry.includes(PRIVATE_WIRING));
assert.ok(!entry.includes(BUCKET),'Production runtime source must consume the declared binding identifier, not hardcode the bucket name');
assert.equal((entry.match(/MEMBER_PRIVATE_MEDIA_BUCKET/g)||[]).length,1,'Production entry must reference the canonical private-media binding exactly once');
assert.doesNotMatch(entry,/private_media_storage_adapter:null/);

assert.ok(adapter.includes('createMemberPrivateMediaStorageAdapter(binding)'));
assert.ok(adapter.includes('implicit_env_binding:false'));
assert.ok(adapter.includes('get_only:true'));
assert.ok(adapter.includes('storage_write:false'));
assert.ok(adapter.includes('storage_delete:false'));
assert.doesNotMatch(adapter,/\.put\s*\(/);
assert.doesNotMatch(adapter,/\.delete\s*\(/);

console.log('MEMBER_R2_BINDING_SOURCE_CONTRACT=PASS');
console.log(`CANONICAL_BUCKET_SHA256=${BUCKET_SHA256}`);
console.log(`CANONICAL_BINDING=${BINDING}`);
console.log(`READ_ONLY_VERIFICATION_RUN=${READ_ONLY_RUN}`);
console.log('CANONICAL_R2_BINDING_CONFIGURED=SOURCE_ONLY');
console.log('PRODUCTION_RUNTIME_BINDING_CONSUMPTION=SOURCE_ONLY_WIRED');
console.log('PRODUCTION_STORAGE_FETCH=0');
console.log('R2_OBJECT_READ=0');
console.log('R2_WRITE=0');
console.log('MEMBER_ROUTE_ACTIVATION=0');
console.log('PRIVATE_MEDIA_ROUTE_ACTIVATION=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CRM_WRITE=0');
console.log('LINE_SEND=0');
console.log('CUSTOMER_ID_GENERATION=0');