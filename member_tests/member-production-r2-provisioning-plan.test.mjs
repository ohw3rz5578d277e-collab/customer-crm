import assert from 'node:assert/strict';
import fs from 'node:fs';

const DOC='docs/member-app/member-production-r2-provisioning-plan.md';
const WRANGLER='wrangler.jsonc';
const ENTRY='src/production-index-crm-customer360-entry.js';
const ADAPTER='src/member-production-storage-adapter.mjs';

const BUCKET='customer-crm-member-private-media';
const BINDING='MEMBER_PRIVATE_MEDIA_BUCKET';

const doc=fs.readFileSync(DOC,'utf8');
const wranglerText=fs.readFileSync(WRANGLER,'utf8');
const cfg=JSON.parse(wranglerText);
const entry=fs.readFileSync(ENTRY,'utf8');
const adapter=fs.readFileSync(ADAPTER,'utf8');

assert.match(BUCKET,/^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/);
assert.equal(BUCKET,'customer-crm-member-private-media');
assert.equal(BINDING,'MEMBER_PRIVATE_MEDIA_BUCKET');

assert.ok(doc.includes(`Proposed bucket name: \`${BUCKET}\``));
assert.ok(doc.includes(`Proposed Worker binding: \`${BINDING}\``));
assert.ok(doc.includes('Authorized jurisdiction target after separate Owner approval: `default`'));
assert.ok(doc.includes('Public access: disabled'));
assert.ok(doc.includes('The first bucket is private-media-only.'));
assert.ok(doc.includes('No step inherits authorization from a previous step.'));
assert.ok(doc.includes('candidate verify run must prove exact-one-match'));
assert.ok(doc.includes('Adding the binding, Production deploy, private-media route activation, Member route activation, and Production storage fetch each remain separately Owner-gated.'));

for(const unrelated of [
  'ai-manga-publisher-assets',
  'album-originals',
  'album-previews',
  'instagram-thumbs'
]){
  assert.ok(doc.includes(`\`${unrelated}\``));
}

assert.ok(!Array.isArray(cfg.r2_buckets)||cfg.r2_buckets.length===0,'canonical wrangler.jsonc must remain without R2 bindings');
assert.notEqual(String(cfg?.vars?.MEMBER_PRODUCTION_ROUTE_MODE||'').trim().toLowerCase(),'enabled');
assert.notEqual(String(cfg?.vars?.MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE||'').trim().toLowerCase(),'enabled');
assert.ok(!wranglerText.includes(BUCKET),'proposed bucket must not be configured before separate authorization');
assert.ok(!wranglerText.includes(BINDING),'proposed binding must not be configured before separate authorization');

assert.ok(entry.includes('const MEMBER_PRODUCTION_OWNER_APPROVED=false;'));
assert.ok(entry.includes('private_media_storage_adapter:null'));
assert.ok(entry.includes('public_asset_adapter:null'));
assert.ok(!entry.includes(BUCKET));
assert.ok(!entry.includes(BINDING));

assert.ok(adapter.includes('createMemberPrivateMediaStorageAdapter(binding)'));
assert.ok(adapter.includes('createMemberPublicAssetStorageAdapter(binding)'));
assert.ok(adapter.includes('async get(key)'));
assert.ok(adapter.includes('binding.get(String(key))'));
assert.ok(adapter.includes('Object.freeze'));
assert.ok(adapter.includes('implicit_env_binding:false'));
assert.ok(adapter.includes('storage_write:false'));
assert.ok(adapter.includes('storage_delete:false'));
assert.doesNotMatch(adapter,/\.put\s*\(/);
assert.doesNotMatch(adapter,/\.delete\s*\(/);

console.log('MEMBER_R2_PROVISIONING_PLAN_CONTRACT=PASS');
console.log(`PROPOSED_BUCKET_SHA_SOURCE=${BUCKET}`);
console.log(`PROPOSED_BINDING=${BINDING}`);
console.log('CANONICAL_R2_BINDING_CONFIGURED=NO');
console.log('BUCKET_CREATE=0');
console.log('R2_OBJECT_READ=0');
console.log('R2_WRITE=0');
console.log('PRODUCTION_STORAGE_BINDING_CHANGE=0');
console.log('PRODUCTION_STORAGE_FETCH=0');
console.log('MEMBER_ROUTE_ACTIVATION=0');
console.log('PRIVATE_MEDIA_ROUTE_ACTIVATION=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('PRODUCTION_D1_WRITE=0');
