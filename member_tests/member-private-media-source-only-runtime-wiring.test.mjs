import fs from 'node:fs';
import assert from 'node:assert/strict';

const source=fs.readFileSync(new URL('../src/production-index-crm-customer360-entry.js',import.meta.url),'utf8');
const wrangler=fs.readFileSync(new URL('../wrangler.jsonc',import.meta.url),'utf8');

function pass(name,condition){
  assert.equal(condition,true,name);
  console.log('PASS',name);
}

pass('Owner approval remains false',/const MEMBER_PRODUCTION_OWNER_APPROVED=false;/.test(source));
pass('private media adapter factory is imported',source.includes("createMemberPrivateMediaStorageAdapter } from './member-production-storage-adapter.mjs'"));
pass('canonical private binding is passed explicitly to adapter factory',source.includes('createMemberPrivateMediaStorageAdapter(env?.MEMBER_PRIVATE_MEDIA_BUCKET)'));
pass('private adapter is passed explicitly to Member Production composition',source.includes('private_media_storage_adapter:privateMediaStorageAdapter'));
pass('public asset runtime wiring remains unchanged and null',source.includes('public_asset_adapter:null'));
pass('LINE Login Production approval remains false',source.includes('line_login_approved:false'));

const config=JSON.parse(wrangler);
const r2=Array.isArray(config.r2_buckets)?config.r2_buckets:[];
pass('exactly one R2 binding remains declared',r2.length===1);
pass('canonical private-media binding remains exact',
  r2[0]?.binding==='MEMBER_PRIVATE_MEDIA_BUCKET'
  && r2[0]?.bucket_name==='customer-crm-member-private-media'
);
pass('Member Production route is not enabled in vars',config.vars?.MEMBER_PRODUCTION_ROUTE_MODE!=='enabled');
pass('private-media content route is not enabled in vars',config.vars?.MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE!=='enabled');

console.log('MEMBER_PRIVATE_MEDIA_SOURCE_ONLY_RUNTIME_WIRING=PASS');
console.log('R2_OBJECT_FETCH=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('ROUTE_ACTIVATION=0');
console.log('LINE_LOGIN_PRODUCTION_ACTIVATION=0');
