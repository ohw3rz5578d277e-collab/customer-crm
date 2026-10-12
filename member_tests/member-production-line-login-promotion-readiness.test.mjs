import assert from 'node:assert/strict';
import {
  buildMemberLineLoginPromotionReadiness,
  memberLineLoginPromotionReadinessHealth
} from '../src/member-production-line-login-promotion-readiness.mjs';

const base={
  expected_sha:'fdfa65fa797c3ee89885ee99244e7a9ad691b124',
  staged_version_id:'be711f54-8716-4178-9949-d5ced183d9dc',
  candidate_sha256:'af11b47ca14b90985fd2a1f6aa5523626b77dbb4f72a7092c1e7fb5da1fb5cd8',
  staging_run_id:'38233408939',
  expected_active_version_id:'6d34db85-9a52-45e9-8f7d-094f9fc7957a',
  stage_receipt:{
    success:true,
    exact_sha:'fdfa65fa797c3ee89885ee99244e7a9ad691b124',
    staged_version_id:'be711f54-8716-4178-9949-d5ced183d9dc',
    candidate_sha256:'af11b47ca14b90985fd2a1f6aa5523626b77dbb4f72a7092c1e7fb5da1fb5cd8',
    unchanged_active_version_id:'6d34db85-9a52-45e9-8f7d-094f9fc7957a',
    staging_run_id:'38233408939',
    owner_approved:true,
    member_route_enabled:true,
    line_login_approved:true,
    line_login_external_exchange_enabled:true,
    private_media_content_route:false,
    favorites_write:false,
    memory_write:false,
    family_pass_entitlement_write:false,
    production_traffic_change:0,
    production_deploy:0,
    production_d1_read:0,
    production_d1_write:0,
    r2_object_read:0,
    r2_object_write:0,
    secret_change:0,
    line_send:0,
    customer_prospect_mutation:0,
    customer_id_generation:0,
    commerce_activation:0,
    black_automatic_award:0
  },
  active_deployment:{
    versions:[{version_id:'6d34db85-9a52-45e9-8f7d-094f9fc7957a',percentage:100}]
  },
  staged_version:{
    version_id:'be711f54-8716-4178-9949-d5ced183d9dc',
    candidate_sha256:'af11b47ca14b90985fd2a1f6aa5523626b77dbb4f72a7092c1e7fb5da1fb5cd8',
    owner_approved:true,
    member_route_enabled:true,
    line_login_approved:true,
    line_login_external_exchange_enabled:true,
    private_media_content_route:false,
    favorites_write:false,
    memory_write:false,
    family_pass_entitlement_write:false,
    resource_bindings_unchanged:true
  }
};

const clone=value=>JSON.parse(JSON.stringify(value));
const tests=[];
const test=(name,fn)=>tests.push([name,fn]);

test('valid exact candidate is promotion-ready',()=>{
  const result=buildMemberLineLoginPromotionReadiness(clone(base));
  assert.equal(result.readiness,'PASS');
  assert.equal(result.staged_version_id,base.staged_version_id);
  assert.equal(result.expected_active_version_id,base.expected_active_version_id);
  assert.equal(result.candidate_binding,'IMMUTABLE_SOURCE_CONSTANTS');
  assert.equal(result.owner_approval_required,true);
  assert.equal(result.production_traffic_change,false);
  assert.equal(result.next_boundary,'FRESH_OWNER_GATED_LINE_LOGIN_PROMOTION_REQUIRED');
});

test('unauthorized expected SHA fails closed',()=>{
  const x=clone(base); x.expected_sha='1'.repeat(40);
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/UNAUTHORIZED_EXPECTED_SHA/);
});

test('unauthorized staged version ID fails closed',()=>{
  const x=clone(base); x.staged_version_id='11111111-1111-1111-1111-111111111111';
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/UNAUTHORIZED_STAGED_VERSION_ID/);
});

test('unauthorized candidate digest fails closed',()=>{
  const x=clone(base); x.candidate_sha256='2'.repeat(64);
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/UNAUTHORIZED_CANDIDATE_SHA256/);
});

test('unauthorized staging run fails closed',()=>{
  const x=clone(base); x.staging_run_id='99999999999';
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/UNAUTHORIZED_STAGING_RUN_ID/);
});

test('unauthorized active version ID fails closed',()=>{
  const x=clone(base); x.expected_active_version_id='22222222-2222-2222-2222-222222222222';
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/UNAUTHORIZED_ACTIVE_VERSION_ID/);
});

test('internally consistent forged candidate identifiers fail closed',()=>{
  const x=clone(base);
  x.expected_sha='1'.repeat(40);
  x.staged_version_id='11111111-1111-1111-1111-111111111111';
  x.candidate_sha256='2'.repeat(64);
  x.staging_run_id='99999999999';
  x.expected_active_version_id='22222222-2222-2222-2222-222222222222';
  x.stage_receipt.exact_sha=x.expected_sha;
  x.stage_receipt.staged_version_id=x.staged_version_id;
  x.stage_receipt.candidate_sha256=x.candidate_sha256;
  x.stage_receipt.unchanged_active_version_id=x.expected_active_version_id;
  x.stage_receipt.staging_run_id=x.staging_run_id;
  x.active_deployment.versions=[{version_id:x.expected_active_version_id,percentage:100}];
  x.staged_version.version_id=x.staged_version_id;
  x.staged_version.candidate_sha256=x.candidate_sha256;
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/UNAUTHORIZED_EXPECTED_SHA/);
});

test('stage receipt run lineage mismatch fails closed',()=>{
  const x=clone(base); x.stage_receipt.staging_run_id='99999999999';
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/LINE_LOGIN_STAGE_RECEIPT_INVALID:staging_run/);
});

test('stage receipt Owner approval disabled fails closed',()=>{
  const x=clone(base); x.stage_receipt.owner_approved=false;
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/LINE_LOGIN_STAGE_RECEIPT_INVALID:owner_approval/);
});

test('staged Owner approval disabled fails closed',()=>{
  const x=clone(base); x.staged_version.owner_approved=false;
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/STAGED_OWNER_APPROVAL_NOT_ENABLED/);
});

test('stage receipt candidate digest mismatch fails closed',()=>{
  const x=clone(base); x.stage_receipt.candidate_sha256='0'.repeat(64);
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/LINE_LOGIN_STAGE_RECEIPT_INVALID:candidate_digest/);
});

test('stage receipt active version mismatch fails closed',()=>{
  const x=clone(base); x.stage_receipt.unchanged_active_version_id='11111111-1111-1111-1111-111111111111';
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/LINE_LOGIN_STAGE_RECEIPT_INVALID:unchanged_active/);
});

test('active version drift fails closed',()=>{
  const x=clone(base); x.active_deployment.versions[0].version_id='11111111-1111-1111-1111-111111111111';
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/ACTIVE_VERSION_NOT_OWNER_EXPECTED/);
});

test('multi-version Production baseline fails closed',()=>{
  const x=clone(base); x.active_deployment.versions.push({version_id:base.staged_version_id,percentage:0});
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/PRODUCTION_BASELINE_NOT_SINGLE_VERSION/);
});

test('candidate receiving Production traffic fails closed',()=>{
  const x=clone(base); x.active_deployment.versions=[
    {version_id:base.expected_active_version_id,percentage:100},
    {version_id:base.staged_version_id,percentage:1}
  ];
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/PRODUCTION_BASELINE_NOT_SINGLE_VERSION|STAGED_VERSION_ALREADY_RECEIVES_TRAFFIC/);
});

test('private media route activation fails closed',()=>{
  const x=clone(base); x.staged_version.private_media_content_route=true;
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/STAGED_PROHIBITED_MODE_NOT_DISABLED:private_media_content_route/);
});

test('favorites write activation fails closed',()=>{
  const x=clone(base); x.stage_receipt.favorites_write=true;
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/LINE_LOGIN_STAGE_RECEIPT_INVALID:favorites_write/);
});

test('resource binding drift fails closed',()=>{
  const x=clone(base); x.staged_version.resource_bindings_unchanged=false;
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/STAGED_RESOURCE_BINDINGS_CHANGED/);
});

test('LINE exchange not enabled fails closed',()=>{
  const x=clone(base); x.staged_version.line_login_external_exchange_enabled=false;
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/STAGED_LINE_LOGIN_EXCHANGE_NOT_ENABLED/);
});

test('strict zero rejects boolean false',()=>{
  const x=clone(base); x.stage_receipt.production_traffic_change=false;
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/LINE_LOGIN_STAGE_RECEIPT_INVALID:traffic/);
});

test('strict zero rejects empty string',()=>{
  const x=clone(base); x.stage_receipt.production_traffic_change='';
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/LINE_LOGIN_STAGE_RECEIPT_INVALID:traffic/);
});

test('strict zero rejects null',()=>{
  const x=clone(base); x.stage_receipt.production_traffic_change=null;
  assert.throws(()=>buildMemberLineLoginPromotionReadiness(x),/LINE_LOGIN_STAGE_RECEIPT_INVALID:traffic/);
});

test('strict zero accepts numeric zero',()=>{
  const x=clone(base); x.stage_receipt.production_traffic_change=0;
  assert.equal(buildMemberLineLoginPromotionReadiness(x).readiness,'PASS');
});

test('strict zero accepts string zero',()=>{
  const x=clone(base); x.stage_receipt.production_traffic_change='0';
  assert.equal(buildMemberLineLoginPromotionReadiness(x).readiness,'PASS');
});

test('health remains source-only and mutation-free',()=>{
  const h=memberLineLoginPromotionReadinessHealth();
  assert.equal(h.source_only,true);
  assert.equal(h.candidate_binding,'IMMUTABLE_SOURCE_CONSTANTS');
  assert.equal(h.owner_approval_required,true);
  assert.equal(h.production_mutation,false);
  assert.equal(h.production_traffic_change,false);
  assert.equal(h.production_deploy,false);
  assert.equal(h.production_d1_read,false);
  assert.equal(h.production_d1_write,false);
  assert.equal(h.r2_object_read,false);
  assert.equal(h.r2_object_write,false);
  assert.equal(h.secret_change,false);
  assert.equal(h.line_send,false);
  assert.equal(h.customer_prospect_mutation,false);
});

let passed=0;
for(const [name,fn] of tests){
  try{fn();passed++;console.log('PASS',name);}catch(error){console.error('FAIL',name);throw error;}
}
console.log(`MEMBER_LINE_LOGIN_PROMOTION_READINESS=${passed}/${tests.length} PASS`);
console.log('PRODUCTION_MUTATION=0');
console.log('PRODUCTION_TRAFFIC_CHANGE=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('R2_OBJECT_READ=0');
console.log('R2_OBJECT_WRITE=0');
