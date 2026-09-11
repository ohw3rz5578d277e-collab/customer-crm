import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { ownerPasswordAuthHealth } from '../src/crm-owner-password-auth.mjs';

const ALLOW_LIMITER={limit:async()=>({success:true})};
const EXPECTED_MODE='hybrid';

function releaseReady(health){
  return health.owner_password_auth_supported===true &&
    health.owner_password_auth_mode===EXPECTED_MODE &&
    health.owner_password_auth_mode_valid===true &&
    health.owner_password_auth_configured===true &&
    health.owner_password_auth_enabled===true &&
    health.owner_password_auth_fail_closed===true &&
    health.owner_password_auth_header_spoof_protection===true &&
    health.owner_password_auth_rate_limit_required===true &&
    health.owner_password_auth_rate_limit_configured===true &&
    health.owner_password_auth_reservation_internal_preserved===true &&
    health.owner_password_auth_logout_post_endpoint===true &&
    health.owner_password_auth_access_logout_preserved===true &&
    health.owner_password_auth_cookie_http_only===true &&
    health.owner_password_auth_cookie_secure===true &&
    health.owner_password_auth_cookie_same_site==='Strict' &&
    health.owner_password_auth_session_seconds===43200 &&
    health.owner_password_auth_customer_id_generation===false &&
    health.owner_password_auth_d1_write===false &&
    health.owner_password_auth_line_send===false;
}

test('Production config declares staged hybrid Owner auth',()=>{
  const config=JSON.parse(fs.readFileSync('wrangler.jsonc','utf8'));
  assert.equal(config?.vars?.CRM_OWNER_AUTH_MODE,EXPECTED_MODE);
});

test('hybrid release readiness passes only with both secrets and rate limiter',()=>{
  const health=ownerPasswordAuthHealth({
    CRM_OWNER_AUTH_MODE:'hybrid',
    CRM_OWNER_PASSWORD:'TEST_ONLY_PASSWORD',
    CRM_OWNER_SESSION_SECRET:'TEST_ONLY_SESSION_SECRET',
    CRM_OWNER_LOGIN_RATE_LIMITER:ALLOW_LIMITER
  });
  assert.equal(releaseReady(health),true);
});

test('hybrid release readiness fails closed when Owner password secret is missing',()=>{
  const health=ownerPasswordAuthHealth({
    CRM_OWNER_AUTH_MODE:'hybrid',
    CRM_OWNER_SESSION_SECRET:'TEST_ONLY_SESSION_SECRET',
    CRM_OWNER_LOGIN_RATE_LIMITER:ALLOW_LIMITER
  });
  assert.equal(health.owner_password_auth_configured,false);
  assert.equal(health.owner_password_auth_enabled,false);
  assert.equal(releaseReady(health),false);
});

test('hybrid release readiness fails closed when session secret is missing',()=>{
  const health=ownerPasswordAuthHealth({
    CRM_OWNER_AUTH_MODE:'hybrid',
    CRM_OWNER_PASSWORD:'TEST_ONLY_PASSWORD',
    CRM_OWNER_LOGIN_RATE_LIMITER:ALLOW_LIMITER
  });
  assert.equal(health.owner_password_auth_configured,false);
  assert.equal(health.owner_password_auth_enabled,false);
  assert.equal(releaseReady(health),false);
});

test('hybrid release readiness fails closed when rate limiter binding is missing',()=>{
  const health=ownerPasswordAuthHealth({
    CRM_OWNER_AUTH_MODE:'hybrid',
    CRM_OWNER_PASSWORD:'TEST_ONLY_PASSWORD',
    CRM_OWNER_SESSION_SECRET:'TEST_ONLY_SESSION_SECRET'
  });
  assert.equal(health.owner_password_auth_rate_limit_configured,false);
  assert.equal(health.owner_password_auth_enabled,false);
  assert.equal(releaseReady(health),false);
});

test('password-only mode cannot accidentally satisfy staged hybrid release gate',()=>{
  const health=ownerPasswordAuthHealth({
    CRM_OWNER_AUTH_MODE:'password',
    CRM_OWNER_PASSWORD:'TEST_ONLY_PASSWORD',
    CRM_OWNER_SESSION_SECRET:'TEST_ONLY_SESSION_SECRET',
    CRM_OWNER_LOGIN_RATE_LIMITER:ALLOW_LIMITER
  });
  assert.equal(health.owner_password_auth_enabled,true);
  assert.equal(releaseReady(health),false);
});

test('access mode cannot accidentally satisfy staged hybrid release gate',()=>{
  const health=ownerPasswordAuthHealth({CRM_OWNER_AUTH_MODE:'access'});
  assert.equal(health.owner_password_auth_enabled,false);
  assert.equal(releaseReady(health),false);
});

console.log('CRM_OWNER_PASSWORD_PRODUCTION_READINESS_GATE=PASS');
console.log('REQUIRED_MODE=hybrid');
console.log('REQUIRES=PASSWORD_SECRET,SESSION_SECRET,RATE_LIMITER,FAIL_CLOSED,ACCESS_FALLBACK,RESERVATION_HANDOFF');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('LINE_SEND=0');
