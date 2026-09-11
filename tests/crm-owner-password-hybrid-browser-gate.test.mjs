import test from 'node:test';
import assert from 'node:assert/strict';
import { handleOwnerPasswordAuth, withOwnerPasswordPrincipal, handleOwnerPasswordBrowserGate } from '../src/crm-owner-password-auth.mjs';

const ALLOW_LIMITER={limit:async()=>({success:true})};
const ENV={
  CRM_OWNER_AUTH_MODE:'hybrid',
  CRM_OWNER_PASSWORD:'TEST_ONLY_PASSWORD',
  CRM_OWNER_SESSION_SECRET:'TEST_ONLY_SESSION_SECRET_VALUE',
  CRM_OWNER_LOGIN_RATE_LIMITER:ALLOW_LIMITER
};
const LOGIN='https://crm.example.test/__crm/owner-login';

function formRequest(value){
  return new Request(LOGIN,{
    method:'POST',
    headers:{'content-type':'application/x-www-form-urlencoded','origin':'https://crm.example.test'},
    body:new URLSearchParams({password:value})
  });
}

test('hybrid browser entry redirects unauthenticated owner to password login',async()=>{
  const req=await withOwnerPasswordPrincipal(new Request('https://crm.example.test/admin'),ENV);
  const res=handleOwnerPasswordBrowserGate(req,ENV);
  assert.equal(res.status,302);
  assert.equal(res.headers.get('location'),'/__crm/owner-login');
});

test('hybrid browser gate preserves authenticated Cloudflare Access principal',async()=>{
  const req=await withOwnerPasswordPrincipal(new Request('https://crm.example.test/admin',{headers:{'cf-access-authenticated-user-email':'owner@example.com'}}),ENV);
  assert.equal(handleOwnerPasswordBrowserGate(req,ENV),null);
});

test('hybrid browser gate preserves valid signed password session',async()=>{
  const login=await handleOwnerPasswordAuth(formRequest(ENV.CRM_OWNER_PASSWORD),ENV);
  const cookie=(login.headers.get('set-cookie')||'').split(';')[0];
  const req=await withOwnerPasswordPrincipal(new Request('https://crm.example.test/admin',{headers:{cookie}}),ENV);
  assert.equal(req.headers.get('x-crm-owner-auth'),'password-session');
  assert.equal(handleOwnerPasswordBrowserGate(req,ENV),null);
});

test('hybrid browser gate fails closed when password secrets are missing and no Access principal exists',async()=>{
  const env={CRM_OWNER_AUTH_MODE:'hybrid',CRM_OWNER_LOGIN_RATE_LIMITER:ALLOW_LIMITER};
  const req=await withOwnerPasswordPrincipal(new Request('https://crm.example.test/admin'),env);
  const res=handleOwnerPasswordBrowserGate(req,env);
  assert.equal(res.status,503);
  assert.match(await res.text(),/secret設定が不足/);
});

test('hybrid browser gate preserves authenticated Reservation internal handoff',async()=>{
  const env={...ENV,CRM_INTERNAL_TOKEN:'TEST_ONLY_INTERNAL_TOKEN'};
  const req=await withOwnerPasswordPrincipal(new Request('https://crm.example.test/admin',{headers:{'x-internal-token':'TEST_ONLY_INTERNAL_TOKEN','x-crm-internal-principal':'reservation-app'}}),env);
  assert.equal(handleOwnerPasswordBrowserGate(req,env),null);
});

test('access mode behavior remains unchanged',()=>{
  const env={CRM_OWNER_AUTH_MODE:'access'};
  const res=handleOwnerPasswordBrowserGate(new Request('https://crm.example.test/admin'),env);
  assert.equal(res,null);
});
