import test from 'node:test';
import assert from 'node:assert/strict';
import { handleOwnerPasswordAuth, withOwnerPasswordPrincipal, ownerPasswordAuthHealth } from '../src/crm-owner-password-auth.mjs';

const ENV={CRM_OWNER_PASSWORD:'correct-horse-battery-staple',CRM_OWNER_SESSION_SECRET:'test-session-secret-32-bytes-minimum-value'};
const LOGIN='https://crm.example.test/__crm/owner-login';

function formRequest(password){
  return new Request(LOGIN,{
    method:'POST',
    headers:{'content-type':'application/x-www-form-urlencoded','origin':'https://crm.example.test'},
    body:new URLSearchParams({password})
  });
}

test('password auth fails closed when secrets are not configured',async()=>{
  const res=await handleOwnerPasswordAuth(new Request(LOGIN),{});
  assert.equal(res.status,503);
  assert.match(await res.text(),/まだ有効化されていません/);
});

test('wrong password is rejected without issuing session cookie',async()=>{
  const res=await handleOwnerPasswordAuth(formRequest('wrong-password'),ENV);
  assert.equal(res.status,401);
  assert.equal(res.headers.get('set-cookie'),null);
});

test('cross-origin login is rejected',async()=>{
  const req=new Request(LOGIN,{
    method:'POST',
    headers:{'content-type':'application/x-www-form-urlencoded','origin':'https://evil.example'},
    body:new URLSearchParams({password:ENV.CRM_OWNER_PASSWORD})
  });
  const res=await handleOwnerPasswordAuth(req,ENV);
  assert.equal(res.status,403);
});

test('correct password creates a strict secure HttpOnly owner session',async()=>{
  const res=await handleOwnerPasswordAuth(formRequest(ENV.CRM_OWNER_PASSWORD),ENV);
  assert.equal(res.status,303);
  assert.equal(res.headers.get('location'),'/admin');
  const cookie=res.headers.get('set-cookie')||'';
  assert.match(cookie,/crm_owner_session=/);
  assert.match(cookie,/HttpOnly/i);
  assert.match(cookie,/Secure/i);
  assert.match(cookie,/SameSite=Strict/i);
  assert.match(cookie,/Max-Age=43200/i);
});

test('valid owner session maps only to canonical owner principal',async()=>{
  const login=await handleOwnerPasswordAuth(formRequest(ENV.CRM_OWNER_PASSWORD),ENV);
  const setCookie=login.headers.get('set-cookie')||'';
  const cookie=setCookie.split(';')[0];
  const req=new Request('https://crm.example.test/admin',{headers:{cookie}});
  const effective=await withOwnerPasswordPrincipal(req,ENV);
  assert.equal(effective.headers.get('cf-access-authenticated-user-email'),'ohw3rz5578d277e@gmail.com');
  assert.equal(effective.headers.get('x-crm-owner-auth'),'password-session');
});

test('tampered owner session is never elevated',async()=>{
  const login=await handleOwnerPasswordAuth(formRequest(ENV.CRM_OWNER_PASSWORD),ENV);
  const raw=(login.headers.get('set-cookie')||'').split(';')[0];
  const cookie=raw.slice(0,-1)+(raw.endsWith('A')?'B':'A');
  const req=new Request('https://crm.example.test/admin',{headers:{cookie}});
  const effective=await withOwnerPasswordPrincipal(req,ENV);
  assert.equal(effective.headers.get('cf-access-authenticated-user-email'),null);
});

test('existing Cloudflare Access principal is preserved',async()=>{
  const req=new Request('https://crm.example.test/admin',{headers:{'cf-access-authenticated-user-email':'existing@example.com'}});
  const effective=await withOwnerPasswordPrincipal(req,ENV);
  assert.equal(effective.headers.get('cf-access-authenticated-user-email'),'existing@example.com');
  assert.equal(effective.headers.get('x-crm-owner-auth'),null);
});

test('logout expires only the owner session cookie',async()=>{
  const req=new Request('https://crm.example.test/__crm/owner-logout',{method:'POST',headers:{origin:'https://crm.example.test'}});
  const res=await handleOwnerPasswordAuth(req,ENV);
  assert.equal(res.status,303);
  assert.equal(res.headers.get('location'),'/__crm/owner-login');
  assert.match(res.headers.get('set-cookie')||'',/Max-Age=0/);
});

test('health contract confirms auth adds no D1/customer-id/LINE write path',()=>{
  const h=ownerPasswordAuthHealth();
  assert.equal(h.owner_password_auth_supported,true);
  assert.equal(h.owner_password_auth_fail_closed,true);
  assert.equal(h.owner_password_auth_customer_id_generation,false);
  assert.equal(h.owner_password_auth_d1_write,false);
  assert.equal(h.owner_password_auth_line_send,false);
});
