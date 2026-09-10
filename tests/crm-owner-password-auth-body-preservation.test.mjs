import test from 'node:test';
import assert from 'node:assert/strict';
import { handleOwnerPasswordAuth, withOwnerPasswordPrincipal } from '../src/crm-owner-password-auth.mjs';

const ENV={
  CRM_OWNER_AUTH_MODE:'password',
  CRM_OWNER_PASSWORD:'correct-horse-battery-staple',
  CRM_OWNER_SESSION_SECRET:'test-session-secret-32-bytes-minimum-value',
  CRM_OWNER_LOGIN_RATE_LIMITER:{limit:async()=>({success:true})}
};
const ORIGIN='https://crm.example.test';

test('password-authenticated body-bearing request preserves method body and canonical principal',async()=>{
  const login=await handleOwnerPasswordAuth(new Request(`${ORIGIN}/__crm/owner-login`,{
    method:'POST',
    headers:{'content-type':'application/x-www-form-urlencoded',origin:ORIGIN},
    body:new URLSearchParams({password:ENV.CRM_OWNER_PASSWORD})
  }),ENV);
  assert.equal(login.status,303);
  const cookie=(login.headers.get('set-cookie')||'').split(';')[0];
  const payload=JSON.stringify({memo:'body-must-survive'});
  const original=new Request(`${ORIGIN}/api/customer360/profile/26000001`,{
    method:'PATCH',
    headers:{cookie,'content-type':'application/json','x-user-email':'attacker@example.com'},
    body:payload
  });
  const effective=await withOwnerPasswordPrincipal(original,ENV);
  assert.equal(effective.method,'PATCH');
  assert.equal(effective.headers.get('cf-access-authenticated-user-email'),'ohw3rz5578d277e@gmail.com');
  assert.equal(effective.headers.get('x-user-email'),null);
  assert.equal(effective.headers.get('x-crm-owner-auth'),'password-session');
  assert.equal(await effective.text(),payload);
});

console.log('CRM_OWNER_PASSWORD_AUTH_BODY_PRESERVATION=PASS');
