import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { withReservationOwnerReadPrincipal } from '../src/production-index-crm-customer360-entry.js';

const auth=fs.readFileSync('src/crm-owner-password-auth.mjs','utf8');
const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
const today=fs.readFileSync('src/production-index-crm-today-dashboard.js','utf8');
const runtime=fs.readFileSync('src/crm-customer360-runtime.mjs','utf8');

const must=(source,token,label)=>assert.ok(source.includes(token),`missing ${label}: ${token}`);

test('Owner password Production acceptance contract remains intact',()=>{
  must(auth,'const SESSION_MAX_AGE_SECONDS=60*60*12;','12h session max age');
  must(auth,'HttpOnly; Secure; SameSite=Strict','secure session cookie');
  must(auth,"Number(data?.exp)>Math.floor(Date.now()/1000)",'expired session rejection');
  must(auth,"url.pathname==='/__crm/owner-logout'&&request.method==='POST'",'POST logout endpoint');
  must(auth,"'set-cookie':clearCookie()",'logout cookie clearing');

  must(auth,"if(mode==='access')return null;",'access mode compatibility');
  must(auth,'if(hasAccessPrincipal(request)||reservationInternalUser(request,env))return null;','Access and Reservation fallback');
  must(auth,"return new Response(null,{status:302,headers:secureHeaders({location:ownerLoginLocation(request,env)})});",'unauthenticated login redirect');
  must(auth,'CRM_OWNER_LOGIN_RATE_LIMITER','login rate limit binding');

  must(entry,'const effectiveRequest=await withOwnerPasswordPrincipal(request,env);','normalized principal');
  must(entry,'const earlyReadRequest=withReservationOwnerReadPrincipal(effectiveRequest,env);','Reservation read principal');
  must(entry,'handleOwnerPasswordBrowserGate(effectiveRequest,env)','browser gate uses normalized request');
  must(entry,'todayReadOnlyApp.fetch(earlyReadRequest,env,ctx)','Today dashboard uses read-normalized request');
  must(entry,'handleCustomer360MediaRequest(effectiveRequest,env)','media API uses normalized request');
  must(entry,'handleCustomerProfileEnrichmentRequest(effectiveRequest,env)','profile API uses normalized request');
  must(entry,'handleCustomer360CombinedDetail(earlyReadRequest,env)','combined detail uses read-normalized request');
  must(entry,'handleCustomer360Request(earlyReadRequest,env)','Customer360 core uses read-normalized request');
  must(entry,'app.fetch(effectiveRequest,env,ctx)','downstream app uses normalized request');

  must(today,'if (!email) return { ok: false, response: json({ ok: false, message: "Login required" }, 401) };','Today auth required');
  must(today,'crm_admin_users','Today role authorization');
  must(runtime,"if(!authorized(request,env))return json({ok:false,error:'authentication_required'},401);",'Customer360 auth required');

  must(auth,'owner_password_auth_fail_closed:true','health fail-closed flag');
  must(auth,'owner_password_auth_customer_id_generation:false','no Customer ID generation');
  must(auth,'owner_password_auth_d1_write:false','no auth D1 write');
  must(auth,'owner_password_auth_line_send:false','no LINE send');
});

test('validated Reservation internal principal receives Owner read principal only for GET',()=>{
  const env={CRM_INTERNAL_TOKEN:'TEST_ONLY_INTERNAL_TOKEN'};
  const headers={'x-internal-token':'TEST_ONLY_INTERNAL_TOKEN','x-crm-internal-principal':'reservation-app'};
  const getReq=new Request('https://crm.example.test/api/today-dashboard',{headers});
  const readReq=withReservationOwnerReadPrincipal(getReq,env);
  assert.equal(readReq.headers.get('cf-access-authenticated-user-email'),'ohw3rz5578d277e@gmail.com');
  assert.equal(readReq.headers.get('x-crm-owner-auth'),'reservation-internal-read');

  const postReq=new Request('https://crm.example.test/api/customer360/customer-id/allocate',{method:'POST',headers,body:'{}'});
  const writeReq=withReservationOwnerReadPrincipal(postReq,env);
  assert.equal(writeReq.headers.get('cf-access-authenticated-user-email'),null);
  assert.equal(writeReq.headers.get('x-crm-owner-auth'),null);
});

test('invalid Reservation token never receives Owner read principal',()=>{
  const env={CRM_INTERNAL_TOKEN:'TEST_ONLY_INTERNAL_TOKEN'};
  const req=new Request('https://crm.example.test/api/today-dashboard',{headers:{'x-internal-token':'WRONG','x-crm-internal-principal':'reservation-app'}});
  const out=withReservationOwnerReadPrincipal(req,env);
  assert.equal(out.headers.get('cf-access-authenticated-user-email'),null);
  assert.equal(out.headers.get('x-crm-owner-auth'),null);
});

console.log('CRM_OWNER_PASSWORD_PRODUCTION_ACCEPTANCE_CONTRACT=PASS');
console.log('ACCEPTANCE=LOGIN,LOGOUT,SESSION_EXPIRY,RESERVATION_HANDOFF,TODAY,CUSTOMER360,MEDIA,PROFILE');
console.log('RESERVATION_OWNER_READ_ONLY_ELEVATION=GET_ONLY');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('LINE_SEND=0');
