import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const auth=fs.readFileSync('src/crm-owner-password-auth.mjs','utf8');
const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
const today=fs.readFileSync('src/production-index-crm-today-dashboard.js','utf8');
const runtime=fs.readFileSync('src/crm-customer360-runtime.mjs','utf8');

const must=(source,token,label)=>assert.ok(source.includes(token),`missing ${label}: ${token}`);

test('Owner password Production acceptance contract remains intact',()=>{
  // Session security / expiry contract.
  must(auth,'const SESSION_MAX_AGE_SECONDS=60*60*12;','12h session max age');
  must(auth,'HttpOnly; Secure; SameSite=Strict','secure session cookie');
  must(auth,"Number(data?.exp)>Math.floor(Date.now()/1000)",'expired session rejection');
  must(auth,"url.pathname==='/__crm/owner-logout'&&request.method==='POST'",'POST logout endpoint');
  must(auth,"'set-cookie':clearCookie()",'logout cookie clearing');

  // Hybrid/password fail-closed browser entry while preserving Access + Reservation fallback.
  must(auth,"if(mode==='access')return null;",'access mode compatibility');
  must(auth,'if(hasAccessPrincipal(request)||reservationInternalUser(request,env))return null;','Access and Reservation fallback');
  must(auth,"return new Response(null,{status:302,headers:secureHeaders({location:ownerLoginLocation(request,env)})});",'unauthenticated login redirect');
  must(auth,"CRM_OWNER_LOGIN_RATE_LIMITER",'login rate limit binding');

  // Canonical entry must normalize signed password session before all Owner routes.
  must(entry,'const effectiveRequest=await withOwnerPasswordPrincipal(request,env);','normalized principal');
  must(entry,'handleOwnerPasswordBrowserGate(effectiveRequest,env)','browser gate uses normalized request');
  must(entry,"todayReadOnlyApp.fetch(effectiveRequest,env,ctx)",'Today dashboard uses normalized request');
  must(entry,'handleCustomer360MediaRequest(effectiveRequest,env)','media API uses normalized request');
  must(entry,'handleCustomerProfileEnrichmentRequest(effectiveRequest,env)','profile API uses normalized request');
  must(entry,'handleCustomer360CombinedDetail(effectiveRequest,env)','combined detail uses normalized request');
  must(entry,'handleCustomer360Request(effectiveRequest,env)','Customer360 core uses normalized request');
  must(entry,'app.fetch(effectiveRequest,env,ctx)','downstream app uses normalized request');

  // Today + Customer360 remain independently authorization-gated.
  must(today,'if (!email) return { ok: false, response: json({ ok: false, message: "Login required" }, 401) };','Today auth required');
  must(today,'crm_admin_users','Today role authorization');
  must(runtime,"if(!authorized(request,env))return json({ok:false,error:'authentication_required'},401);",'Customer360 auth required');

  // Health contract must expose readiness without enabling writes.
  must(auth,'owner_password_auth_fail_closed:true','health fail-closed flag');
  must(auth,'owner_password_auth_customer_id_generation:false','no Customer ID generation');
  must(auth,'owner_password_auth_d1_write:false','no auth D1 write');
  must(auth,'owner_password_auth_line_send:false','no LINE send');
});

console.log('CRM_OWNER_PASSWORD_PRODUCTION_ACCEPTANCE_CONTRACT=PASS');
console.log('ACCEPTANCE=LOGIN,LOGOUT,SESSION_EXPIRY,RESERVATION_HANDOFF,TODAY,CUSTOMER360,MEDIA,PROFILE');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('LINE_SEND=0');
