import {
  handleMemberLineLoginHttpRequest,
  memberLineLoginHttpContractHealth,
  __test
} from '../src/member-line-login-http-contract.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const now=1800000000;
const origin='https://member.example.test';
const callbackUrl=origin+__test.CALLBACK_PATH;
const env={
  MEMBER_LINE_LOGIN_CHANNEL_ID:'1234567890',
  MEMBER_LINE_LOGIN_REDIRECT_URI:callbackUrl,
  MEMBER_LINE_LOGIN_TRANSACTION_SECRET:'line-login-transaction-secret-123456789012345',
  MEMBER_LINE_LOGIN_CHANNEL_SECRET:'line-channel-secret-not-for-output',
  MEMBER_SESSION_SECRET:'member-session-secret-12345678901234567890'
};

function cookies(response){
  if(typeof response.headers.getSetCookie==='function')return response.headers.getSetCookie();
  const value=response.headers.get('set-cookie');
  return value?[value]:[];
}

const unrelated=await handleMemberLineLoginHttpRequest(
  new Request(origin+'/api/internal/member/home'),
  env
);
pass('unrelated path is not handled',unrelated===null);

const wrongMethod=await handleMemberLineLoginHttpRequest(
  new Request(origin+__test.START_PATH,{method:'POST'}),
  env
);
pass('login start is GET-only',wrongMethod.status===405);

let txCalls=0;
const badReturn=await handleMemberLineLoginHttpRequest(
  new Request(origin+__test.START_PATH+'?return_to=%2Fadmin'),
  env,
  {
    create_transaction:async()=>{txCalls++;throw new Error('must not create transaction')}
  }
);
pass('start only accepts Member-local return target',badReturn.status===400&&(await badReturn.json()).error==='invalid_return_to');
pass('invalid Member return target creates no transaction',txCalls===0);

const badQuery=await handleMemberLineLoginHttpRequest(
  new Request(origin+__test.START_PATH+'?return_to=%2Fmember&next=%2Fadmin'),
  env,
  {
    create_transaction:async()=>{txCalls++;throw new Error('must not create transaction')}
  }
);
pass('start rejects unexpected query parameters',badQuery.status===400&&(await badQuery.json()).error==='invalid_start_query');
pass('unexpected query creates no transaction',txCalls===0);

const wrongCallbackEnv={
  ...env,
  MEMBER_LINE_LOGIN_REDIRECT_URI:origin+'/wrong/callback'
};
const badConfig=await handleMemberLineLoginHttpRequest(
  new Request(origin+__test.START_PATH),
  wrongCallbackEnv,
  {
    create_transaction:async()=>{txCalls++;throw new Error('must not create transaction')}
  }
);
pass('configured redirect must point at source callback contract',badConfig.status===503&&(await badConfig.json()).error==='member_line_login_callback_route_not_configured');
pass('callback route mismatch creates no transaction',txCalls===0);

const start=await handleMemberLineLoginHttpRequest(
  new Request(origin+__test.START_PATH+'?return_to=%2Fmember%2Fmemories'),
  env,
  {now_seconds:now}
);
pass('start redirects to LINE authorization',start.status===302&&new URL(start.headers.get('location')).origin==='https://access.line.me');
pass('start response is private and non-indexable',start.headers.get('cache-control').includes('no-store')&&start.headers.get('x-robots-tag').includes('noindex'));
const startCookies=cookies(start);
pass('start sets signed one-shot transaction cookie',startCookies.join(';').includes('__Host-mizuno_member_login_tx='));

const authUrl=new URL(start.headers.get('location'));
const state=authUrl.searchParams.get('state');
const txCookie=startCookies.join(';').match(/__Host-mizuno_member_login_tx=([^;,]+)/)?.[1]||'';
assert(state&&txCookie,'start fixture missing state/cookie');

const denied=await handleMemberLineLoginHttpRequest(
  new Request(callbackUrl+'?error=access_denied&state='+encodeURIComponent(state),{
    headers:{cookie:'__Host-mizuno_member_login_tx='+txCookie}
  }),
  env,
  {now_seconds:now}
);
pass('LINE authorization denial fails closed',denied.status===400&&(await denied.clone().json()).error==='line_authorization_denied');
pass('failed callback clears transaction cookie',cookies(denied).join(';').includes('__Host-mizuno_member_login_tx=')&&cookies(denied).join(';').includes('Max-Age=0'));

let capturedApproved=null;
let capturedVerified=null;
const callbackNoApproval=await handleMemberLineLoginHttpRequest(
  new Request(callbackUrl+'?code=code-123&state='+encodeURIComponent(state),{
    headers:{cookie:'__Host-mizuno_member_login_tx='+txCookie}
  }),
  env,
  {
    now_seconds:now,
    execute_exchange:async(_env,verified,options)=>{
      capturedVerified=verified;
      capturedApproved=options.approved;
      return {
        status:'line_login_activation_authorization_required',
        ok:false,
        stage:'activation_gate'
      };
    }
  }
);
pass('callback transaction is verified before executor',capturedVerified?.status==='ok'&&capturedVerified?.verified===true&&capturedVerified?.return_to==='/member/memories');
pass('HTTP contract does not invent activation approval',capturedApproved===false);
pass('default callback stays blocked without activation approval',callbackNoApproval.status===503&&(await callbackNoApproval.clone().json()).error==='line_login_activation_authorization_required');
pass('blocked callback clears transaction cookie',cookies(callbackNoApproval).join(';').includes('Max-Age=0'));
pass('blocked callback sets no Member session cookie',!cookies(callbackNoApproval).join(';').includes('__Host-mizuno_member_session='));

let approvedSeen=false;
const success=await handleMemberLineLoginHttpRequest(
  new Request(callbackUrl+'?code=code-123&state='+encodeURIComponent(state),{
    headers:{cookie:'__Host-mizuno_member_login_tx='+txCookie}
  }),
  env,
  {
    approved:true,
    now_seconds:now,
    execute_exchange:async(_env,verified,options)=>{
      approvedSeen=options.approved===true;
      return {
        status:'ok',
        ok:true,
        stage:'complete',
        return_to:verified.return_to,
        clear_transaction_cookie:verified.clear_cookie,
        member_session_cookie:'__Host-mizuno_member_session=signed-session; Path=/; Max-Age=604800; HttpOnly; Secure; SameSite=Lax',
        member_session_issued:true,
        production_write:false
      };
    }
  }
);
pass('explicit caller approval reaches guarded executor',approvedSeen===true);
pass('successful callback uses 303 local Member redirect',success.status===303&&success.headers.get('location')===origin+'/member/memories');
const successCookies=cookies(success).join(';');
pass('successful callback clears transaction cookie',successCookies.includes('__Host-mizuno_member_login_tx=')&&successCookies.includes('Max-Age=0'));
pass('successful callback sets signed Member session cookie',successCookies.includes('__Host-mizuno_member_session=signed-session'));
pass('callback response remains private and non-indexable',success.headers.get('cache-control').includes('no-store')&&success.headers.get('x-robots-tag').includes('noindex'));

const mismatch=await handleMemberLineLoginHttpRequest(
  new Request(callbackUrl+'?code=code-123&state='+encodeURIComponent(state),{
    headers:{cookie:'__Host-mizuno_member_login_tx='+txCookie}
  }),
  env,
  {
    approved:true,
    now_seconds:now,
    execute_exchange:async(_env,verified)=>({
      status:'ok',
      ok:true,
      return_to:'/member/my',
      clear_transaction_cookie:verified.clear_cookie,
      member_session_cookie:'__Host-mizuno_member_session=signed-session; Path=/; HttpOnly; Secure; SameSite=Lax'
    })
  }
);
pass('executor cannot change signed transaction return target',mismatch.status===400&&(await mismatch.clone().json()).error==='member_line_login_return_target_mismatch');
pass('return target mismatch does not set Member session cookie',!cookies(mismatch).join(';').includes('__Host-mizuno_member_session='));

pass('Member return helper accepts canonical Member paths',__test.memberReturnTo('/member')==='/member'&&__test.memberReturnTo('/member/my?tab=family')==='/member/my?tab=family');
pass('Member return helper rejects non-Member and protocol-relative paths',__test.memberReturnTo('/admin')===''&&__test.memberReturnTo('//evil.example')==='');

const health=memberLineLoginHttpContractHealth({});
pass('health reports source-only HTTP contract',health.member_line_login_http_contract===true&&health.source_only===true);
pass('health reports Production route not wired',health.production_route_wired===false&&health.line_login_activated===false);
pass('health reports external exchange default disabled',health.external_exchange_default_disabled===true&&health.explicit_activation_approval_required===true);
pass('health keeps all identity mutation and send actions off',health.customer_create===false&&health.customer_id_generation===false&&health.family_create===false&&health.family_auto_link===false&&health.canonical_crm_write===false&&health.line_send===false&&health.production_write===false);

console.log(`MEMBER_LINE_LOGIN_HTTP_CONTRACT=${n}/${n} PASS`);
