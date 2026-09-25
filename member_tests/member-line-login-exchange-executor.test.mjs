import {
  executeMemberLineLoginExchange,
  memberLineLoginExchangeExecutorHealth
} from '../src/member-line-login-exchange-executor.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const now=1800000000;
const clientId='1234567890';
const redirectUri='https://member.example.test/line/callback';
const lineUserId='U1234567890abcdef1234567890abcdef';
const customerId='26000123';
const familyId='family-26000123';
const codeVerifier='a'.repeat(43);

const callback={
  status:'ok',
  verified:true,
  authorization_code:'authorization-code-123',
  nonce:'nonce-123',
  code_verifier:codeVerifier,
  client_id:clientId,
  redirect_uri:redirectUri,
  return_to:'/member/memories',
  clear_cookie:'__Host-mizuno_member_login_tx=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax'
};

const baseEnv={
  MEMBER_LINE_LOGIN_CHANNEL_ID:clientId,
  MEMBER_LINE_LOGIN_REDIRECT_URI:redirectUri,
  MEMBER_LINE_LOGIN_CHANNEL_SECRET:'line-channel-secret-not-for-output',
  MEMBER_SESSION_SECRET:'member-session-secret-12345678901234567890'
};

let calls=0;
const disabled=await executeMemberLineLoginExchange(
  baseEnv,
  callback,
  {
    approved:true,
    fetch_impl:async()=>{calls++;throw new Error('must not call')}
  }
);
pass('external exchange defaults disabled',disabled.status==='line_login_external_exchange_disabled'&&disabled.stage==='activation_gate');
pass('disabled executor performs zero external calls',calls===0);
pass('disabled executor performs zero write or send',disabled.production_write===false&&disabled.line_send===false&&disabled.customer_id_generation===false);

const enabledEnv={
  ...baseEnv,
  MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE:'enabled'
};

const noApproval=await executeMemberLineLoginExchange(
  enabledEnv,
  callback,
  {
    approved:false,
    fetch_impl:async()=>{calls++;throw new Error('must not call')}
  }
);
pass('explicit activation approval is required',noApproval.status==='line_login_activation_authorization_required');
pass('missing approval performs zero external calls',calls===0);

const incomplete=await executeMemberLineLoginExchange(
  enabledEnv,
  {status:'ok',verified:true},
  {
    approved:true,
    fetch_impl:async()=>{calls++;throw new Error('must not call')}
  }
);
pass('incomplete verified callback fails closed',incomplete.status==='verified_callback_incomplete'&&incomplete.stage==='callback');
pass('invalid callback performs zero external calls',calls===0);

const mismatch=await executeMemberLineLoginExchange(
  enabledEnv,
  {...callback,client_id:'9999999999'},
  {
    approved:true,
    fetch_impl:async()=>{calls++;throw new Error('must not call')}
  }
);
pass('callback configuration must still match current env',mismatch.status==='line_login_callback_config_mismatch');
pass('config mismatch performs zero external calls',calls===0);

const requestBodies=[];
let fetchCalls=0;
const fetchImpl=async request=>{
  fetchCalls++;
  const url=new URL(request.url);
  const body=await request.text();
  requestBodies.push({url:url.toString(),body});

  if(fetchCalls===1){
    return new Response(JSON.stringify({
      token_type:'Bearer',
      access_token:'access-token-must-not-escape',
      refresh_token:'refresh-token-must-not-escape',
      expires_in:3600,
      scope:'openid',
      id_token:'signed.id.token'
    }),{
      status:200,
      headers:{'content-type':'application/json'}
    });
  }

  if(fetchCalls===2){
    return new Response(JSON.stringify({
      iss:'https://access.line.me',
      sub:lineUserId,
      aud:clientId,
      nonce:'nonce-123',
      iat:now-10,
      exp:now+3600
    }),{
      status:200,
      headers:{'content-type':'application/json'}
    });
  }

  throw new Error('unexpected external call');
};

let resolvedLineId='';
let familyCustomerId='';
let issuedArgs=null;
const success=await executeMemberLineLoginExchange(
  enabledEnv,
  callback,
  {
    approved:true,
    now_seconds:now,
    fetch_impl:fetchImpl,
    resolve_customer:async(_env,verifiedLineId)=>{
      resolvedLineId=verifiedLineId;
      return {status:'linked',customer_id:customerId};
    },
    read_family:async(_env,resolvedCustomerId)=>{
      familyCustomerId=resolvedCustomerId;
      return {
        status:'linked',
        customer_id:resolvedCustomerId,
        family:{family_id:familyId}
      };
    },
    issue_session:async(_env,args)=>{
      issuedArgs=args;
      return {
        status:'ok',
        issued:true,
        cookie:'__Host-mizuno_member_session=signed-member-session; Path=/; HttpOnly; Secure; SameSite=Lax',
        session:{expires_at:now+3600},
        write_executed:false
      };
    }
  }
);

pass('successful executor performs exactly two LINE endpoint calls',success.status==='ok'&&fetchCalls===2&&success.external_call_count===2);
pass('first call is LINE token exchange',requestBodies[0].url==='https://api.line.me/oauth2/v2.1/token');
const exchangeParams=new URLSearchParams(requestBodies[0].body);
pass('token exchange binds authorization code and PKCE verifier',exchangeParams.get('code')==='authorization-code-123'&&exchangeParams.get('code_verifier')===codeVerifier);
pass('token exchange binds exact configured channel and redirect',exchangeParams.get('client_id')===clientId&&exchangeParams.get('redirect_uri')===redirectUri);
pass('token exchange uses configured channel secret without returning it',exchangeParams.get('client_secret')===baseEnv.MEMBER_LINE_LOGIN_CHANNEL_SECRET);

pass('second call is LINE ID token verification',requestBodies[1].url==='https://api.line.me/oauth2/v2.1/verify');
const verifyParams=new URLSearchParams(requestBodies[1].body);
pass('verification sends ID token and exact channel ID',verifyParams.get('id_token')==='signed.id.token'&&verifyParams.get('client_id')===clientId);

pass('only verified LINE subject reaches exact customer resolver',resolvedLineId===lineUserId);
pass('canonical Customer ID reaches explicit Family lookup',familyCustomerId===customerId);
pass('session issuance receives exact Customer and Family IDs',issuedArgs.customer_id===customerId&&issuedArgs.family_id===familyId);
pass('successful result returns local target and Member cookie',success.return_to==='/member/memories'&&success.member_session_cookie.startsWith('__Host-mizuno_member_session='));
pass('successful result clears one-shot transaction cookie',success.clear_transaction_cookie.startsWith('__Host-mizuno_member_login_tx='));
pass('LINE access and refresh tokens do not escape result',!JSON.stringify(success).includes('access-token-must-not-escape')&&!JSON.stringify(success).includes('refresh-token-must-not-escape'));
pass('verified LINE Customer and Family identifiers do not escape result',success.verified_line_user_id_exposed===false&&success.customer_id_exposed===false&&success.family_id_exposed===false);
pass('successful login still performs zero CRM write, ID generation, LINE send, or Family creation',success.production_write===false&&success.canonical_crm_write===false&&success.customer_id_generation===false&&success.line_send===false&&success.family_created===false&&success.family_link_created===false);

let unlinkedSessionCalls=0;
const unlinked=await executeMemberLineLoginExchange(
  enabledEnv,
  callback,
  {
    approved:true,
    now_seconds:now,
    fetch_impl:async request=>{
      const url=new URL(request.url);
      if(url.pathname.endsWith('/token')){
        return new Response(JSON.stringify({
          token_type:'Bearer',
          access_token:'a',
          expires_in:3600,
          scope:'openid',
          id_token:'signed.id.token'
        }),{status:200});
      }
      return new Response(JSON.stringify({
        iss:'https://access.line.me',
        sub:lineUserId,
        aud:clientId,
        nonce:'nonce-123',
        iat:now-10,
        exp:now+3600
      }),{status:200});
    },
    resolve_customer:async()=>({status:'unlinked',customer_id:null}),
    read_family:async()=>{throw new Error('must not read family')},
    issue_session:async()=>{unlinkedSessionCalls++;throw new Error('must not issue')}
  }
);
pass('unknown exact LINE identity remains unlinked with no Customer creation',unlinked.status==='member_customer_unlinked'&&unlinked.customer_created===false&&unlinked.customer_id_generation===false);
pass('unlinked identity never issues a Member session',unlinkedSessionCalls===0);

const badNonce=await executeMemberLineLoginExchange(
  enabledEnv,
  callback,
  {
    approved:true,
    now_seconds:now,
    fetch_impl:async request=>{
      const url=new URL(request.url);
      if(url.pathname.endsWith('/token')){
        return new Response(JSON.stringify({
          token_type:'Bearer',
          access_token:'a',
          expires_in:3600,
          scope:'openid',
          id_token:'signed.id.token'
        }),{status:200});
      }
      return new Response(JSON.stringify({
        iss:'https://access.line.me',
        sub:lineUserId,
        aud:clientId,
        nonce:'wrong-nonce',
        iat:now-10,
        exp:now+3600
      }),{status:200});
    },
    resolve_customer:async()=>{throw new Error('must not resolve customer')},
    read_family:async()=>{throw new Error('must not read family')},
    issue_session:async()=>{throw new Error('must not issue')}
  }
);
pass('nonce mismatch fails before CRM identity lookup',badNonce.status==='line_id_token_nonce_mismatch'&&badNonce.stage==='id_token_verification');

const health=memberLineLoginExchangeExecutorHealth({});
pass('health records source-only executor',health.member_line_login_exchange_executor===true&&health.source_only===true);
pass('health records external runtime default disabled',health.runtime_default_disabled===true&&health.runtime_mode==='disabled'&&health.explicit_activation_approval_required===true);
pass('health keeps Production route and activation off',health.production_route_wired===false&&health.line_login_activated===false);
pass('health keeps identity mutation and LINE send off',health.customer_create===false&&health.customer_id_generation===false&&health.family_auto_link===false&&health.canonical_crm_write===false&&health.line_send===false&&health.production_write===false);

console.log(`MEMBER_LINE_LOGIN_EXCHANGE_EXECUTOR=${n}/${n} PASS`);
