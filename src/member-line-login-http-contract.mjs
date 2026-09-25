import {
  createMemberLineLoginTransaction,
  verifyMemberLineLoginCallback
} from './member-line-login-transaction.mjs';
import {
  executeMemberLineLoginExchange,
  memberLineLoginExchangeExecutorHealth
} from './member-line-login-exchange-executor.mjs';

const BUILD='member-line-login-http-contract-20260925-01';
const START_PATH='/api/member/login/line/start';
const CALLBACK_PATH='/api/member/login/line/callback';

const text=value=>value==null?'':String(value).trim();

function baseHeaders(extra={}){
  return {
    'cache-control':'no-store, no-cache, must-revalidate, max-age=0',
    'pragma':'no-cache',
    'x-content-type-options':'nosniff',
    'x-frame-options':'DENY',
    'x-robots-tag':'noindex, nofollow, noarchive',
    'referrer-policy':'no-referrer',
    'cross-origin-opener-policy':'same-origin',
    'x-member-line-login-http-build':BUILD,
    ...extra
  };
}

function json(data,status=200,cookies=[]){
  const headers=new Headers(baseHeaders({
    'content-type':'application/json; charset=utf-8'
  }));
  for(const cookie of cookies.map(text).filter(Boolean)){
    headers.append('set-cookie',cookie);
  }
  return new Response(JSON.stringify(data),{status,headers});
}

function redirect(location,status,cookies=[]){
  const headers=new Headers(baseHeaders({location}));
  for(const cookie of cookies.map(text).filter(Boolean)){
    headers.append('set-cookie',cookie);
  }
  return new Response(null,{status,headers});
}

function memberReturnTo(value){
  const raw=text(value)||'/member';
  if(!raw.startsWith('/')||raw.startsWith('//'))return '';
  if(/[\u0000-\u001f\u007f]/.test(raw))return '';
  try{
    const u=new URL(raw,'https://member.invalid');
    if(u.origin!=='https://member.invalid')return '';
    if(u.pathname!=='/member'&&!u.pathname.startsWith('/member/'))return '';
    return u.pathname+u.search+u.hash;
  }catch{
    return '';
  }
}

function callbackRouteConfigured(env){
  const configured=text(env?.MEMBER_LINE_LOGIN_REDIRECT_URI);
  if(!configured)return false;
  try{
    const u=new URL(configured);
    return (
      u.protocol==='https:'
      && !u.username
      && !u.password
      && !u.hash
      && u.pathname===CALLBACK_PATH
    );
  }catch{
    return false;
  }
}

function exactStartQuery(url){
  const keys=[...url.searchParams.keys()];
  if(keys.some(key=>key!=='return_to'))return {ok:false,error:'invalid_start_query'};
  if(url.searchParams.getAll('return_to').length>1)return {ok:false,error:'invalid_start_query'};
  const returnTo=memberReturnTo(url.searchParams.get('return_to')||'/member');
  if(!returnTo)return {ok:false,error:'invalid_return_to'};
  return {ok:true,return_to:returnTo};
}

function executorStatus(result){
  const status=text(result?.status);
  if(
    status==='line_login_external_exchange_disabled'
    || status==='line_login_activation_authorization_required'
    || status==='line_login_channel_secret_not_configured'
    || status==='trusted_fetch_required'
    || status==='trusted_identity_dependencies_required'
    || status==='member_session_secret_not_configured'
  )return 503;

  if(
    status==='line_token_exchange_network_failed'
    || status==='line_token_exchange_rejected'
    || status==='line_id_token_verification_network_failed'
    || status==='line_id_token_verification_rejected'
    || status==='external_response_invalid'
    || status==='external_response_read_failed'
    || status==='external_response_too_large'
    || status==='external_response_json_invalid'
  )return 502;

  if(
    status.startsWith('line_id_token_')
    || status==='invalid_id_token'
    || status==='invalid_authorization_code'
  )return 401;

  if(
    status==='member_customer_unlinked'
    || status==='unlinked'
    || status==='family_inactive_or_missing'
  )return 403;

  if(result?.review_required===true||status.includes('ambiguous')||status==='noncanonical_customer_id'){
    return 409;
  }

  return 400;
}

export async function handleMemberLineLoginHttpRequest(
  request,
  env,
  {
    approved=false,
    fetch_impl=globalThis.fetch,
    now_seconds,
    create_transaction=createMemberLineLoginTransaction,
    verify_callback=verifyMemberLineLoginCallback,
    execute_exchange=executeMemberLineLoginExchange
  }={}
){
  const url=new URL(request.url);

  if(url.pathname!==START_PATH&&url.pathname!==CALLBACK_PATH)return null;

  if(request.method!=='GET'){
    return json({
      ok:false,
      error:'method_not_allowed',
      source_only:true,
      production_write:false
    },405);
  }

  if(!callbackRouteConfigured(env)){
    return json({
      ok:false,
      error:'member_line_login_callback_route_not_configured',
      source_only:true,
      production_write:false
    },503);
  }

  if(url.pathname===START_PATH){
    const query=exactStartQuery(url);
    if(!query.ok){
      return json({
        ok:false,
        error:query.error,
        source_only:true,
        production_write:false
      },400);
    }

    const transaction=await create_transaction(env,{
      return_to:query.return_to,
      now_seconds
    });

    if(transaction?.status!=='ok'||transaction?.created!==true){
      const status=text(transaction?.status);
      const httpStatus=status.includes('not_configured')||status.includes('invalid')?503:400;
      return json({
        ok:false,
        error:status||'member_line_login_transaction_failed',
        source_only:true,
        production_write:false
      },httpStatus);
    }

    return redirect(
      transaction.authorization_url,
      302,
      [transaction.cookie]
    );
  }

  const verified=await verify_callback(request,env,{now_seconds});
  if(verified?.status!=='ok'||verified?.verified!==true){
    return json({
      ok:false,
      error:text(verified?.status)||'member_line_login_callback_verification_failed',
      source_only:true,
      production_write:false
    },400,[verified?.clear_cookie]);
  }

  const returnTo=memberReturnTo(verified.return_to);
  if(!returnTo){
    return json({
      ok:false,
      error:'invalid_return_to',
      source_only:true,
      production_write:false
    },400,[verified.clear_cookie]);
  }

  const execution=await execute_exchange(
    env,
    verified,
    {
      approved,
      fetch_impl,
      now_seconds
    }
  );

  if(execution?.status!=='ok'||execution?.ok!==true){
    return json({
      ok:false,
      error:text(execution?.status)||'member_line_login_exchange_failed',
      stage:text(execution?.stage)||'exchange',
      review_required:execution?.review_required===true,
      source_only:true,
      production_write:false
    },executorStatus(execution),[verified.clear_cookie]);
  }

  const finalReturnTo=memberReturnTo(execution.return_to);
  if(!finalReturnTo||finalReturnTo!==returnTo){
    return json({
      ok:false,
      error:'member_line_login_return_target_mismatch',
      source_only:true,
      production_write:false
    },400,[verified.clear_cookie]);
  }

  const destination=new URL(finalReturnTo,url.origin).toString();
  return redirect(
    destination,
    303,
    [
      execution.clear_transaction_cookie||verified.clear_cookie,
      execution.member_session_cookie
    ]
  );
}

export function memberLineLoginHttpContractHealth(env){
  const executor=memberLineLoginExchangeExecutorHealth(env);
  return {
    member_line_login_http_contract:true,
    build:BUILD,
    source_only:true,
    start_path:START_PATH,
    callback_path:CALLBACK_PATH,
    callback_route_configured:callbackRouteConfigured(env),
    local_member_return_only:true,
    start_method:'GET',
    callback_method:'GET',
    transaction_cookie_http_only:true,
    transaction_cookie_one_shot:true,
    session_cookie_set_only_after_verified_exchange:true,
    executor_source_ready:executor.member_line_login_exchange_executor===true,
    external_exchange_default_disabled:executor.runtime_default_disabled===true,
    explicit_activation_approval_required:true,
    production_route_wired:false,
    line_login_activated:false,
    customer_create:false,
    customer_id_generation:false,
    family_create:false,
    family_auto_link:false,
    canonical_crm_write:false,
    line_send:false,
    production_write:false
  };
}

export const __test={
  BUILD,
  START_PATH,
  CALLBACK_PATH,
  memberReturnTo,
  callbackRouteConfigured,
  exactStartQuery,
  executorStatus
};
