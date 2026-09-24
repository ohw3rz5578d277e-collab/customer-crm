import { verifyMemberSessionRequest } from './member-session-foundation.mjs';
import { executeMemberFavoriteMutation } from './member-favorites-write-executor.mjs';
import { consumeMemberFavoriteMutationRateLimit, memberFavoritesRateLimitHealth } from './member-favorites-rate-limit.mjs';

const BUILD='member-favorites-http-contract-20260925-01';
const ROUTE_MODE='enabled';
const MAX_BODY_BYTES=2048;
const encoder=new TextEncoder();

const text=v=>v==null?'':String(v).trim();

function json(data,status=200,extraHeaders={}){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-member-favorites-http-build':BUILD,
      'x-robots-tag':'noindex, nofollow',
      'referrer-policy':'no-referrer',
      ...extraHeaders
    }
  });
}

function routeEnabled(env){
  return text(env?.MEMBER_FAVORITES_MUTATION_ROUTE_MODE).toLowerCase()===ROUTE_MODE;
}

function sameOrigin(request){
  const raw=request?.headers?.get?.('origin')||'';
  if(!raw||raw==='null')return false;
  try{
    const requestUrl=new URL(request.url);
    const originUrl=new URL(raw);
    return originUrl.origin===requestUrl.origin;
  }catch{
    return false;
  }
}

function jsonContentType(request){
  const raw=text(request?.headers?.get?.('content-type')).toLowerCase();
  return raw==='application/json'||raw.startsWith('application/json;');
}

function exactMutationBody(value){
  if(!value||typeof value!=='object'||Array.isArray(value))return null;
  const keys=Object.keys(value).sort();
  if(keys.length!==2||keys[0]!=='desired_favorite'||keys[1]!=='memory_id')return null;
  if(typeof value.memory_id!=='string')return null;
  if(typeof value.desired_favorite!=='boolean')return null;
  return {
    memory_id:value.memory_id,
    desired_favorite:value.desired_favorite
  };
}

async function readJsonBody(request){
  const header=text(request?.headers?.get?.('content-length'));
  if(header){
    const declared=Number(header);
    if(!Number.isInteger(declared)||declared<0)return {ok:false,error:'invalid_content_length'};
    if(declared>MAX_BODY_BYTES)return {ok:false,error:'request_body_too_large'};
  }

  let raw='';
  try{
    raw=await request.text();
  }catch{
    return {ok:false,error:'invalid_json_body'};
  }

  if(encoder.encode(raw).byteLength>MAX_BODY_BYTES){
    return {ok:false,error:'request_body_too_large'};
  }

  let parsed;
  try{
    parsed=JSON.parse(raw);
  }catch{
    return {ok:false,error:'invalid_json_body'};
  }

  const body=exactMutationBody(parsed);
  if(!body)return {ok:false,error:'invalid_mutation_body'};
  return {ok:true,body};
}

function mapExecutorStatus(result){
  const status=text(result?.status);
  if(status==='ok')return 200;
  if(status==='memory_not_found')return 404;
  if(status==='family_access_denied')return 403;
  if(
    status==='invalid_memory_id'
    || status==='invalid_desired_favorite'
    || status==='invalid_member_session'
  )return 400;
  if(
    status==='favorites_schema_not_applied'
    || status==='member_memory_schema_not_applied'
    || status==='schema_not_applied'
    || status==='ambiguous_family_identity'
  )return 409;
  if(status==='favorites_write_disabled'||status==='write_approval_required')return 503;
  if(status==='unlinked'||status==='family_inactive_or_missing')return 403;
  if(status==='favorite_write_failed'||status==='favorite_postwrite_verification_failed')return 409;
  return 409;
}

export async function handleMemberFavoriteMutationRequest(request,env){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member/favorites/mutate')return null;

  if(!routeEnabled(env)){
    return json({ok:false,error:'not_found'},404);
  }
  if(request.method!=='POST'){
    return json({ok:false,error:'method_not_allowed'},405);
  }
  if(!sameOrigin(request)){
    return json({ok:false,error:'same_origin_required'},403);
  }
  if(!jsonContentType(request)){
    return json({ok:false,error:'application_json_required'},415);
  }

  const verified=await verifyMemberSessionRequest(request,env);
  if(verified.status!=='ok'||verified.verified!==true||!verified.session){
    return json({ok:false,error:'member_session_required'},401);
  }

  const parsed=await readJsonBody(request);
  if(!parsed.ok){
    const status=parsed.error==='request_body_too_large'?413:400;
    return json({ok:false,error:parsed.error},status);
  }

  const rate=await consumeMemberFavoriteMutationRateLimit(
    env,
    verified.session,
    {
      memory_id:parsed.body.memory_id
    }
  );

  if(rate.status==='rate_limited'){
    return json(
      {ok:false,error:'rate_limited'},
      429,
      {'retry-after':String(rate.retry_after_seconds||1)}
    );
  }

  if(rate.status==='invalid_memory_id'){
    return json({ok:false,error:'invalid_memory_id'},400);
  }

  if(rate.status!=='ok'||rate.allowed!==true){
    return json({ok:false,error:'favorite_rate_limit_unavailable'},503);
  }

  const result=await executeMemberFavoriteMutation(
    env,
    verified.session,
    {
      ...parsed.body,
      approved:true
    }
  );

  const status=mapExecutorStatus(result);
  if(status===200){
    return json({
      ok:true,
      memory_id:result.memory_id,
      favorite:result.favorite===true,
      changed:result.favorite_changed===true,
      idempotent_noop:result.idempotent_noop===true
    });
  }

  return json({
    ok:false,
    error:result.status||'favorite_mutation_unavailable',
    review_required:result.review_required===true
  },status);
}

export function memberFavoritesHttpContractHealth(env){
  const rateLimit=memberFavoritesRateLimitHealth(env);
  return {
    member_favorites_http_contract:true,
    build:BUILD,
    route_mode:routeEnabled(env)?'enabled':'disabled',
    route_default_disabled:true,
    conceptual_path:'/api/internal/member/favorites/mutate',
    production_route_wired:false,
    method:'POST',
    content_type:'application/json',
    max_body_bytes:MAX_BODY_BYTES,
    exact_request_keys:['memory_id','desired_favorite'],
    client_approved_input:false,
    client_customer_id_input:false,
    client_family_id_input:false,
    signed_member_session_cookie_required:true,
    same_origin_required:true,
    same_site_cookie_defense:'Lax',
    declarative_desired_state:true,
    executor_write_mode_still_required:true,
    rate_limit_required:true,
    rate_limit_mode:rateLimit.mode,
    rate_limit_default_disabled:rateLimit.default_disabled,
    rate_limit_window_seconds:rateLimit.fixed_window_seconds,
    rate_limit_customer_attempts:rateLimit.customer_attempt_limit,
    rate_limit_memory_attempts:rateLimit.memory_attempt_limit,
    rate_limit_retry_after_supported:rateLimit.retry_after_supported,
    automatic_write:false,
    line_send:false,
    production_write_enabled:false
  };
}

export const __test={
  routeEnabled,
  sameOrigin,
  jsonContentType,
  exactMutationBody,
  readJsonBody,
  mapExecutorStatus,
  MAX_BODY_BYTES
};
