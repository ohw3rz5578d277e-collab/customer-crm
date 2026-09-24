import { isFormalLineUserId } from './customer-identity-resolver.mjs';

const BUILD='crm-member-line-identity-20260924-01';
const CUSTOMER_ID_RE=/^\d{8}$/;

const text=v=>v==null?'':String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-crm-member-line-identity-build':BUILD,
      'x-robots-tag':'noindex, nofollow',
      'referrer-policy':'no-referrer'
    }
  });
}

function bearer(request){
  const h=text(request.headers.get('authorization'));
  return /^Bearer\s+/i.test(h)?h.replace(/^Bearer\s+/i,'').trim():'';
}

function internalAllowed(request,env){
  const expected=text(env?.CRM_INTERNAL_TOKEN);
  const supplied=text(request.headers.get('x-internal-token')||bearer(request));
  return !!expected&&supplied===expected;
}

async function all(env,sql,params=[]){
  let q=env.DB.prepare(sql);
  if(params.length)q=q.bind(...params);
  const r=await q.all();
  return r.results||[];
}

export async function resolveCanonicalCustomerByVerifiedLineUserId(env,lineUserId){
  const lineId=text(lineUserId);
  if(!isFormalLineUserId(lineId)){
    return {status:'invalid_line_user_id',customer_id:null};
  }

  const rows=await all(
    env,
    "SELECT customer_id,line_user_id FROM customers WHERE line_user_id=? AND COALESCE(deleted_at,'')='' LIMIT 3",
    [lineId]
  );

  const exact=rows.filter(row=>text(row.line_user_id)===lineId);

  if(exact.length===0){
    return {status:'unlinked',customer_id:null};
  }
  if(exact.length>1){
    return {status:'ambiguous_line_identity',customer_id:null,match_count:exact.length};
  }

  const customerId=text(exact[0].customer_id);
  if(!CUSTOMER_ID_RE.test(customerId)){
    return {status:'noncanonical_customer_id',customer_id:null};
  }

  return {
    status:'linked',
    customer_id:customerId
  };
}

export async function handleMemberLineIdentityReadRequest(request,env){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member-line-identity/resolve')return null;
  if(request.method!=='POST')return json({ok:false,error:'method_not_allowed'},405);
  if(!internalAllowed(request,env))return json({ok:false,error:'authentication_required'},401);

  const body=await request.json().catch(()=>null);
  if(!body||typeof body!=='object'||Array.isArray(body)){
    return json({ok:false,error:'invalid_json'},400);
  }

  const result=await resolveCanonicalCustomerByVerifiedLineUserId(env,body.line_user_id);

  if(result.status==='linked'){
    return json({
      ok:true,
      status:'linked',
      customer_id:result.customer_id,
      identity_source:'verified_line_user_id_exact',
      fallback_used:false
    });
  }

  if(result.status==='unlinked'){
    return json({
      ok:true,
      status:'unlinked',
      customer_id:null,
      identity_source:'verified_line_user_id_exact',
      fallback_used:false
    });
  }

  if(result.status==='ambiguous_line_identity'){
    return json({ok:false,error:'ambiguous_line_identity',review_required:true},409);
  }

  if(result.status==='noncanonical_customer_id'){
    return json({ok:false,error:'noncanonical_customer_id',review_required:true},409);
  }

  return json({ok:false,error:'invalid_line_user_id'},400);
}

export function memberLineIdentityHealth(){
  return {
    member_line_identity_foundation:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    verified_line_token_required_upstream:true,
    exact_line_user_id_only:true,
    name_fallback:false,
    phone_fallback:false,
    email_fallback:false,
    line_display_name_fallback:false,
    customer_create:false,
    customer_id_generation:false,
    production_write:false
  };
}
