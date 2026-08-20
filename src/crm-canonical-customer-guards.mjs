// TASK-CRM-LINE-FOLLOW-CANONICAL-CUSTOMER-01
// Canonical LINE identity guards for Customer CRM.
// Display names are metadata only; identity matching is formal LINE user ID only.

import { resolveOrCreateCustomerIdentity, isFormalLineUserId, isPlaceholderCustomerName } from './customer-identity-resolver.mjs';

const BUILD='crm-canonical-customer-guards-20260820-04';
const GUARDED_UPSERT_PATHS=new Set(['/api/customers/upsert','/api/sync/customers/upsert']);
function text(v){return v==null?'':String(v).trim();}
function json(data,status=200){return new Response(JSON.stringify(data,null,2),{status,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store','x-crm-canonical-customer-guards-build':BUILD}});}
function bearer(req){const h=text(req.headers.get('authorization'));return /^Bearer\s+/i.test(h)?h.replace(/^Bearer\s+/i,'').trim():'';}
function internalToken(req){return text(req.headers.get('x-internal-token'))||bearer(req);}
export {isFormalLineUserId};
function displayNameFromPayload(v){return text(v&&(v.line_display_name||v.display_name||v.profile_display_name));}
function itemsOf(body){return Array.isArray(body)?body:Array.isArray(body&&body.items)?body.items:[body];}
async function existingIdentity(env,customerId){if(!env||!env.DB||!customerId)return null;try{return await env.DB.prepare('SELECT customer_id,name,line_user_id FROM customers WHERE customer_id=? LIMIT 1').bind(customerId).first();}catch(_){return null;}}
async function ownersByLine(env,lineUserId){if(!env||!env.DB||!lineUserId)return[];try{const r=await env.DB.prepare('SELECT customer_id,line_user_id FROM customers WHERE line_user_id=? LIMIT 3').bind(lineUserId).all();return r.results||[];}catch(_){return[];}}

async function sanitizeCustomerItem(item,env){
  const next={...item};
  const flags={ignored_invalid_line_user_id:false,protected_existing_real_name:false,display_name_fallback:false};
  const customerId=text(next.customer_id);
  const existing=await existingIdentity(env,customerId);
  const incomingLineId=text(next.line_user_id||next.lineUserId||next.line_id||next.line_uid||next.line_mid);
  if(incomingLineId){
    if(isFormalLineUserId(incomingLineId)){
      const existingLineId=text(existing&&existing.line_user_id);
      if(existingLineId && existingLineId!==incomingLineId){
        return {item:next,flags,conflict:{error:'customer_line_identity_conflict',customer_id:customerId,existing_line_user_id:existingLineId,incoming_line_user_id:incomingLineId}};
      }
      const owners=await ownersByLine(env,incomingLineId);
      const foreign=owners.filter(r=>text(r.customer_id)!==customerId);
      if(foreign.length){
        return {item:next,flags,conflict:{error:'line_identity_already_owned',customer_id:customerId||null,line_user_id:incomingLineId,owner_customer_ids:foreign.map(r=>text(r.customer_id)).filter(Boolean)}};
      }
      next.line_user_id=incomingLineId;
    }else{
      delete next.line_user_id;delete next.lineUserId;delete next.line_id;delete next.line_uid;delete next.line_mid;
      next.line_user_id=null;
      flags.ignored_invalid_line_user_id=true;
    }
  }

  const displayName=displayNameFromPayload(next);
  if(displayName) next.line_display_name=displayName;
  const currentName=text(existing&&existing.name);
  const incomingName=text(next.name||next.customer_name);
  if(!isPlaceholderCustomerName(currentName) && (isPlaceholderCustomerName(incomingName) || (!incomingName&&displayName))){
    next.name=currentName;
    flags.protected_existing_real_name=true;
  }else if(isPlaceholderCustomerName(incomingName) && displayName && !isPlaceholderCustomerName(displayName)){
    next.name=displayName;
    flags.display_name_fallback=true;
  }else if(isPlaceholderCustomerName(incomingName) && !displayName){
    delete next.name;delete next.customer_name;
  }
  next.identity_guard={...(next.identity_guard||{}),...flags};
  return {item:next,flags,conflict:null};
}

export async function handleCanonicalLineFollow(request,env){
  const u=new URL(request.url);
  if(u.pathname!=='/api/internal/line/follow-canonical-customer')return null;
  if(request.method!=='POST')return json({ok:false,error:'method_not_allowed'},405);
  const expected=text(env&&env.CRM_INTERNAL_TOKEN);
  if(!expected)return json({ok:false,error:'identity_auth_not_configured'},503);
  if(internalToken(request)!==expected)return json({ok:false,error:'unauthorized'},401);
  let body={};try{body=await request.json();}catch(_){return json({ok:false,error:'invalid_json'},400);}
  const lineUserId=text(body.line_user_id||body.user_id);
  if(!isFormalLineUserId(lineUserId))return json({ok:false,error:'invalid_line_user_id',review_required:false},400);
  const eventId=text(body.webhook_event_id||body.event_id||body.follow_event_id||body.idempotency_key);
  const displayName=displayNameFromPayload(body);
  const resolved=await resolveOrCreateCustomerIdentity(env,{
    line_user_id:lineUserId,
    line_display_name:displayName,
    idempotency_key:eventId?`line_follow:${eventId}`:`line_follow_user:${lineUserId}`,
    source:text(body.source)||'line_follow',
    webhook_event_id:eventId,
    followed_at:text(body.followed_at)
  });
  const status=resolved.statusCode||200;delete resolved.statusCode;
  return json({
    ...resolved,
    build:BUILD,
    line_user_id:lineUserId,
    line_display_name:displayName||null,
    line_profile_status:displayName?'provided':'missing',
    identity_matching:'line_user_id_only',
    name_matching:false,
    line_send_executed:false
  },status);
}

export async function handleGuardedCustomerUpsert(request,env,app,ctx){
  const u=new URL(request.url);
  if(request.method!=='POST'||!GUARDED_UPSERT_PATHS.has(u.pathname))return null;
  let body={};try{body=await request.clone().json();}catch(_){return app.fetch(request,env,ctx);}
  const source=itemsOf(body).filter(Boolean);
  const sanitized=[];let invalid=0,protectedNames=0,displayFallback=0;
  for(const item of source){
    const r=await sanitizeCustomerItem(item,env);
    if(r.conflict)return json({ok:false,...r.conflict,review_required:true,canonical_line_guard:true,mutation_forwarded:false,build_guard:BUILD},409);
    sanitized.push(r.item);
    if(r.flags.ignored_invalid_line_user_id)invalid++;
    if(r.flags.protected_existing_real_name)protectedNames++;
    if(r.flags.display_name_fallback)displayFallback++;
  }
  const payload=Array.isArray(body)?sanitized:(Array.isArray(body&&body.items)?{...body,items:sanitized}:sanitized[0]);
  const headers=new Headers(request.headers);headers.delete('content-length');headers.set('content-type','application/json');headers.set('x-crm-canonical-line-guard','1');
  const guarded=new Request(request.url,{method:'POST',headers,body:JSON.stringify(payload),redirect:request.redirect});
  const res=await app.fetch(guarded,env,ctx);
  const ct=res.headers.get('content-type')||'';
  if(!ct.includes('application/json'))return res;
  const raw=await res.text();let data={};try{data=raw?JSON.parse(raw):{};}catch(_){data={raw};}
  return json({...data,canonical_line_guard:true,ignored_invalid_line_user_id_count:invalid,protected_existing_real_name_count:protectedNames,display_name_fallback_count:displayFallback,build_guard:BUILD},res.status);
}

export function canonicalCustomerGuardHealth(){
  return {
    canonical_customer_guard_enabled:true,
    canonical_line_id_regex:'^U[0-9a-fA-F]{20,}$',
    canonical_customer_id_owner:'customer-crm',
    canonical_line_id_name_matching:false,
    canonical_invalid_line_id_overwrite:false,
    canonical_existing_line_id_relink:false,
    canonical_duplicate_line_identity_write:false,
    canonical_placeholder_name_overwrite:false,
    canonical_guarded_upsert_paths:[...GUARDED_UPSERT_PATHS],
    canonical_line_follow_receiver:'/api/internal/line/follow-canonical-customer',
    canonical_line_send_enabled:false,
    canonical_customer_guard_build:BUILD
  };
}
