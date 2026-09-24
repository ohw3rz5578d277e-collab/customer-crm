import { authorizeMemberPrivateMediaAccess } from './member-private-media-access.mjs';
import { verifyMemberSessionRequest } from './member-session-foundation.mjs';

const BUILD='member-private-media-delivery-grant-20260925-01';
const TOKEN_VERSION='v1';
const TOKEN_TTL_SECONDS=120;
const CLOCK_SKEW_SECONDS=30;
const MIN_SECRET_LENGTH=32;
const MAX_MEDIA_ID=160;
const encoder=new TextEncoder();

const text=v=>v==null?'':String(v).trim();

function b64url(bytes){
  let s='';
  for(const b of bytes)s+=String.fromCharCode(b);
  return btoa(s).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
}

function fromB64url(value){
  const s=String(value||'').replace(/-/g,'+').replace(/_/g,'/');
  const padded=s+'='.repeat((4-(s.length%4))%4);
  try{
    const raw=atob(padded);
    const out=new Uint8Array(raw.length);
    for(let i=0;i<raw.length;i++)out[i]=raw.charCodeAt(i);
    return out;
  }catch{
    return new Uint8Array();
  }
}

function safeEqual(a,b){
  if(a.length!==b.length)return false;
  let diff=0;
  for(let i=0;i<a.length;i++)diff|=a[i]^b[i];
  return diff===0;
}

async function hmac(value,secret){
  const key=await crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    {name:'HMAC',hash:'SHA-256'},
    false,
    ['sign']
  );
  return new Uint8Array(await crypto.subtle.sign('HMAC',key,encoder.encode(value)));
}

function deliverySecret(env){
  const secret=text(env?.MEMBER_PRIVATE_MEDIA_DELIVERY_SECRET);
  return secret.length>=MIN_SECRET_LENGTH?secret:'';
}

function validMediaId(mediaId){
  const raw=mediaId==null?'':String(mediaId);
  if(!raw||raw.length>MAX_MEDIA_ID)return false;
  if(/[\u0000-\u001f\u007f]/.test(raw))return false;
  if(raw!==raw.trim())return false;
  return true;
}

function validSession(session){
  const familyId=text(session?.family_id);
  const customerId=text(session?.customer_id);
  if(!familyId||familyId.length>128)return false;
  return /^\d{8}$/.test(customerId);
}

function nowSeconds(value){
  if(Number.isFinite(Number(value)))return Math.floor(Number(value));
  return Math.floor(Date.now()/1000);
}

function signingInput(session,mediaId,iat,exp){
  return [
    TOKEN_VERSION,
    text(session.family_id),
    text(session.customer_id),
    String(mediaId),
    String(iat),
    String(exp)
  ].join('\n');
}

function parseGrant(token){
  const raw=text(token);
  const parts=raw.split('.');
  if(parts.length!==4||parts[0]!==TOKEN_VERSION)return null;

  const iat=Number(parts[1]);
  const exp=Number(parts[2]);
  const signature=parts[3];

  if(!Number.isInteger(iat)||!Number.isInteger(exp)||exp<=iat)return null;
  if(exp-iat!==TOKEN_TTL_SECONDS)return null;
  if(!signature||signature.length>256)return null;

  return {iat,exp,signature};
}

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-member-private-media-grant-build':BUILD,
      'x-robots-tag':'noindex, nofollow',
      'referrer-policy':'no-referrer'
    }
  });
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

function exactGrantBody(value){
  if(!value||typeof value!=='object'||Array.isArray(value))return null;
  const keys=Object.keys(value);
  if(keys.length!==1||keys[0]!=='media_id')return null;
  if(!validMediaId(value.media_id))return null;
  return {media_id:value.media_id};
}

export async function issueMemberPrivateMediaDeliveryGrant(
  env,
  session,
  mediaId,
  {now_seconds}={}
){
  if(!validSession(session)){
    return {status:'invalid_member_session',issued:false};
  }
  if(!validMediaId(mediaId)){
    return {status:'invalid_media_id',issued:false};
  }

  const secret=deliverySecret(env);
  if(!secret){
    return {status:'private_media_delivery_secret_not_configured',issued:false};
  }

  const authorization=await authorizeMemberPrivateMediaAccess(env,session,mediaId);
  if(authorization.status!=='ok'||authorization.authorized!==true){
    return {
      status:authorization.status||'private_media_not_authorized',
      issued:false,
      review_required:authorization.review_required===true
    };
  }

  const iat=nowSeconds(now_seconds);
  const exp=iat+TOKEN_TTL_SECONDS;
  const signature=b64url(
    await hmac(signingInput(session,mediaId,iat,exp),secret)
  );
  const grant=`${TOKEN_VERSION}.${iat}.${exp}.${signature}`;

  return {
    status:'ok',
    issued:true,
    media:{
      media_id:authorization.media.media_id,
      memory_id:authorization.media.memory_id,
      media_type:authorization.media.media_type,
      role:authorization.media.role,
      width:authorization.media.width,
      height:authorization.media.height,
      storage_key_exposed:false
    },
    grant,
    expires_at:exp,
    expires_in_seconds:TOKEN_TTL_SECONDS,
    delivery_contract:{
      method:'POST',
      path:'/api/internal/member/media/content',
      body_keys:['media_id','grant'],
      binary_response_expected:true,
      production_route_wired:false
    },
    read_only:true
  };
}

export async function verifyMemberPrivateMediaDeliveryGrant(
  env,
  session,
  mediaId,
  grant,
  {now_seconds}={}
){
  if(!validSession(session)){
    return {status:'invalid_member_session',verified:false};
  }
  if(!validMediaId(mediaId)){
    return {status:'invalid_media_id',verified:false};
  }

  const secret=deliverySecret(env);
  if(!secret){
    return {status:'private_media_delivery_secret_not_configured',verified:false};
  }

  const parsed=parseGrant(grant);
  if(!parsed){
    return {status:'invalid_private_media_grant',verified:false};
  }

  const now=nowSeconds(now_seconds);
  if(parsed.iat>now+CLOCK_SKEW_SECONDS){
    return {status:'private_media_grant_not_yet_valid',verified:false};
  }
  if(parsed.exp<=now-CLOCK_SKEW_SECONDS){
    return {status:'private_media_grant_expired',verified:false};
  }

  const expected=await hmac(
    signingInput(session,mediaId,parsed.iat,parsed.exp),
    secret
  );
  const supplied=fromB64url(parsed.signature);
  if(!safeEqual(supplied,expected)){
    return {status:'invalid_private_media_grant_signature',verified:false};
  }

  const authorization=await authorizeMemberPrivateMediaAccess(env,session,mediaId);
  if(authorization.status!=='ok'||authorization.authorized!==true){
    return {
      status:authorization.status||'private_media_not_authorized',
      verified:false,
      review_required:authorization.review_required===true
    };
  }

  return {
    status:'ok',
    verified:true,
    media:authorization.media,
    read_only:true
  };
}

export async function handleMemberPrivateMediaGrantRequest(request,env){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member/media/grant')return null;
  if(request.method!=='POST')return json({ok:false,error:'method_not_allowed'},405);
  if(!sameOrigin(request))return json({ok:false,error:'same_origin_required'},403);

  const contentType=text(request.headers.get('content-type')).toLowerCase();
  if(!(contentType==='application/json'||contentType.startsWith('application/json;'))){
    return json({ok:false,error:'application_json_required'},415);
  }

  const verified=await verifyMemberSessionRequest(request,env);
  if(verified.status!=='ok'||verified.verified!==true||!verified.session){
    return json({ok:false,error:'member_session_required'},401);
  }

  let parsed;
  try{
    parsed=await request.json();
  }catch{
    return json({ok:false,error:'invalid_json_body'},400);
  }

  const body=exactGrantBody(parsed);
  if(!body)return json({ok:false,error:'invalid_media_id'},400);

  const result=await issueMemberPrivateMediaDeliveryGrant(
    env,
    verified.session,
    body.media_id
  );

  if(result.status==='ok'){
    return json({
      ok:true,
      media:result.media,
      grant:result.grant,
      expires_at:result.expires_at,
      expires_in_seconds:result.expires_in_seconds,
      delivery_contract:result.delivery_contract
    });
  }

  if(result.status==='media_not_found')return json({ok:false,error:'media_not_found'},404);
  if(result.status==='family_access_denied'||result.status==='unlinked'||result.status==='family_inactive_or_missing'){
    return json({ok:false,error:result.status},403);
  }
  if(result.status==='invalid_private_storage_key'){
    return json({ok:false,error:'private_media_unavailable',review_required:true},409);
  }
  if(result.status==='member_memory_schema_not_applied'||result.status==='schema_not_applied'){
    return json({ok:false,error:result.status},409);
  }
  if(result.status==='private_media_delivery_secret_not_configured'){
    return json({ok:false,error:'private_media_delivery_unavailable'},503);
  }

  return json({ok:false,error:result.status||'private_media_delivery_unavailable'},409);
}

export function memberPrivateMediaDeliveryPublicContract(){
  return {
    source_contract_ready:true,
    delivery_ready:false,
    grant:{
      method:'POST',
      path:'/api/internal/member/media/grant',
      body_keys:['media_id'],
      signed_member_session_required:true,
      same_origin_required:true,
      ttl_seconds:TOKEN_TTL_SECONDS,
      production_route_wired:false
    },
    content:{
      method:'POST',
      path:'/api/internal/member/media/content',
      body_keys:['media_id','grant'],
      signed_member_session_required:true,
      same_origin_required:true,
      grant_required:true,
      production_route_wired:false,
      production_storage_binding:false,
      production_storage_fetch:false
    },
    storage_key_exposed:false,
    signed_storage_url_exposed:false,
    external_redirect:false
  };
}

export function memberPrivateMediaDeliveryGrantHealth(env){
  return {
    member_private_media_delivery_grant:true,
    build:BUILD,
    configured:!!deliverySecret(env),
    secret_min_length:MIN_SECRET_LENGTH,
    token_version:TOKEN_VERSION,
    token_ttl_seconds:TOKEN_TTL_SECONDS,
    clock_skew_seconds:CLOCK_SKEW_SECONDS,
    token_contains_customer_id:false,
    token_contains_family_id:false,
    token_contains_storage_key:false,
    signed_binding_inputs:[
      'family_id',
      'customer_id',
      'media_id',
      'issued_at',
      'expires_at'
    ],
    same_origin_grant_issue_required:true,
    signed_member_session_cookie_required:true,
    private_media_reauthorization_on_issue:true,
    private_media_reauthorization_on_verify:true,
    content_delivery_method:'POST',
    grant_in_url:false,
    storage_key_public_response:false,
    binary_route_implemented:false,
    production_route_wired:false,
    production_storage_fetch:false,
    production_write:false,
    public_contract_exported:true
  };
}

export const __test={
  deliverySecret,
  validMediaId,
  signingInput,
  parseGrant,
  sameOrigin,
  exactGrantBody,
  TOKEN_TTL_SECONDS,
  CLOCK_SKEW_SECONDS,
  MIN_SECRET_LENGTH
};
