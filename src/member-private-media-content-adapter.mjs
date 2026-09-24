import {
  verifyMemberPrivateMediaDeliveryGrant
} from './member-private-media-delivery-grant.mjs';
import {
  verifyMemberSessionRequest
} from './member-session-foundation.mjs';

const BUILD='member-private-media-content-adapter-20260925-01';
const ROUTE_MODE='enabled';
const MAX_BODY_BYTES=4096;
const MAX_MEDIA_ID=160;
const MAX_GRANT_LENGTH=512;
const MAX_PRIVATE_MEDIA_BYTES=50*1024*1024;
const IMAGE_MIMES=new Set([
  'image/jpeg',
  'image/png',
  'image/webp'
]);
const encoder=new TextEncoder();

const text=v=>v==null?'':String(v).trim();

function routeEnabled(env){
  return text(env?.MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE).toLowerCase()===ROUTE_MODE;
}

function json(data,status=200,headers={}){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-content-type-options':'nosniff',
      'referrer-policy':'no-referrer',
      'x-robots-tag':'noindex, nofollow',
      'x-member-private-media-content-build':BUILD,
      ...headers
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

function validMediaId(mediaId){
  const raw=mediaId==null?'':String(mediaId);
  if(!raw||raw.length>MAX_MEDIA_ID)return false;
  if(/[\u0000-\u001f\u007f]/.test(raw))return false;
  if(raw!==raw.trim())return false;
  return true;
}

function validGrant(grant){
  const raw=grant==null?'':String(grant);
  if(!raw||raw.length>MAX_GRANT_LENGTH)return false;
  if(/[\u0000-\u001f\u007f]/.test(raw))return false;
  if(raw!==raw.trim())return false;
  return /^v1\.\d{1,12}\.\d{1,12}\.[A-Za-z0-9_-]{20,256}$/.test(raw);
}

function exactContentBody(value){
  if(!value||typeof value!=='object'||Array.isArray(value))return null;
  const keys=Object.keys(value).sort();
  if(keys.length!==2||keys[0]!=='grant'||keys[1]!=='media_id')return null;
  if(!validMediaId(value.media_id)||!validGrant(value.grant))return null;
  return {
    media_id:value.media_id,
    grant:value.grant
  };
}

async function readBody(request){
  const header=text(request?.headers?.get?.('content-length'));
  if(header){
    const declared=Number(header);
    if(!Number.isInteger(declared)||declared<0){
      return {ok:false,error:'invalid_content_length'};
    }
    if(declared>MAX_BODY_BYTES){
      return {ok:false,error:'request_body_too_large'};
    }
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

  const body=exactContentBody(parsed);
  if(!body)return {ok:false,error:'invalid_content_body'};
  return {ok:true,body};
}

function validStorageAdapter(adapter){
  return !!adapter&&typeof adapter.get==='function';
}

function validBody(body){
  if(body instanceof ArrayBuffer)return body.byteLength>0;
  if(ArrayBuffer.isView(body))return body.byteLength>0;
  if(typeof Blob!=='undefined'&&body instanceof Blob)return body.size>0;
  if(typeof ReadableStream!=='undefined'&&body instanceof ReadableStream)return true;
  return false;
}

function normalizeStorageObject(object){
  if(!object||typeof object!=='object')return null;

  const contentType=text(object.content_type).toLowerCase();
  const size=Number(object.size);

  if(!IMAGE_MIMES.has(contentType))return null;
  if(!Number.isInteger(size)||size<1||size>MAX_PRIVATE_MEDIA_BYTES)return null;
  if(!validBody(object.body))return null;

  const etag=text(object.etag);
  if(etag&&(
    etag.length>160
    || /[\u0000-\u001f\u007f]/.test(etag)
  ))return null;

  return {
    body:object.body,
    content_type:contentType,
    size,
    etag:etag||null
  };
}

function binaryResponse(object){
  const headers={
    'content-type':object.content_type,
    'content-length':String(object.size),
    'cache-control':'private, no-store, max-age=0',
    'pragma':'no-cache',
    'x-content-type-options':'nosniff',
    'referrer-policy':'no-referrer',
    'content-security-policy':"default-src 'none'; sandbox",
    'cross-origin-resource-policy':'same-origin',
    'accept-ranges':'none',
    'x-member-private-media-content-build':BUILD
  };

  if(object.etag){
    headers.etag=`"${object.etag.replace(/^"+|"+$/g,'')}"`;
  }

  return new Response(object.body,{
    status:200,
    headers
  });
}

export async function readAuthorizedMemberPrivateMediaContent(
  env,
  session,
  {
    media_id,
    grant
  }={},
  storageAdapter,
  {now_seconds}={}
){
  if(!validStorageAdapter(storageAdapter)){
    return {
      status:'private_media_storage_adapter_unavailable',
      delivered:false,
      storage_fetch_executed:false
    };
  }

  const verified=await verifyMemberPrivateMediaDeliveryGrant(
    env,
    session,
    media_id,
    grant,
    {now_seconds}
  );

  if(verified.status!=='ok'||verified.verified!==true){
    return {
      status:verified.status||'private_media_grant_invalid',
      delivered:false,
      storage_fetch_executed:false,
      review_required:verified.review_required===true
    };
  }

  if(text(verified.media?.media_type)!=='image'){
    return {
      status:'private_media_type_not_supported',
      delivered:false,
      storage_fetch_executed:false
    };
  }

  let object;
  try{
    object=await storageAdapter.get(verified.media.storage_key);
  }catch(error){
    return {
      status:'private_media_storage_fetch_failed',
      delivered:false,
      storage_fetch_executed:true,
      error_class:text(error?.name)||'Error',
      review_required:true
    };
  }

  if(!object){
    return {
      status:'private_media_content_not_found',
      delivered:false,
      storage_fetch_executed:true
    };
  }

  const normalized=normalizeStorageObject(object);
  if(!normalized){
    return {
      status:'invalid_private_media_storage_object',
      delivered:false,
      storage_fetch_executed:true,
      review_required:true
    };
  }

  return {
    status:'ok',
    delivered:true,
    media_id:verified.media.media_id,
    memory_id:verified.media.memory_id,
    content:normalized,
    storage_fetch_executed:true,
    storage_key_exposed:false,
    read_only:true
  };
}

export async function handleMemberPrivateMediaContentRequest(
  request,
  env,
  storageAdapter
){
  const url=new URL(request.url);
  if(url.pathname!=='/api/internal/member/media/content')return null;

  if(!routeEnabled(env)){
    return json({ok:false,error:'not_found'},404);
  }
  if(request.method!=='POST'){
    return json({ok:false,error:'method_not_allowed'},405);
  }
  if(!sameOrigin(request)){
    return json({ok:false,error:'same_origin_required'},403);
  }

  const contentType=text(request.headers.get('content-type')).toLowerCase();
  if(!(contentType==='application/json'||contentType.startsWith('application/json;'))){
    return json({ok:false,error:'application_json_required'},415);
  }

  if(request.headers.get('range')){
    return json(
      {ok:false,error:'range_not_supported'},
      416,
      {'accept-ranges':'none'}
    );
  }

  const session=await verifyMemberSessionRequest(request,env);
  if(session.status!=='ok'||session.verified!==true||!session.session){
    return json({ok:false,error:'member_session_required'},401);
  }

  const parsed=await readBody(request);
  if(!parsed.ok){
    return json(
      {ok:false,error:parsed.error},
      parsed.error==='request_body_too_large'?413:400
    );
  }

  const result=await readAuthorizedMemberPrivateMediaContent(
    env,
    session.session,
    parsed.body,
    storageAdapter
  );

  if(result.status==='ok'){
    return binaryResponse(result.content);
  }

  if(
    result.status==='invalid_private_media_grant'
    || result.status==='invalid_private_media_grant_signature'
    || result.status==='private_media_grant_expired'
    || result.status==='private_media_grant_not_yet_valid'
  ){
    return json({ok:false,error:'private_media_grant_invalid'},403);
  }

  if(result.status==='media_not_found'||result.status==='private_media_content_not_found'){
    return json({ok:false,error:'private_media_not_found'},404);
  }

  if(
    result.status==='family_access_denied'
    || result.status==='unlinked'
    || result.status==='family_inactive_or_missing'
  ){
    return json({ok:false,error:'private_media_access_denied'},403);
  }

  if(
    result.status==='private_media_storage_adapter_unavailable'
    || result.status==='private_media_delivery_secret_not_configured'
  ){
    return json({ok:false,error:'private_media_delivery_unavailable'},503);
  }

  return json({
    ok:false,
    error:'private_media_delivery_unavailable',
    review_required:result.review_required===true
  },409);
}

export function memberPrivateMediaContentAdapterHealth(env){
  return {
    member_private_media_content_adapter:true,
    build:BUILD,
    route_mode:routeEnabled(env)?'enabled':'disabled',
    route_default_disabled:true,
    conceptual_path:'/api/internal/member/media/content',
    method:'POST',
    same_origin_required:true,
    signed_member_session_required:true,
    grant_required:true,
    grant_reauthorization_required:true,
    trusted_storage_adapter_required:true,
    implicit_env_storage_binding:false,
    supported_media_types:['image'],
    supported_content_types:[...IMAGE_MIMES],
    max_private_media_bytes:MAX_PRIVATE_MEDIA_BYTES,
    range_requests_supported:false,
    private_no_store:true,
    nosniff:true,
    cross_origin_resource_policy:'same-origin',
    storage_key_public_response:false,
    external_url_redirect:false,
    production_storage_binding:false,
    production_route_wired:false,
    production_storage_fetch:false,
    production_write:false
  };
}

export const __test={
  routeEnabled,
  sameOrigin,
  validMediaId,
  validGrant,
  exactContentBody,
  readBody,
  validStorageAdapter,
  validBody,
  normalizeStorageObject,
  binaryResponse,
  MAX_BODY_BYTES,
  MAX_PRIVATE_MEDIA_BYTES
};
