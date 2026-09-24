import { readMemberFamilyByCustomer } from './crm-member-family-identity.mjs';

const BUILD='member-session-foundation-20260924-01';
const COOKIE_NAME='__Host-mizuno_member_session';
const SUBJECT='member';
const AUDIENCE='mizuno-photo-member';
const SESSION_MAX_AGE_SECONDS=60*60*24*7;
const CLOCK_SKEW_SECONDS=60;
const MIN_SECRET_LENGTH=32;
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const encoder=new TextEncoder();
const decoder=new TextDecoder();

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

function sessionSecret(env){
  const secret=text(env?.MEMBER_SESSION_SECRET);
  return secret.length>=MIN_SECRET_LENGTH?secret:'';
}

function validFamilyId(value){
  const id=text(value);
  return !!id&&id.length<=MAX_FAMILY_ID&&!/[\u0000-\u001f\u007f]/.test(id);
}

function nowSeconds(nowSecondsOverride){
  if(Number.isFinite(Number(nowSecondsOverride)))return Math.floor(Number(nowSecondsOverride));
  return Math.floor(Date.now()/1000);
}

function cookieValue(request,name){
  const raw=request?.headers?.get?.('cookie')||'';
  for(const part of raw.split(';')){
    const i=part.indexOf('=');
    if(i<0)continue;
    if(part.slice(0,i).trim()===name)return part.slice(i+1).trim();
  }
  return '';
}

function sessionCookie(token){
  return `${COOKIE_NAME}=${token}; Path=/; Max-Age=${SESSION_MAX_AGE_SECONDS}; HttpOnly; Secure; SameSite=Lax`;
}

function clearSessionCookie(){
  return `${COOKIE_NAME}=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax`;
}

function validateClaims(data,now){
  if(!data||typeof data!=='object')return {ok:false,error:'invalid_member_session'};
  if(data.v!==1||data.sub!==SUBJECT||data.aud!==AUDIENCE)return {ok:false,error:'invalid_member_session'};

  const customerId=text(data.customer_id);
  const familyId=text(data.family_id);
  const iat=Number(data.iat);
  const exp=Number(data.exp);

  if(!CUSTOMER_ID_RE.test(customerId)||!validFamilyId(familyId)){
    return {ok:false,error:'invalid_member_session'};
  }
  if(!Number.isInteger(iat)||!Number.isInteger(exp)||exp<=iat){
    return {ok:false,error:'invalid_member_session'};
  }
  if(iat>now+CLOCK_SKEW_SECONDS){
    return {ok:false,error:'member_session_not_yet_valid'};
  }
  if(exp<=now-CLOCK_SKEW_SECONDS){
    return {ok:false,error:'member_session_expired'};
  }
  if(exp-iat>SESSION_MAX_AGE_SECONDS){
    return {ok:false,error:'invalid_member_session'};
  }

  return {
    ok:true,
    session:{
      customer_id:customerId,
      family_id:familyId,
      issued_at:iat,
      expires_at:exp,
      verified:true,
      source:'signed_member_session'
    }
  };
}

export async function issueMemberSessionForVerifiedCustomer(env,{
  customer_id,
  family_id,
  now_seconds
}={}){
  const secret=sessionSecret(env);
  if(!secret)return {status:'member_session_secret_not_configured',issued:false};

  const customerId=text(customer_id);
  const familyId=text(family_id);
  if(!CUSTOMER_ID_RE.test(customerId))return {status:'invalid_customer_id',issued:false};
  if(!validFamilyId(familyId))return {status:'invalid_family_id',issued:false};

  const family=await readMemberFamilyByCustomer(env,customerId);
  if(family.status!=='linked')return {status:family.status,issued:false};
  if(text(family.family?.family_id)!==familyId){
    return {status:'family_access_denied',issued:false};
  }

  const iat=nowSeconds(now_seconds);
  const payloadBytes=encoder.encode(JSON.stringify({
    v:1,
    sub:SUBJECT,
    aud:AUDIENCE,
    customer_id:customerId,
    family_id:familyId,
    iat,
    exp:iat+SESSION_MAX_AGE_SECONDS
  }));
  const payload=b64url(payloadBytes);
  const signature=b64url(await hmac(payload,secret));
  const token=`${payload}.${signature}`;

  return {
    status:'ok',
    issued:true,
    token,
    cookie:sessionCookie(token),
    session:{
      customer_id:customerId,
      family_id:familyId,
      issued_at:iat,
      expires_at:iat+SESSION_MAX_AGE_SECONDS,
      verified:true,
      source:'signed_member_session'
    },
    write_executed:false
  };
}

export async function verifyMemberSessionToken(env,token,{now_seconds}={}){
  const secret=sessionSecret(env);
  if(!secret)return {status:'member_session_secret_not_configured',verified:false};

  const parts=text(token).split('.');
  if(parts.length!==2||!parts[0]||!parts[1]){
    return {status:'invalid_member_session',verified:false};
  }

  const [payload,signature]=parts;
  const expected=await hmac(payload,secret);
  const supplied=fromB64url(signature);
  if(!safeEqual(supplied,expected)){
    return {status:'invalid_member_session_signature',verified:false};
  }

  let data;
  try{
    data=JSON.parse(decoder.decode(fromB64url(payload)));
  }catch{
    return {status:'invalid_member_session',verified:false};
  }

  const checked=validateClaims(data,nowSeconds(now_seconds));
  if(!checked.ok)return {status:checked.error,verified:false};

  return {
    status:'ok',
    verified:true,
    session:checked.session,
    write_executed:false
  };
}

export async function verifyMemberSessionRequest(request,env,options={}){
  const token=cookieValue(request,COOKIE_NAME);
  if(!token)return {status:'member_session_required',verified:false};
  return verifyMemberSessionToken(env,token,options);
}

export function memberSessionFoundationHealth(env){
  return {
    member_session_foundation:true,
    build:BUILD,
    configured:!!sessionSecret(env),
    cookie_name:COOKIE_NAME,
    cookie_http_only:true,
    cookie_secure:true,
    cookie_same_site:'Lax',
    cookie_host_prefix:true,
    max_age_seconds:SESSION_MAX_AGE_SECONDS,
    signature:'HMAC-SHA256',
    secret_min_length:MIN_SECRET_LENGTH,
    subject:SUBJECT,
    audience:AUDIENCE,
    exact_customer_id_only:true,
    explicit_family_link_required_at_issue:true,
    names_in_token:false,
    email_in_token:false,
    phone_in_token:false,
    line_user_id_in_token:false,
    production_route_wired:false,
    line_login_activated:false,
    production_write:false
  };
}

export const memberSessionCookie={
  name:COOKIE_NAME,
  max_age_seconds:SESSION_MAX_AGE_SECONDS,
  clear:clearSessionCookie
};

export const __test={
  COOKIE_NAME,
  SESSION_MAX_AGE_SECONDS,
  MIN_SECRET_LENGTH,
  validFamilyId,
  validateClaims,
  sessionCookie,
  clearSessionCookie
};
