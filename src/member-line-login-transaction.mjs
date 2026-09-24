const BUILD='member-line-login-transaction-20260924-01';
const AUTHORIZATION_ENDPOINT='https://access.line.me/oauth2/v2.1/authorize';
const COOKIE_NAME='__Host-mizuno_member_login_tx';
const TX_MAX_AGE_SECONDS=60*10;
const CLOCK_SKEW_SECONDS=30;
const MIN_SECRET_LENGTH=32;
const MAX_COOKIE_TOKEN=3800;
const MAX_REDIRECT_URI=512;
const MAX_RETURN_TO=256;
const CHANNEL_ID_RE=/^\d{5,32}$/;
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

function safeEqualBytes(a,b){
  if(a.length!==b.length)return false;
  let diff=0;
  for(let i=0;i<a.length;i++)diff|=a[i]^b[i];
  return diff===0;
}

function safeEqualText(a,b){
  return safeEqualBytes(encoder.encode(String(a||'')),encoder.encode(String(b||'')));
}

async function sha256Bytes(value){
  return new Uint8Array(await crypto.subtle.digest('SHA-256',encoder.encode(String(value))));
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

function randomToken(bytes=32){
  const out=new Uint8Array(bytes);
  crypto.getRandomValues(out);
  return b64url(out);
}

function nowSeconds(nowOverride){
  if(Number.isFinite(Number(nowOverride)))return Math.floor(Number(nowOverride));
  return Math.floor(Date.now()/1000);
}

function config(env){
  const channelId=text(env?.MEMBER_LINE_LOGIN_CHANNEL_ID);
  const redirectUri=text(env?.MEMBER_LINE_LOGIN_REDIRECT_URI);
  const secret=text(env?.MEMBER_LINE_LOGIN_TRANSACTION_SECRET);

  if(!CHANNEL_ID_RE.test(channelId)){
    return {ok:false,error:'line_login_channel_id_not_configured'};
  }
  if(secret.length<MIN_SECRET_LENGTH){
    return {ok:false,error:'line_login_transaction_secret_not_configured'};
  }
  if(!redirectUri||redirectUri.length>MAX_REDIRECT_URI){
    return {ok:false,error:'line_login_redirect_uri_not_configured'};
  }

  try{
    const u=new URL(redirectUri);
    if(u.protocol!=='https:'||u.username||u.password||u.hash){
      return {ok:false,error:'line_login_redirect_uri_invalid'};
    }
  }catch{
    return {ok:false,error:'line_login_redirect_uri_invalid'};
  }

  return {ok:true,channel_id:channelId,redirect_uri:redirectUri,secret};
}

function validReturnTo(value){
  const v=text(value);
  if(!v)return '/member';
  if(v.length>MAX_RETURN_TO)return '';
  if(!v.startsWith('/')||v.startsWith('//'))return '';
  if(/[\u0000-\u001f\u007f]/.test(v))return '';
  return v;
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

function transactionCookie(token){
  return `${COOKIE_NAME}=${token}; Path=/; Max-Age=${TX_MAX_AGE_SECONDS}; HttpOnly; Secure; SameSite=Lax`;
}

function clearTransactionCookie(){
  return `${COOKIE_NAME}=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Lax`;
}

async function signPayload(payload,secret){
  const encoded=b64url(encoder.encode(JSON.stringify(payload)));
  const sig=b64url(await hmac(encoded,secret));
  return `${encoded}.${sig}`;
}

async function verifySignedPayload(token,secret,now){
  if(!token||token.length>MAX_COOKIE_TOKEN){
    return {ok:false,error:'line_login_transaction_missing'};
  }
  const parts=token.split('.');
  if(parts.length!==2||!parts[0]||!parts[1]){
    return {ok:false,error:'line_login_transaction_invalid'};
  }

  const [payload,sig]=parts;
  const expected=await hmac(payload,secret);
  if(!safeEqualBytes(fromB64url(sig),expected)){
    return {ok:false,error:'line_login_transaction_signature_invalid'};
  }

  let data;
  try{
    data=JSON.parse(decoder.decode(fromB64url(payload)));
  }catch{
    return {ok:false,error:'line_login_transaction_invalid'};
  }

  if(
    data?.v!==1
    || data?.purpose!=='line_login'
    || !CHANNEL_ID_RE.test(text(data?.client_id))
    || !text(data?.redirect_uri)
    || !text(data?.state)
    || !text(data?.nonce)
    || !text(data?.code_verifier)
  ){
    return {ok:false,error:'line_login_transaction_invalid'};
  }

  const iat=Number(data.iat);
  const exp=Number(data.exp);
  if(!Number.isInteger(iat)||!Number.isInteger(exp)||exp<=iat||exp-iat>TX_MAX_AGE_SECONDS){
    return {ok:false,error:'line_login_transaction_invalid'};
  }
  if(iat>now+CLOCK_SKEW_SECONDS){
    return {ok:false,error:'line_login_transaction_not_yet_valid'};
  }
  if(exp<=now-CLOCK_SKEW_SECONDS){
    return {ok:false,error:'line_login_transaction_expired'};
  }

  const returnTo=validReturnTo(data.return_to);
  if(!returnTo){
    return {ok:false,error:'line_login_transaction_invalid'};
  }

  return {
    ok:true,
    transaction:{
      state:text(data.state),
      nonce:text(data.nonce),
      code_verifier:text(data.code_verifier),
      client_id:text(data.client_id),
      redirect_uri:text(data.redirect_uri),
      return_to:returnTo,
      issued_at:iat,
      expires_at:exp
    }
  };
}

export async function createMemberLineLoginTransaction(env,{
  return_to='/member',
  now_seconds
}={}){
  const cfg=config(env);
  if(!cfg.ok)return {status:cfg.error,created:false};

  const returnTo=validReturnTo(return_to);
  if(!returnTo)return {status:'invalid_return_to',created:false};

  const state=randomToken(32);
  const nonce=randomToken(32);
  const codeVerifier=randomToken(32);
  const codeChallenge=b64url(await sha256Bytes(codeVerifier));
  const iat=nowSeconds(now_seconds);

  const tx={
    v:1,
    purpose:'line_login',
    client_id:cfg.channel_id,
    redirect_uri:cfg.redirect_uri,
    state,
    nonce,
    code_verifier:codeVerifier,
    return_to:returnTo,
    iat,
    exp:iat+TX_MAX_AGE_SECONDS
  };

  const signed=await signPayload(tx,cfg.secret);
  if(signed.length>MAX_COOKIE_TOKEN){
    return {status:'line_login_transaction_too_large',created:false};
  }

  const url=new URL(AUTHORIZATION_ENDPOINT);
  url.searchParams.set('response_type','code');
  url.searchParams.set('client_id',cfg.channel_id);
  url.searchParams.set('redirect_uri',cfg.redirect_uri);
  url.searchParams.set('state',state);
  url.searchParams.set('scope','openid');
  url.searchParams.set('nonce',nonce);
  url.searchParams.set('code_challenge',codeChallenge);
  url.searchParams.set('code_challenge_method','S256');

  return {
    status:'ok',
    created:true,
    authorization_url:url.toString(),
    cookie:transactionCookie(signed),
    transaction:{
      state,
      nonce,
      code_challenge:codeChallenge,
      code_challenge_method:'S256',
      return_to:returnTo,
      expires_at:iat+TX_MAX_AGE_SECONDS
    },
    session_issued:false,
    production_write:false
  };
}

export async function verifyMemberLineLoginCallback(request,env,{now_seconds}={}){
  const cfg=config(env);
  if(!cfg.ok)return {status:cfg.error,verified:false};

  const url=new URL(request.url);
  const lineError=text(url.searchParams.get('error'));
  if(lineError){
    return {
      status:'line_authorization_denied',
      verified:false,
      clear_cookie:clearTransactionCookie()
    };
  }

  const code=text(url.searchParams.get('code'));
  const state=text(url.searchParams.get('state'));
  if(!code||!state){
    return {
      status:'line_login_callback_invalid',
      verified:false,
      clear_cookie:clearTransactionCookie()
    };
  }

  const token=cookieValue(request,COOKIE_NAME);
  const verified=await verifySignedPayload(token,cfg.secret,nowSeconds(now_seconds));
  if(!verified.ok){
    return {
      status:verified.error,
      verified:false,
      clear_cookie:clearTransactionCookie()
    };
  }

  const tx=verified.transaction;
  if(!safeEqualText(state,tx.state)){
    return {
      status:'line_login_state_mismatch',
      verified:false,
      clear_cookie:clearTransactionCookie()
    };
  }
  if(tx.client_id!==cfg.channel_id||tx.redirect_uri!==cfg.redirect_uri){
    return {
      status:'line_login_transaction_config_mismatch',
      verified:false,
      clear_cookie:clearTransactionCookie()
    };
  }

  return {
    status:'ok',
    verified:true,
    authorization_code:code,
    nonce:tx.nonce,
    code_verifier:tx.code_verifier,
    client_id:tx.client_id,
    redirect_uri:tx.redirect_uri,
    return_to:tx.return_to,
    clear_cookie:clearTransactionCookie(),
    token_exchange_executed:false,
    id_token_verified:false,
    member_session_issued:false,
    production_write:false
  };
}

export function memberLineLoginTransactionHealth(env){
  const cfg=config(env);
  return {
    member_line_login_transaction:true,
    build:BUILD,
    configured:cfg.ok===true,
    authorization_endpoint:AUTHORIZATION_ENDPOINT,
    oauth_version:'2.1',
    response_type:'code',
    scope:'openid',
    state_required:true,
    nonce_required:true,
    pkce_required:true,
    pkce_method:'S256',
    transaction_cookie:COOKIE_NAME,
    cookie_http_only:true,
    cookie_secure:true,
    cookie_same_site:'Lax',
    cookie_host_prefix:true,
    transaction_max_age_seconds:TX_MAX_AGE_SECONDS,
    local_return_to_only:true,
    profile_scope_requested:false,
    email_scope_requested:false,
    token_exchange_executed:false,
    id_token_verification_executed:false,
    member_session_issued:false,
    production_route_wired:false,
    line_login_activated:false,
    production_write:false
  };
}

export const memberLineLoginTransactionCookie={
  name:COOKIE_NAME,
  max_age_seconds:TX_MAX_AGE_SECONDS,
  clear:clearTransactionCookie
};

export const __test={
  AUTHORIZATION_ENDPOINT,
  COOKIE_NAME,
  TX_MAX_AGE_SECONDS,
  MIN_SECRET_LENGTH,
  validReturnTo,
  config,
  verifySignedPayload,
  safeEqualText
};
