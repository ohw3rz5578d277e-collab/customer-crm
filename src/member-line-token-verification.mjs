import { isFormalLineUserId } from './customer-identity-resolver.mjs';

const BUILD='member-line-token-verification-20260924-01';
const TOKEN_ENDPOINT='https://api.line.me/oauth2/v2.1/token';
const VERIFY_ENDPOINT='https://api.line.me/oauth2/v2.1/verify';
const ISSUER='https://access.line.me';
const CLOCK_SKEW_SECONDS=60;
const MAX_AUTH_CODE=2048;
const MAX_CODE_VERIFIER=256;
const MAX_ID_TOKEN=16384;
const CHANNEL_ID_RE=/^\d{5,32}$/;

const text=v=>v==null?'':String(v).trim();

function validHttpsUrl(value){
  try{
    const u=new URL(text(value));
    return u.protocol==='https:'&&!u.username&&!u.password&&!u.hash;
  }catch{
    return false;
  }
}

function validCodeVerifier(value){
  const v=text(value);
  return v.length>=43&&v.length<=128&&/^[A-Za-z0-9\-._~]+$/.test(v);
}

function validAuthorizationCode(value){
  const v=text(value);
  return !!v&&v.length<=MAX_AUTH_CODE&&!/[\u0000-\u001f\u007f]/.test(v);
}

function validIdToken(value){
  const v=text(value);
  return !!v&&v.length<=MAX_ID_TOKEN&&!/[\u0000-\u001f\u007f\s]/.test(v);
}

function nowSeconds(nowOverride){
  if(Number.isFinite(Number(nowOverride)))return Math.floor(Number(nowOverride));
  return Math.floor(Date.now()/1000);
}

export function buildMemberLineTokenExchangeRequest({
  authorization_code,
  redirect_uri,
  client_id,
  client_secret,
  code_verifier
}={}){
  const code=text(authorization_code);
  const redirectUri=text(redirect_uri);
  const clientId=text(client_id);
  const clientSecret=text(client_secret);
  const verifier=text(code_verifier);

  if(!validAuthorizationCode(code))return {status:'invalid_authorization_code',request:null};
  if(!validHttpsUrl(redirectUri))return {status:'invalid_redirect_uri',request:null};
  if(!CHANNEL_ID_RE.test(clientId))return {status:'invalid_client_id',request:null};
  if(!clientSecret)return {status:'client_secret_required',request:null};
  if(!validCodeVerifier(verifier))return {status:'invalid_code_verifier',request:null};

  const body=new URLSearchParams();
  body.set('grant_type','authorization_code');
  body.set('code',code);
  body.set('redirect_uri',redirectUri);
  body.set('client_id',clientId);
  body.set('client_secret',clientSecret);
  body.set('code_verifier',verifier);

  return {
    status:'ok',
    request:new Request(TOKEN_ENDPOINT,{
      method:'POST',
      headers:{
        'content-type':'application/x-www-form-urlencoded',
        'accept':'application/json'
      },
      body:body.toString()
    }),
    external_call_executed:false,
    secret_logged:false
  };
}

export function validateMemberLineTokenExchangeResponse(payload){
  if(!payload||typeof payload!=='object'||Array.isArray(payload)){
    return {status:'line_token_response_invalid',ok:false};
  }

  const tokenType=text(payload.token_type);
  const idToken=text(payload.id_token);
  const accessToken=text(payload.access_token);
  const refreshToken=text(payload.refresh_token);
  const expiresIn=Number(payload.expires_in);
  const scope=text(payload.scope).split(/\s+/).filter(Boolean);

  if(tokenType!=='Bearer'){
    return {status:'line_token_type_invalid',ok:false};
  }
  if(!accessToken){
    return {status:'line_access_token_missing',ok:false};
  }
  if(!validIdToken(idToken)){
    return {status:'line_id_token_missing_or_invalid',ok:false};
  }
  if(!Number.isFinite(expiresIn)||expiresIn<=0){
    return {status:'line_token_expiry_invalid',ok:false};
  }
  if(!scope.includes('openid')){
    return {status:'line_openid_scope_missing',ok:false};
  }

  return {
    status:'ok',
    ok:true,
    id_token:idToken,
    token_type:'Bearer',
    expires_in:expiresIn,
    scope,
    access_token_present:true,
    refresh_token_present:!!refreshToken,
    access_token_exposed:false,
    refresh_token_exposed:false
  };
}

export function buildMemberLineIdTokenVerificationRequest({
  id_token,
  client_id
}={}){
  const idToken=text(id_token);
  const clientId=text(client_id);

  if(!validIdToken(idToken))return {status:'invalid_id_token',request:null};
  if(!CHANNEL_ID_RE.test(clientId))return {status:'invalid_client_id',request:null};

  const body=new URLSearchParams();
  body.set('id_token',idToken);
  body.set('client_id',clientId);

  return {
    status:'ok',
    request:new Request(VERIFY_ENDPOINT,{
      method:'POST',
      headers:{
        'content-type':'application/x-www-form-urlencoded',
        'accept':'application/json'
      },
      body:body.toString()
    }),
    external_call_executed:false
  };
}

export function validateVerifiedMemberLineIdTokenPayload(payload,{
  client_id,
  nonce,
  now_seconds
}={}){
  if(!payload||typeof payload!=='object'||Array.isArray(payload)){
    return {status:'line_id_token_verification_response_invalid',verified:false};
  }

  const expectedClientId=text(client_id);
  const expectedNonce=text(nonce);
  if(!CHANNEL_ID_RE.test(expectedClientId)){
    return {status:'invalid_client_id',verified:false};
  }
  if(!expectedNonce){
    return {status:'nonce_required',verified:false};
  }

  const iss=text(payload.iss);
  const sub=text(payload.sub);
  const aud=text(payload.aud);
  const actualNonce=text(payload.nonce);
  const exp=Number(payload.exp);
  const iat=Number(payload.iat);
  const now=nowSeconds(now_seconds);

  if(iss!==ISSUER){
    return {status:'line_id_token_issuer_mismatch',verified:false};
  }
  if(aud!==expectedClientId){
    return {status:'line_id_token_audience_mismatch',verified:false};
  }
  if(actualNonce!==expectedNonce){
    return {status:'line_id_token_nonce_mismatch',verified:false};
  }
  if(!Number.isInteger(exp)||!Number.isInteger(iat)||exp<=iat){
    return {status:'line_id_token_time_claims_invalid',verified:false};
  }
  if(iat>now+CLOCK_SKEW_SECONDS){
    return {status:'line_id_token_not_yet_valid',verified:false};
  }
  if(exp<=now-CLOCK_SKEW_SECONDS){
    return {status:'line_id_token_expired',verified:false};
  }
  if(!isFormalLineUserId(sub)){
    return {status:'line_id_token_subject_invalid',verified:false};
  }

  return {
    status:'ok',
    verified:true,
    verified_line_user_id:sub,
    identity_source:'verified_line_id_token_sub',
    issuer:ISSUER,
    audience:expectedClientId,
    profile_claims_used:false,
    email_claim_used:false,
    access_token_used_as_identity:false,
    production_write:false
  };
}

export function memberLineTokenVerificationHealth(){
  return {
    member_line_token_verification:true,
    build:BUILD,
    token_endpoint:TOKEN_ENDPOINT,
    id_token_verify_endpoint:VERIFY_ENDPOINT,
    expected_issuer:ISSUER,
    token_exchange_request_builder:true,
    token_exchange_external_call:false,
    pkce_code_verifier_required:true,
    id_token_response_required:true,
    openid_scope_required:true,
    id_token_verification_request_builder:true,
    id_token_verification_external_call:false,
    issuer_checked:true,
    audience_checked:true,
    nonce_checked:true,
    expiry_checked:true,
    issued_at_checked:true,
    formal_line_subject_required:true,
    verified_subject_only_identity:true,
    profile_claims_used_for_identity:false,
    email_claim_used_for_identity:false,
    access_token_used_for_identity:false,
    member_session_issued:false,
    production_route_wired:false,
    line_login_activated:false,
    production_write:false
  };
}

export const __test={
  TOKEN_ENDPOINT,
  VERIFY_ENDPOINT,
  ISSUER,
  CLOCK_SKEW_SECONDS,
  validHttpsUrl,
  validCodeVerifier,
  validAuthorizationCode,
  validIdToken
};
