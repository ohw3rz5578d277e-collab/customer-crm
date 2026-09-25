import {
  buildMemberLineTokenExchangeRequest,
  validateMemberLineTokenExchangeResponse,
  buildMemberLineIdTokenVerificationRequest,
  validateVerifiedMemberLineIdTokenPayload
} from './member-line-token-verification.mjs';
import { resolveCanonicalCustomerByVerifiedLineUserId } from './crm-member-line-identity.mjs';
import { readMemberFamilyByCustomer } from './crm-member-family-identity.mjs';
import { issueMemberSessionForVerifiedCustomer } from './member-session-foundation.mjs';

const BUILD='member-line-login-exchange-executor-20260925-01';
const MODE='enabled';
const MAX_RESPONSE_BYTES=32768;

const text=value=>value==null?'':String(value).trim();

function runtimeEnabled(env){
  return text(env?.MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE).toLowerCase()===MODE;
}

function channelSecret(env){
  return text(env?.MEMBER_LINE_LOGIN_CHANNEL_SECRET);
}

function callbackInput(value){
  if(!value||typeof value!=='object'||Array.isArray(value)){
    return {ok:false,error:'verified_callback_required'};
  }
  if(value.status!=='ok'||value.verified!==true){
    return {ok:false,error:'verified_callback_required'};
  }

  const authorizationCode=text(value.authorization_code);
  const nonce=text(value.nonce);
  const codeVerifier=text(value.code_verifier);
  const clientId=text(value.client_id);
  const redirectUri=text(value.redirect_uri);
  const returnTo=text(value.return_to);
  const clearCookie=text(value.clear_cookie);

  if(!authorizationCode||!nonce||!codeVerifier||!clientId||!redirectUri||!returnTo){
    return {ok:false,error:'verified_callback_incomplete'};
  }

  return {
    ok:true,
    authorization_code:authorizationCode,
    nonce,
    code_verifier:codeVerifier,
    client_id:clientId,
    redirect_uri:redirectUri,
    return_to:returnTo,
    clear_cookie:clearCookie
  };
}

function failure(status,stage,extra={}){
  return {
    status,
    ok:false,
    stage,
    ...extra,
    token_exchange_executed:false,
    id_token_verification_executed:false,
    member_session_issued:false,
    access_token_persisted:false,
    refresh_token_persisted:false,
    customer_created:false,
    customer_id_generation:false,
    family_created:false,
    family_link_created:false,
    canonical_crm_write:false,
    line_send:false,
    production_write:false
  };
}

async function readJsonResponse(response){
  if(!response||typeof response.text!=='function'){
    return {ok:false,error:'external_response_invalid'};
  }

  let body;
  try{
    body=await response.text();
  }catch{
    return {ok:false,error:'external_response_read_failed'};
  }

  if(body.length>MAX_RESPONSE_BYTES){
    return {ok:false,error:'external_response_too_large'};
  }

  let payload;
  try{
    payload=JSON.parse(body);
  }catch{
    return {ok:false,error:'external_response_json_invalid'};
  }

  if(!payload||typeof payload!=='object'||Array.isArray(payload)){
    return {ok:false,error:'external_response_json_invalid'};
  }

  return {
    ok:true,
    http_ok:response.ok===true,
    status:Number(response.status)||0,
    payload
  };
}

export async function executeMemberLineLoginExchange(
  env,
  verifiedCallback,
  {
    approved=false,
    fetch_impl=globalThis.fetch,
    now_seconds,
    resolve_customer=resolveCanonicalCustomerByVerifiedLineUserId,
    read_family=readMemberFamilyByCustomer,
    issue_session=issueMemberSessionForVerifiedCustomer
  }={}
){
  if(!runtimeEnabled(env)){
    return failure('line_login_external_exchange_disabled','activation_gate');
  }
  if(approved!==true){
    return failure('line_login_activation_authorization_required','activation_gate');
  }

  const callback=callbackInput(verifiedCallback);
  if(!callback.ok){
    return failure(callback.error,'callback');
  }

  const configuredClientId=text(env?.MEMBER_LINE_LOGIN_CHANNEL_ID);
  const configuredRedirectUri=text(env?.MEMBER_LINE_LOGIN_REDIRECT_URI);
  if(
    !configuredClientId
    || !configuredRedirectUri
    || callback.client_id!==configuredClientId
    || callback.redirect_uri!==configuredRedirectUri
  ){
    return failure('line_login_callback_config_mismatch','callback');
  }

  const secret=channelSecret(env);
  if(!secret){
    return failure('line_login_channel_secret_not_configured','configuration');
  }
  if(typeof fetch_impl!=='function'){
    return failure('trusted_fetch_required','configuration');
  }
  if(
    typeof resolve_customer!=='function'
    || typeof read_family!=='function'
    || typeof issue_session!=='function'
  ){
    return failure('trusted_identity_dependencies_required','configuration');
  }

  const exchangePlan=buildMemberLineTokenExchangeRequest({
    authorization_code:callback.authorization_code,
    redirect_uri:callback.redirect_uri,
    client_id:callback.client_id,
    client_secret:secret,
    code_verifier:callback.code_verifier
  });
  if(exchangePlan.status!=='ok'||!exchangePlan.request){
    return failure(exchangePlan.status,'token_exchange_plan');
  }

  let exchangeResponse;
  try{
    exchangeResponse=await fetch_impl(exchangePlan.request);
  }catch{
    return failure('line_token_exchange_network_failed','token_exchange');
  }

  const exchangeJson=await readJsonResponse(exchangeResponse);
  if(!exchangeJson.ok){
    return failure(exchangeJson.error,'token_exchange');
  }
  if(!exchangeJson.http_ok){
    return failure('line_token_exchange_rejected','token_exchange',{
      external_status:exchangeJson.status
    });
  }

  const tokenResult=validateMemberLineTokenExchangeResponse(exchangeJson.payload);
  if(!tokenResult.ok){
    return failure(tokenResult.status,'token_exchange');
  }

  const verifyPlan=buildMemberLineIdTokenVerificationRequest({
    id_token:tokenResult.id_token,
    client_id:callback.client_id
  });
  if(verifyPlan.status!=='ok'||!verifyPlan.request){
    return failure(verifyPlan.status,'id_token_verification_plan');
  }

  let verifyResponse;
  try{
    verifyResponse=await fetch_impl(verifyPlan.request);
  }catch{
    return failure('line_id_token_verification_network_failed','id_token_verification');
  }

  const verifyJson=await readJsonResponse(verifyResponse);
  if(!verifyJson.ok){
    return failure(verifyJson.error,'id_token_verification');
  }
  if(!verifyJson.http_ok){
    return failure('line_id_token_verification_rejected','id_token_verification',{
      external_status:verifyJson.status
    });
  }

  const verifiedIdentity=validateVerifiedMemberLineIdTokenPayload(
    verifyJson.payload,
    {
      client_id:callback.client_id,
      nonce:callback.nonce,
      now_seconds
    }
  );
  if(!verifiedIdentity.verified){
    return failure(verifiedIdentity.status,'id_token_verification');
  }

  const customer=await resolve_customer(env,verifiedIdentity.verified_line_user_id);
  if(customer?.status!=='linked'){
    const reviewRequired=
      customer?.status==='ambiguous_line_identity'
      || customer?.status==='noncanonical_customer_id';
    return failure(
      customer?.status==='unlinked'?'member_customer_unlinked':text(customer?.status)||'member_customer_resolution_failed',
      'customer_identity',
      reviewRequired?{review_required:true}:{}
    );
  }

  const customerId=text(customer.customer_id);
  const family=await read_family(env,customerId);
  if(family?.status!=='linked'){
    const reviewRequired=family?.status==='ambiguous_family_identity';
    return failure(
      text(family?.status)||'member_family_resolution_failed',
      'family_identity',
      reviewRequired?{review_required:true}:{}
    );
  }

  const familyId=text(family?.family?.family_id);
  const session=await issue_session(env,{
    customer_id:customerId,
    family_id:familyId,
    now_seconds
  });
  if(session?.status!=='ok'||session?.issued!==true||!text(session?.cookie)){
    return failure(
      text(session?.status)||'member_session_issue_failed',
      'member_session'
    );
  }

  return {
    status:'ok',
    ok:true,
    stage:'complete',
    return_to:callback.return_to,
    clear_transaction_cookie:callback.clear_cookie,
    member_session_cookie:session.cookie,
    member_session_issued:true,
    identity_source:'verified_line_id_token_sub_exact_customer_family',
    token_exchange_executed:true,
    id_token_verification_executed:true,
    external_call_count:2,
    access_token_exposed:false,
    refresh_token_exposed:false,
    access_token_persisted:false,
    refresh_token_persisted:false,
    verified_line_user_id_exposed:false,
    customer_id_exposed:false,
    family_id_exposed:false,
    customer_created:false,
    customer_id_generation:false,
    family_created:false,
    family_link_created:false,
    canonical_crm_write:false,
    line_send:false,
    production_write:false
  };
}

export function memberLineLoginExchangeExecutorHealth(env){
  return {
    member_line_login_exchange_executor:true,
    build:BUILD,
    source_only:true,
    runtime_mode:runtimeEnabled(env)?'enabled':'disabled',
    runtime_default_disabled:true,
    explicit_activation_approval_required:true,
    channel_secret_configured:!!channelSecret(env),
    verified_callback_required:true,
    token_exchange_request_builder_reused:true,
    id_token_verification_request_builder_reused:true,
    issuer_audience_nonce_time_checks_reused:true,
    exact_verified_line_subject_only:true,
    exact_customer_identity_resolver_reused:true,
    explicit_family_link_required:true,
    member_session_issuer_reused:true,
    access_token_persistence:false,
    refresh_token_persistence:false,
    customer_create:false,
    customer_id_generation:false,
    family_create:false,
    family_auto_link:false,
    canonical_crm_write:false,
    line_send:false,
    production_route_wired:false,
    line_login_activated:false,
    production_write:false
  };
}

export const __test={
  BUILD,
  MODE,
  MAX_RESPONSE_BYTES,
  runtimeEnabled,
  channelSecret,
  callbackInput,
  readJsonResponse
};
