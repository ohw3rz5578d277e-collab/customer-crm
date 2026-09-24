import {
  buildMemberLineTokenExchangeRequest,
  validateMemberLineTokenExchangeResponse,
  buildMemberLineIdTokenVerificationRequest,
  validateVerifiedMemberLineIdTokenPayload,
  memberLineTokenVerificationHealth,
  __test
} from '../src/member-line-token-verification.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const clientId='1234567890';
const clientSecret='line-channel-secret-for-tests';
const redirectUri='https://example.test/member/line/callback?source=member';
const codeVerifier='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~abc';
const authCode='AUTH-CODE-123';
const nonce='nonce-1234567890';
const lineSub='U1234567890abcdef1234567890abcdef';
const now=1800000000;

const exchange=buildMemberLineTokenExchangeRequest({
  authorization_code:authCode,
  redirect_uri:redirectUri,
  client_id:clientId,
  client_secret:clientSecret,
  code_verifier:codeVerifier
});
pass('token exchange request builder succeeds',exchange.status==='ok'&&exchange.request instanceof Request);
pass('token exchange endpoint is LINE Login v2.1 token endpoint',exchange.request.url===__test.TOKEN_ENDPOINT);
pass('token exchange is POST form request',exchange.request.method==='POST'&&exchange.request.headers.get('content-type')==='application/x-www-form-urlencoded');
const exchangeBody=new URLSearchParams(await exchange.request.text());
pass('token exchange uses authorization_code grant',exchangeBody.get('grant_type')==='authorization_code');
pass('token exchange binds authorization code and redirect URI',exchangeBody.get('code')===authCode&&exchangeBody.get('redirect_uri')===redirectUri);
pass('token exchange binds client id and client secret',exchangeBody.get('client_id')===clientId&&exchangeBody.get('client_secret')===clientSecret);
pass('token exchange submits PKCE verifier',exchangeBody.get('code_verifier')===codeVerifier);
pass('request builder itself performs no external call',exchange.external_call_executed===false&&exchange.secret_logged===false);

const badRedirect=buildMemberLineTokenExchangeRequest({
  authorization_code:authCode,
  redirect_uri:'http://example.test/callback',
  client_id:clientId,
  client_secret:clientSecret,
  code_verifier:codeVerifier
});
pass('non-HTTPS redirect is rejected',badRedirect.status==='invalid_redirect_uri');

const missingSecret=buildMemberLineTokenExchangeRequest({
  authorization_code:authCode,
  redirect_uri:redirectUri,
  client_id:clientId,
  client_secret:'',
  code_verifier:codeVerifier
});
pass('client secret is required for exchange request',missingSecret.status==='client_secret_required');

const badVerifier=buildMemberLineTokenExchangeRequest({
  authorization_code:authCode,
  redirect_uri:redirectUri,
  client_id:clientId,
  client_secret:clientSecret,
  code_verifier:'short'
});
pass('invalid PKCE verifier is rejected',badVerifier.status==='invalid_code_verifier');

const tokenPayload={
  access_token:'ACCESS-TOKEN-SECRET',
  expires_in:2592000,
  id_token:'header.payload.signature',
  refresh_token:'REFRESH-TOKEN-SECRET',
  scope:'openid',
  token_type:'Bearer'
};
const tokenResult=validateMemberLineTokenExchangeResponse(tokenPayload);
pass('valid token endpoint response is accepted',tokenResult.status==='ok'&&tokenResult.ok===true);
pass('normalized token response retains only ID token material needed for next step',tokenResult.id_token==='header.payload.signature');
pass('access and refresh token values are not exposed by normalized result',!('access_token' in tokenResult)&&!('refresh_token' in tokenResult)&&tokenResult.access_token_exposed===false&&tokenResult.refresh_token_exposed===false);
pass('openid scope is mandatory',validateMemberLineTokenExchangeResponse({...tokenPayload,scope:'profile'}).status==='line_openid_scope_missing');
pass('Bearer token type is mandatory',validateMemberLineTokenExchangeResponse({...tokenPayload,token_type:'bearer'}).status==='line_token_type_invalid');
pass('ID token is mandatory',validateMemberLineTokenExchangeResponse({...tokenPayload,id_token:''}).status==='line_id_token_missing_or_invalid');

const verifyRequest=buildMemberLineIdTokenVerificationRequest({
  id_token:tokenResult.id_token,
  client_id:clientId
});
pass('ID token verification request builder succeeds',verifyRequest.status==='ok'&&verifyRequest.request instanceof Request);
pass('ID token verification endpoint is LINE Login v2.1 verify endpoint',verifyRequest.request.url===__test.VERIFY_ENDPOINT);
const verifyBody=new URLSearchParams(await verifyRequest.request.text());
pass('verification request sends only id_token and client_id',verifyBody.get('id_token')===tokenResult.id_token&&verifyBody.get('client_id')===clientId&&[...verifyBody.keys()].length===2);
pass('verification request builder itself performs no external call',verifyRequest.external_call_executed===false);

const verifiedPayload={
  iss:'https://access.line.me',
  sub:lineSub,
  aud:clientId,
  exp:now+600,
  iat:now-10,
  nonce,
  amr:['pwd'],
  name:'Should Not Be Identity',
  picture:'https://example.test/picture.jpg',
  email:'not-used@example.test'
};
const verified=validateVerifiedMemberLineIdTokenPayload(verifiedPayload,{
  client_id:clientId,
  nonce,
  now_seconds:now
});
pass('verified LINE ID token payload is accepted',verified.status==='ok'&&verified.verified===true);
pass('only formal verified LINE sub becomes identity',verified.verified_line_user_id===lineSub&&verified.identity_source==='verified_line_id_token_sub');
pass('profile/email/access-token claims are not used for identity',verified.profile_claims_used===false&&verified.email_claim_used===false&&verified.access_token_used_as_identity===false);
pass('profile fields are not copied into verified result',!('name' in verified)&&!('email' in verified)&&!('picture' in verified));

pass('issuer mismatch fails closed',validateVerifiedMemberLineIdTokenPayload({...verifiedPayload,iss:'https://evil.example'},{client_id:clientId,nonce,now_seconds:now}).status==='line_id_token_issuer_mismatch');
pass('audience mismatch fails closed',validateVerifiedMemberLineIdTokenPayload({...verifiedPayload,aud:'9999999999'},{client_id:clientId,nonce,now_seconds:now}).status==='line_id_token_audience_mismatch');
pass('nonce mismatch fails closed',validateVerifiedMemberLineIdTokenPayload({...verifiedPayload,nonce:'other'},{client_id:clientId,nonce,now_seconds:now}).status==='line_id_token_nonce_mismatch');
pass('expired ID token fails closed',validateVerifiedMemberLineIdTokenPayload({...verifiedPayload,iat:now-600,exp:now-__test.CLOCK_SKEW_SECONDS-1},{client_id:clientId,nonce,now_seconds:now}).status==='line_id_token_expired');
pass('invalid ID token time ordering fails closed',validateVerifiedMemberLineIdTokenPayload({...verifiedPayload,iat:now-10,exp:now-20},{client_id:clientId,nonce,now_seconds:now}).status==='line_id_token_time_claims_invalid');
pass('future-issued ID token fails closed',validateVerifiedMemberLineIdTokenPayload({...verifiedPayload,iat:now+__test.CLOCK_SKEW_SECONDS+1,exp:now+600},{client_id:clientId,nonce,now_seconds:now}).status==='line_id_token_not_yet_valid');
pass('invalid LINE subject fails closed',validateVerifiedMemberLineIdTokenPayload({...verifiedPayload,sub:'not-line-user'},{client_id:clientId,nonce,now_seconds:now}).status==='line_id_token_subject_invalid');
pass('missing expected nonce fails closed',validateVerifiedMemberLineIdTokenPayload(verifiedPayload,{client_id:clientId,nonce:'',now_seconds:now}).status==='nonce_required');

const health=memberLineTokenVerificationHealth();
pass('health records token and ID verification endpoints',health.token_endpoint===__test.TOKEN_ENDPOINT&&health.id_token_verify_endpoint===__test.VERIFY_ENDPOINT);
pass('health requires PKCE and verified subject-only identity',health.pkce_code_verifier_required===true&&health.formal_line_subject_required===true&&health.verified_subject_only_identity===true);
pass('health records issuer audience nonce time checks',health.issuer_checked===true&&health.audience_checked===true&&health.nonce_checked===true&&health.expiry_checked===true&&health.issued_at_checked===true);
pass('health records no external calls or Member session issue',health.token_exchange_external_call===false&&health.id_token_verification_external_call===false&&health.member_session_issued===false);
pass('health records Production inactive',health.production_route_wired===false&&health.line_login_activated===false&&health.production_write===false);

console.log(`MEMBER_LINE_TOKEN_VERIFICATION=${n}/${n} PASS`);
