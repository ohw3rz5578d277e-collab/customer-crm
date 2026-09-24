import {
  createMemberLineLoginTransaction,
  verifyMemberLineLoginCallback,
  memberLineLoginTransactionHealth,
  memberLineLoginTransactionCookie,
  __test
} from '../src/member-line-login-transaction.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};

const channelId='1234567890';
const redirectUri='https://example.test/member/line/callback?source=member';
const secret='member-line-login-transaction-secret-1234567890';
const now=1800000000;

const env={
  MEMBER_LINE_LOGIN_CHANNEL_ID:channelId,
  MEMBER_LINE_LOGIN_REDIRECT_URI:redirectUri,
  MEMBER_LINE_LOGIN_TRANSACTION_SECRET:secret
};

function tokenFromSetCookie(setCookie){
  const first=String(setCookie||'').split(';',1)[0];
  return first.slice(first.indexOf('=')+1);
}

async function sha256B64url(value){
  const bytes=new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value)));
  let s='';
  for(const b of bytes)s+=String.fromCharCode(b);
  return btoa(s).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
}

const tx=await createMemberLineLoginTransaction(env,{return_to:'/member/memories',now_seconds:now});
pass('configured transaction is created',tx.status==='ok'&&tx.created===true);
pass('authorization endpoint is LINE Login v2.1',new URL(tx.authorization_url).origin==='https://access.line.me'&&new URL(tx.authorization_url).pathname==='/oauth2/v2.1/authorize');

const authUrl=new URL(tx.authorization_url);
pass('authorization request uses code flow',authUrl.searchParams.get('response_type')==='code');
pass('authorization request uses configured channel and redirect',authUrl.searchParams.get('client_id')===channelId&&authUrl.searchParams.get('redirect_uri')===redirectUri);
pass('state and nonce are present and distinct',!!authUrl.searchParams.get('state')&&!!authUrl.searchParams.get('nonce')&&authUrl.searchParams.get('state')!==authUrl.searchParams.get('nonce'));
pass('minimal scope requests openid only',authUrl.searchParams.get('scope')==='openid');
pass('PKCE uses S256',authUrl.searchParams.get('code_challenge_method')==='S256'&&!!authUrl.searchParams.get('code_challenge'));
pass('transaction output records local return target',tx.transaction.return_to==='/member/memories');

const cookieToken=tokenFromSetCookie(tx.cookie);
const decoded=await __test.verifySignedPayload(cookieToken,secret,now+1);
pass('signed transaction cookie verifies',decoded.ok===true);
pass('PKCE challenge matches hidden verifier',authUrl.searchParams.get('code_challenge')===await sha256B64url(decoded.transaction.code_verifier));
pass('transaction cookie is HttpOnly Secure SameSite=Lax',tx.cookie.includes('HttpOnly')&&tx.cookie.includes('Secure')&&tx.cookie.includes('SameSite=Lax')&&tx.cookie.includes('Path=/'));
pass('transaction cookie uses __Host prefix',tx.cookie.startsWith('__Host-mizuno_member_login_tx='));

const callbackUrl=new URL(redirectUri);
callbackUrl.searchParams.set('code','AUTH-CODE-123');
callbackUrl.searchParams.set('state',tx.transaction.state);
const verified=await verifyMemberLineLoginCallback(
  new Request(callbackUrl.toString(),{headers:{cookie:`${__test.COOKIE_NAME}=${cookieToken}`}}),
  env,
  {now_seconds:now+30}
);
pass('matching callback transaction verifies',verified.status==='ok'&&verified.verified===true);
pass('verified callback returns server-side PKCE verifier and nonce',!!verified.code_verifier&&verified.nonce===tx.transaction.nonce);
pass('verified callback does not exchange token or issue Member session',verified.token_exchange_executed===false&&verified.id_token_verified===false&&verified.member_session_issued===false);
pass('verified callback keeps configured redirect and local return target',verified.redirect_uri===redirectUri&&verified.return_to==='/member/memories');

const wrongStateUrl=new URL(callbackUrl);
wrongStateUrl.searchParams.set('state','wrong-state');
const wrongState=await verifyMemberLineLoginCallback(
  new Request(wrongStateUrl.toString(),{headers:{cookie:`${__test.COOKIE_NAME}=${cookieToken}`}}),
  env,
  {now_seconds:now+30}
);
pass('state mismatch fails closed',wrongState.status==='line_login_state_mismatch'&&wrongState.verified===false);

const [tamperedPayload,originalSig]=cookieToken.split('.');
const tamperedSig=(originalSig[0]==='a'?'b':'a')+originalSig.slice(1);
const tamperedToken=`${tamperedPayload}.${tamperedSig}`;
const tampered=await verifyMemberLineLoginCallback(
  new Request(callbackUrl.toString(),{headers:{cookie:`${__test.COOKIE_NAME}=${tamperedToken}`}}),
  env,
  {now_seconds:now+30}
);
pass('tampered transaction cookie fails signature verification',tampered.status==='line_login_transaction_signature_invalid'&&tampered.verified===false);

const expired=await verifyMemberLineLoginCallback(
  new Request(callbackUrl.toString(),{headers:{cookie:`${__test.COOKIE_NAME}=${cookieToken}`}}),
  env,
  {now_seconds:now+__test.TX_MAX_AGE_SECONDS+31}
);
pass('expired transaction fails closed',expired.status==='line_login_transaction_expired'&&expired.verified===false);

const missingCookie=await verifyMemberLineLoginCallback(
  new Request(callbackUrl.toString()),
  env,
  {now_seconds:now+30}
);
pass('missing transaction cookie fails closed',missingCookie.status==='line_login_transaction_missing');

const deniedUrl=new URL(redirectUri);
deniedUrl.searchParams.set('error','access_denied');
deniedUrl.searchParams.set('state',tx.transaction.state);
const denied=await verifyMemberLineLoginCallback(
  new Request(deniedUrl.toString(),{headers:{cookie:`${__test.COOKIE_NAME}=${cookieToken}`}}),
  env,
  {now_seconds:now+30}
);
pass('LINE authorization error is handled without token exchange',denied.status==='line_authorization_denied'&&denied.verified===false);

const badCallback=await verifyMemberLineLoginCallback(
  new Request('https://evil.example/member/line/callback?source=member&code=AUTH-CODE-123&state='+encodeURIComponent(tx.transaction.state),{
    headers:{cookie:`${__test.COOKIE_NAME}=${cookieToken}`}
  }),
  env,
  {now_seconds:now+30}
);
pass('callback origin/path must match configured redirect URI',badCallback.status==='line_login_callback_redirect_mismatch');

const missingConfiguredQuery=await verifyMemberLineLoginCallback(
  new Request('https://example.test/member/line/callback?code=AUTH-CODE-123&state='+encodeURIComponent(tx.transaction.state),{
    headers:{cookie:`${__test.COOKIE_NAME}=${cookieToken}`}
  }),
  env,
  {now_seconds:now+30}
);
pass('configured redirect query parameters are revalidated',missingConfiguredQuery.status==='line_login_callback_redirect_mismatch');

const changedChannel=await verifyMemberLineLoginCallback(
  new Request(callbackUrl.toString(),{headers:{cookie:`${__test.COOKIE_NAME}=${cookieToken}`}}),
  {...env,MEMBER_LINE_LOGIN_CHANNEL_ID:'2345678901'},
  {now_seconds:now+30}
);
pass('transaction bound to original channel configuration',changedChannel.status==='line_login_transaction_config_mismatch');

const openRedirect=await createMemberLineLoginTransaction(env,{return_to:'https://evil.example/',now_seconds:now});
pass('absolute return target is rejected',openRedirect.status==='invalid_return_to'&&openRedirect.created===false);

const protocolRelative=await createMemberLineLoginTransaction(env,{return_to:'//evil.example/path',now_seconds:now});
pass('protocol-relative return target is rejected',protocolRelative.status==='invalid_return_to');

const shortSecret=await createMemberLineLoginTransaction({
  ...env,
  MEMBER_LINE_LOGIN_TRANSACTION_SECRET:'short'
},{now_seconds:now});
pass('short transaction secret fails closed',shortSecret.status==='line_login_transaction_secret_not_configured');

const httpRedirect=await createMemberLineLoginTransaction({
  ...env,
  MEMBER_LINE_LOGIN_REDIRECT_URI:'http://example.test/member/line/callback'
},{now_seconds:now});
pass('non-HTTPS callback is rejected',httpRedirect.status==='line_login_redirect_uri_invalid');

pass('callback matcher accepts configured query plus OAuth response params',__test.callbackMatchesRedirectUri(callbackUrl.toString(),redirectUri)===true);
pass('callback matcher rejects different path',__test.callbackMatchesRedirectUri('https://example.test/other?source=member&code=x&state=y',redirectUri)===false);
pass('clear cookie keeps security attributes',memberLineLoginTransactionCookie.clear().includes('Max-Age=0')&&memberLineLoginTransactionCookie.clear().includes('HttpOnly')&&memberLineLoginTransactionCookie.clear().includes('Secure'));

const health=memberLineLoginTransactionHealth(env);
pass('health records state nonce and PKCE requirements',health.state_required===true&&health.nonce_required===true&&health.pkce_required===true&&health.pkce_method==='S256');
pass('health records minimal scopes',health.scope==='openid'&&health.profile_scope_requested===false&&health.email_scope_requested===false);
pass('health records callback redirect revalidation',health.callback_redirect_revalidated===true);
pass('health records no token exchange/session issue/Production route',health.token_exchange_executed===false&&health.id_token_verification_executed===false&&health.member_session_issued===false&&health.production_route_wired===false&&health.line_login_activated===false&&health.production_write===false);

console.log(`MEMBER_LINE_LOGIN_TRANSACTION=${n}/${n} PASS`);
