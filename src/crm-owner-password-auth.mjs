const BUILD='crm-owner-password-auth-20260910-04';
const COOKIE_NAME='crm_owner_session';
const SESSION_MAX_AGE_SECONDS=60*60*12;
const OWNER_EMAIL='ohw3rz5578d277e@gmail.com';
const encoder=new TextEncoder();

function text(v){return v==null?'':String(v).trim()}
function authMode(env){
  const mode=text(env?.CRM_OWNER_AUTH_MODE).toLowerCase();
  return mode==='hybrid'||mode==='password'?mode:'access';
}
function secureHeaders(headers={}){
  const h=new Headers(headers);
  h.set('cache-control','no-store, no-cache, must-revalidate, max-age=0');
  h.set('pragma','no-cache');
  h.set('expires','0');
  h.set('x-robots-tag','noindex, nofollow,noarchive');
  h.set('x-content-type-options','nosniff');
  h.set('referrer-policy','no-referrer');
  h.set('x-frame-options','DENY');
  return h;
}
function json(data,status=200,headers={}){
  return new Response(JSON.stringify(data),{status,headers:secureHeaders({'content-type':'application/json; charset=utf-8','x-crm-owner-password-auth-build':BUILD,...headers})});
}
function html(body,status=200,headers={}){
  return new Response(body,{status,headers:secureHeaders({'content-type':'text/html; charset=utf-8','x-crm-owner-password-auth-build':BUILD,...headers})});
}
function b64url(bytes){
  let s='';for(const b of bytes)s+=String.fromCharCode(b);
  return btoa(s).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
}
function fromB64url(value){
  const s=String(value||'').replace(/-/g,'+').replace(/_/g,'/');
  const padded=s+'='.repeat((4-(s.length%4))%4);
  try{const raw=atob(padded),out=new Uint8Array(raw.length);for(let i=0;i<raw.length;i++)out[i]=raw.charCodeAt(i);return out}catch{return new Uint8Array()}
}
function safeEqual(a,b){
  if(a.length!==b.length)return false;
  let diff=0;for(let i=0;i<a.length;i++)diff|=a[i]^b[i];return diff===0;
}
async function sha256(value){return new Uint8Array(await crypto.subtle.digest('SHA-256',encoder.encode(String(value))))}
async function passwordMatches(supplied,expected){
  if(!supplied||!expected)return false;
  return safeEqual(await sha256(supplied),await sha256(expected));
}
async function hmac(value,secret){
  const key=await crypto.subtle.importKey('raw',encoder.encode(secret),{name:'HMAC',hash:'SHA-256'},false,['sign']);
  return new Uint8Array(await crypto.subtle.sign('HMAC',key,encoder.encode(value)));
}
function cookieValue(request,name){
  const raw=request.headers.get('cookie')||'';
  for(const part of raw.split(';')){const i=part.indexOf('=');if(i<0)continue;if(part.slice(0,i).trim()===name)return part.slice(i+1).trim()}
  return'';
}
function sessionCookie(token){return `${COOKIE_NAME}=${token}; Path=/; Max-Age=${SESSION_MAX_AGE_SECONDS}; HttpOnly; Secure; SameSite=Strict`}
function clearCookie(){return `${COOKIE_NAME}=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Strict`}
function configured(env){return !!text(env?.CRM_OWNER_PASSWORD)&&!!text(env?.CRM_OWNER_SESSION_SECRET)}
function passwordEnabled(env){return authMode(env)!=='access'&&configured(env)}
function hasAccessPrincipal(request){return !!text(request.headers.get('cf-access-authenticated-user-email')||request.headers.get('Cf-Access-Authenticated-User-Email')||request.headers.get('cf-access-user-email'))}
function loginPage(error=''){
  const errorHtml=error?`<div class="error">${error}</div>`:'';
  return `<!doctype html><html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow,noarchive"><title>Owner Login</title><style>*{box-sizing:border-box}body{margin:0;min-height:100vh;display:grid;place-items:center;background:#0b0d10;color:#f5f7fa;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans JP',sans-serif;padding:20px}.card{width:min(100%,420px);background:#15181d;border:1px solid #2a2f37;border-radius:24px;padding:28px;box-shadow:0 24px 80px rgba(0,0,0,.35)}h1{font-size:26px;margin:0 0 8px}.sub{font-size:14px;color:#9aa4b2;line-height:1.7;margin-bottom:24px}label{display:block;font-size:13px;color:#c8d0da;margin-bottom:8px}input{width:100%;font-size:18px;padding:15px 16px;border-radius:14px;border:1px solid #343b46;background:#0f1216;color:#fff;outline:none}input:focus{border-color:#8d99aa}button{width:100%;margin-top:16px;border:0;border-radius:14px;padding:15px 16px;font-size:16px;font-weight:700;background:#f5f7fa;color:#111318}.error{background:#32191d;border:1px solid #643038;color:#ffb4bd;border-radius:12px;padding:11px 12px;margin:0 0 16px;font-size:13px}</style></head><body><main class="card"><h1>予約・顧客管理</h1><div class="sub">Ownerパスワードを入力してください。</div>${errorHtml}<form method="post" action="/__crm/owner-login" autocomplete="on"><label for="password">パスワード</label><input id="password" name="password" type="password" autocomplete="current-password" required autofocus><button type="submit">ログイン</button></form></main></body></html>`;
}
async function readPassword(request){
  const ct=(request.headers.get('content-type')||'').toLowerCase();
  if(ct.includes('application/json')){const body=await request.json().catch(()=>null);return text(body?.password)}
  if(ct.includes('application/x-www-form-urlencoded')||ct.includes('multipart/form-data')){const form=await request.formData().catch(()=>null);return text(form?.get('password'))}
  return'';
}
function sameOrigin(request){
  const origin=text(request.headers.get('origin'));if(!origin)return true;
  try{return new URL(origin).origin===new URL(request.url).origin}catch{return false}
}
async function issueSession(env){
  const payloadBytes=encoder.encode(JSON.stringify({v:1,sub:'owner',email:OWNER_EMAIL,exp:Math.floor(Date.now()/1000)+SESSION_MAX_AGE_SECONDS}));
  const payload=b64url(payloadBytes),sig=b64url(await hmac(payload,text(env.CRM_OWNER_SESSION_SECRET)));
  return `${payload}.${sig}`;
}
async function verifySession(request,env){
  if(!configured(env))return false;
  const token=cookieValue(request,COOKIE_NAME),parts=token.split('.');if(parts.length!==2)return false;
  const [payload,sig]=parts;if(!payload||!sig)return false;
  const expected=await hmac(payload,text(env.CRM_OWNER_SESSION_SECRET));if(!safeEqual(fromB64url(sig),expected))return false;
  try{
    const decoded=new TextDecoder().decode(fromB64url(payload)),data=JSON.parse(decoded);
    return data?.v===1&&data?.sub==='owner'&&data?.email===OWNER_EMAIL&&Number(data?.exp)>Math.floor(Date.now()/1000);
  }catch{return false}
}
function stripSyntheticAuthHeaders(request){
  const headers=new Headers(request.headers);
  headers.delete('x-crm-owner-auth');
  return headers;
}
export async function handleOwnerPasswordAuth(request,env){
  const url=new URL(request.url),mode=authMode(env);
  if(url.pathname==='/__crm/owner-login'&&request.method==='GET'){
    if(mode==='access')return html(loginPage('現在はCloudflare Accessログインが有効です。'),503);
    if(!configured(env))return html(loginPage('パスワードログインはまだ有効化されていません。'),503);
    if(await verifySession(request,env))return new Response(null,{status:302,headers:secureHeaders({location:'/admin'})});
    return html(loginPage());
  }
  if(url.pathname==='/__crm/owner-login'&&request.method==='POST'){
    if(mode==='access'||!configured(env))return json({ok:false,error:'owner_password_auth_not_configured'},503);
    if(!sameOrigin(request))return json({ok:false,error:'origin_mismatch'},403);
    const password=await readPassword(request);
    if(!(await passwordMatches(password,text(env.CRM_OWNER_PASSWORD))))return html(loginPage('パスワードが違います。'),401);
    const token=await issueSession(env);
    return new Response(null,{status:303,headers:secureHeaders({location:'/admin','set-cookie':sessionCookie(token)})});
  }
  if(url.pathname==='/__crm/owner-logout'&&request.method==='POST'){
    if(!sameOrigin(request))return json({ok:false,error:'origin_mismatch'},403);
    return new Response(null,{status:303,headers:secureHeaders({location:'/__crm/owner-login','set-cookie':clearCookie()})});
  }
  return null;
}
export async function withOwnerPasswordPrincipal(request,env){
  const mode=authMode(env),headers=stripSyntheticAuthHeaders(request);
  if(mode==='password'){
    // Password-only mode must not trust any client-supplied legacy or Access identity header.
    headers.delete('cf-access-authenticated-user-email');
    headers.delete('cf-access-user-email');
    headers.delete('x-user-email');
  }
  const base=new Request(request,{headers});
  if(mode==='access')return base;
  if(mode==='hybrid'&&hasAccessPrincipal(base))return base;
  if(!(await verifySession(base,env)))return base;
  headers.set('cf-access-authenticated-user-email',OWNER_EMAIL);
  headers.set('x-crm-owner-auth','password-session');
  return new Request(base,{headers});
}
export function handleOwnerPasswordBrowserGate(request,env){
  if(authMode(env)!=='password')return null;
  const url=new URL(request.url);
  if(request.method!=='GET'||(url.pathname!=='/'&&url.pathname!=='/admin'))return null;
  if(hasAccessPrincipal(request))return null;
  if(!configured(env))return html(loginPage('パスワードログインのsecret設定が不足しています。'),503);
  return new Response(null,{status:302,headers:secureHeaders({location:'/__crm/owner-login'})});
}
export function ownerPasswordAuthHealth(env){return{
  owner_password_auth_supported:true,
  owner_password_auth_mode:authMode(env),
  owner_password_auth_configured:configured(env),
  owner_password_auth_enabled:passwordEnabled(env),
  owner_password_auth_fail_closed:true,
  owner_password_auth_header_spoof_protection:true,
  owner_password_auth_cookie_http_only:true,
  owner_password_auth_cookie_secure:true,
  owner_password_auth_cookie_same_site:'Strict',
  owner_password_auth_session_seconds:SESSION_MAX_AGE_SECONDS,
  owner_password_auth_customer_id_generation:false,
  owner_password_auth_d1_write:false,
  owner_password_auth_line_send:false
}}
