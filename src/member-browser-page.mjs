import { verifyMemberSessionRequest } from './member-session-foundation.mjs';
import { readMemberHomeForSession } from './member-home-read-model.mjs';
import { readMemberMemoriesForSession } from './member-memories-read-model.mjs';
import { readMemberCreativeCatalogForSession } from './member-creative-catalog-read-model.mjs';
import { readMemberShopCatalogForSession } from './member-shop-pickup-read-model.mjs';
import { readMemberMyForSession } from './member-my-read-model.mjs';
import { readMemberFamilyPassForSession } from './member-family-pass-read-model.mjs';
import { buildMemberAppShellViewModel, renderMemberAppShellMarkup } from './member-app-shell-ui.mjs';
import { buildMemberHomeViewModel, renderMemberHomeMarkup } from './member-home-ui.mjs';
import { buildMemberMemoriesViewModel, renderMemberMemoriesMarkup } from './member-memories-ui.mjs';
import { buildMemberCreativeCreateViewModel, renderMemberCreativeCreateMarkup } from './member-creative-create-ui.mjs';
import { buildMemberShopViewModel, renderMemberShopMarkup } from './member-shop-ui.mjs';
import { buildMemberMyViewModel, renderMemberMyMarkup } from './member-my-ui.mjs';

const BUILD='member-browser-page-20260925-01';
const TABS=new Set(['home','memories','create','shop','my']);
const SECURITY_DENIALS=new Set([
  'family_access_denied',
  'ambiguous_family_identity',
  'family_identity_incomplete',
  'ambiguous_child_identity',
  'component_identity_mismatch'
]);

const text=value=>value==null?'':String(value).trim();

function html(body,status=200){
  return new Response(body,{
    status,
    headers:{
      'content-type':'text/html; charset=utf-8',
      'cache-control':'no-store, no-cache, must-revalidate, max-age=0',
      'pragma':'no-cache',
      'x-content-type-options':'nosniff',
      'x-frame-options':'DENY',
      'x-robots-tag':'noindex, nofollow, noarchive',
      'referrer-policy':'no-referrer',
      'cross-origin-opener-policy':'same-origin',
      'cross-origin-resource-policy':'same-origin',
      'content-security-policy':[
        "default-src 'none'",
        "base-uri 'none'",
        "form-action 'self'",
        "frame-ancestors 'none'",
        "script-src 'self'",
        "style-src 'self'",
        "style-src-attr 'unsafe-inline'",
        "img-src 'self' blob: data:",
        "media-src 'self'",
        "connect-src 'self'",
        "font-src 'self'"
      ].join('; '),
      'x-member-browser-page-build':BUILD
    }
  });
}

function activeTabFromPath(pathname){
  const raw=text(pathname);
  if(raw==='/member'||raw==='/member/')return 'home';
  const match=raw.match(/^\/member\/(home|memories|create|shop|my)\/?$/);
  return match?match[1]:null;
}

function safeReturnPath(pathname){
  const tab=activeTabFromPath(pathname);
  if(!tab)return '/member';
  return tab==='home'?'/member':'/member/'+tab;
}

function escapeHtml(value){
  return String(value??'').replace(/[&<>"']/g,char=>({
    '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
  })[char]);
}

function documentShell({title,body,body_class=''}) {
  return '<!doctype html><html lang="ja"><head>'
    +'<meta charset="utf-8">'
    +'<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">'
    +'<meta name="color-scheme" content="light dark">'
    +'<title>'+escapeHtml(title)+'</title>'
    +'<link rel="stylesheet" href="/member-assets/member-app.css">'
    +'</head><body class="'+escapeHtml(body_class)+'">'+body
    +'<script defer src="/member-assets/member-app.js"></script>'
    +'</body></html>';
}

function loginDocument(returnTo,{reason='session_required'}={}){
  const href='/api/member/login/line/start?return_to='+encodeURIComponent(returnTo);
  const note=reason==='session_expired'
    ?'ログインの有効期限が切れました。もう一度LINEでログインしてください。'
    :'家族のMEMORYを見るにはLINEでログインしてください。';

  return documentShell({
    title:'MIZUNO PHOTO MEMBER',
    body:'<main class="mp-browser-login"><div class="mp-browser-login__card">'
      +'<p class="mp-browser-login__eyebrow">MIZUNO PHOTO</p>'
      +'<h1>MEMBER</h1>'
      +'<p>'+escapeHtml(note)+'</p>'
      +'<a class="mp-browser-login__button" href="'+escapeHtml(href)+'">LINEでログイン</a>'
      +'<small>ログイン後、このページに戻ります。</small>'
      +'</div></main>',
    body_class:'mp-browser-body'
  });
}

function unavailableResponse(message,status=503){
  return html(documentShell({
    title:'MIZUNO PHOTO MEMBER',
    body:'<main class="mp-browser-login"><div class="mp-browser-login__card">'
      +'<p class="mp-browser-login__eyebrow">MIZUNO PHOTO</p>'
      +'<h1>MEMBER</h1>'
      +'<p>'+escapeHtml(message)+'</p>'
      +'</div></main>',
    body_class:'mp-browser-body'
  }),status);
}

async function safeRead(reader,...args){
  try{
    return await reader(...args);
  }catch{
    return {status:'member_component_unavailable',read_only:true};
  }
}

export async function loadMemberBrowserData(env,session,{
  read_home=readMemberHomeForSession,
  read_memories=readMemberMemoriesForSession,
  read_creative=readMemberCreativeCatalogForSession,
  read_shop=readMemberShopCatalogForSession,
  read_my=readMemberMyForSession,
  read_family_pass=readMemberFamilyPassForSession
}={}){
  const [home,memories,creative,shop,my,familyPass]=await Promise.all([
    safeRead(read_home,env,session),
    safeRead(read_memories,env,session),
    safeRead(read_creative,env,session),
    safeRead(read_shop,env,session),
    safeRead(read_my,env,session),
    safeRead(read_family_pass,env,session)
  ]);

  const statuses=[home,memories,creative,shop,my,familyPass]
    .map(item=>text(item?.status))
    .filter(Boolean);
  const securityStatus=statuses.find(status=>SECURITY_DENIALS.has(status))||'';

  return {
    status:securityStatus||'ok',
    security_status:securityStatus||null,
    home,
    memories,
    creative,
    shop,
    my,
    family_pass:familyPass,
    read_only:true
  };
}

function replaceMount(shell,tab,markup){
  const marker='<div class="mp-shell__mount" data-member-mount="'+tab+'"></div>';
  if(!shell.includes(marker))return null;
  return shell.replace(
    marker,
    '<div class="mp-shell__mount" data-member-mount="'+tab+'">'+markup+'</div>'
  );
}

export function renderMemberBrowserApp(data,{active_tab='home'}={}){
  if(data?.status!=='ok'){
    return {status:text(data?.status)||'member_page_unavailable',html:''};
  }

  const shellVm=buildMemberAppShellViewModel({
    active_tab,
    family_pass:data.family_pass
  });

  let shell=renderMemberAppShellMarkup(shellVm);
  const tabMarkup={
    home:renderMemberHomeMarkup(buildMemberHomeViewModel(data.home)),
    memories:renderMemberMemoriesMarkup(buildMemberMemoriesViewModel(data.memories)),
    create:renderMemberCreativeCreateMarkup(buildMemberCreativeCreateViewModel({
      catalog:data.creative,
      memory_details:[]
    })),
    shop:renderMemberShopMarkup(buildMemberShopViewModel(data.shop)),
    my:renderMemberMyMarkup(buildMemberMyViewModel(data.my))
  };

  for(const tab of TABS){
    const next=replaceMount(shell,tab,tabMarkup[tab]);
    if(next===null){
      return {status:'member_shell_mount_contract_mismatch',html:''};
    }
    shell=next;
  }

  return {
    status:'ok',
    html:documentShell({
      title:'MIZUNO PHOTO MEMBER',
      body:shell,
      body_class:'mp-browser-body'
    }),
    source_only:true
  };
}

export async function handleMemberBrowserPageRequest(
  request,
  env,
  {
    verify_session=verifyMemberSessionRequest,
    load_data=loadMemberBrowserData,
    now_seconds
  }={}
){
  const url=new URL(request.url);
  const activeTab=activeTabFromPath(url.pathname);
  if(!activeTab)return null;

  if(request.method!=='GET'){
    return html('',405);
  }

  if(typeof verify_session!=='function'){
    return unavailableResponse('Memberログインを確認できません。',503);
  }

  const verified=await verify_session(request,env,{now_seconds});
  if(verified?.status==='member_session_secret_not_configured'){
    return unavailableResponse('Memberログインはまだ有効化されていません。',503);
  }

  if(verified?.status!=='ok'||verified?.verified!==true||!verified?.session){
    const reason=verified?.status==='member_session_expired'?'session_expired':'session_required';
    return html(loginDocument(safeReturnPath(url.pathname),{reason}),200);
  }

  const data=await load_data(env,verified.session);
  if(data?.security_status){
    return unavailableResponse('Member情報を安全に確認できませんでした。',403);
  }

  const rendered=renderMemberBrowserApp(data,{active_tab:activeTab});
  if(rendered.status!=='ok'){
    return unavailableResponse('Member appを表示できません。',503);
  }

  return html(rendered.html,200);
}

export function memberBrowserPageHealth(){
  return {
    member_browser_page:true,
    build:BUILD,
    source_only:true,
    routes:[
      '/member',
      '/member/home',
      '/member/memories',
      '/member/create',
      '/member/shop',
      '/member/my'
    ],
    signed_member_session_required_for_app:true,
    unauthenticated_login_page:true,
    login_start_path:'/api/member/login/line/start',
    server_rendered_canonical_five_tabs:true,
    client_auto_fetch:false,
    client_auto_write:false,
    customer_id_exposed:false,
    family_id_exposed:false,
    private_storage_key_exposed:false,
    inline_script:false,
    external_script_path:'/member-assets/member-app.js',
    external_style_path:'/member-assets/member-app.css',
    production_route_wired:false,
    production_write:false,
    line_send:false,
    customer_id_generation:false
  };
}

export const __test={
  BUILD,
  TABS,
  SECURITY_DENIALS,
  activeTabFromPath,
  safeReturnPath,
  escapeHtml,
  documentShell,
  loginDocument,
  replaceMount
};
