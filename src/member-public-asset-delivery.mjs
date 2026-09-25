import { memberAppShellCss } from './member-app-shell-ui.mjs';
import { memberHomeCss } from './member-home-ui.mjs';
import { memberMemoriesCss } from './member-memories-ui.mjs';
import { memberCreativeCreateCss } from './member-creative-create-ui.mjs';
import { memberShopCss } from './member-shop-ui.mjs';
import { memberMyCss } from './member-my-ui.mjs';

const BUILD='member-public-asset-delivery-20260925-01';
const PREFIX='/member-assets/';
const CSS_PATH='/member-assets/member-app.css';
const JS_PATH='/member-assets/member-app.js';
const MAX_PUBLIC_ASSET_BYTES=32*1024*1024;
const MIME_TYPES=new Set(['image/jpeg','image/png','image/webp','video/mp4']);

const text=value=>value==null?'':String(value).trim();

const CLIENT_JS=[
  "(()=>{'use strict';",
  "const tabs=new Set(['home','memories','create','shop','my']);",
  "function pathFor(tab){return tab==='home'?'/member':'/member/'+tab}",
  "function tabFromPath(path){if(path==='/member'||path==='/member/')return'home';const m=path.match(/^\\/member\\/(home|memories|create|shop|my)\\/?$/);return m?m[1]:'home'}",
  "function activate(tab,{push=false}={}){if(!tabs.has(tab))return;const root=document.querySelector('[data-member-app-shell]');if(!root)return;root.dataset.activeTab=tab;root.querySelectorAll('[data-member-tab]').forEach(el=>{const on=el.getAttribute('data-member-tab')===tab;el.classList.toggle('is-active',on);el.setAttribute('aria-selected',on?'true':'false')});root.querySelectorAll('[data-member-panel]').forEach(el=>{const on=el.getAttribute('data-member-panel')===tab;if(on)el.removeAttribute('hidden');else el.setAttribute('hidden','')});if(push&&location.pathname!==pathFor(tab))history.pushState({tab},'',pathFor(tab));window.scrollTo({top:0,behavior:'instant'})}",
  "document.addEventListener('click',event=>{const target=event.target.closest?.('[data-member-tab],[data-home-go-tab],[data-my-go-tab]');if(!target)return;const tab=target.getAttribute('data-member-tab')||target.getAttribute('data-home-go-tab')||target.getAttribute('data-my-go-tab');if(tabs.has(tab)){event.preventDefault();activate(tab,{push:true})}});",
  "addEventListener('popstate',()=>activate(tabFromPath(location.pathname)));",
  "activate(tabFromPath(location.pathname));",
  "})();"
].join('');

function builtInCss(){
  return [
    'html,body{margin:0;min-height:100%;background:#f7f4ec}',
    'body.mp-browser-body{min-height:100dvh}',
    '.mp-browser-login{min-height:100dvh;display:grid;place-items:center;padding:24px;background:#f7f4ec;color:#222520;font-family:ui-serif,"Hiragino Mincho ProN","Yu Mincho",serif}',
    '.mp-browser-login__card{width:min(520px,100%);padding:42px 30px;border:1px solid rgba(34,37,32,.10);border-radius:28px;background:#fffdf8;box-shadow:0 20px 55px rgba(34,37,32,.08)}',
    '.mp-browser-login__eyebrow{margin:0 0 10px;color:#315d4f;font:700 10px/1.2 ui-sans-serif,sans-serif;letter-spacing:.22em}',
    '.mp-browser-login h1{margin:0 0 22px;font-size:clamp(42px,12vw,72px);font-weight:500;letter-spacing:-.05em}',
    '.mp-browser-login p{color:#6f746c;line-height:1.9}',
    '.mp-browser-login__button{display:flex;justify-content:center;margin-top:28px;padding:15px 18px;border-radius:999px;background:#315d4f;color:#fff;text-decoration:none;font:700 13px/1 ui-sans-serif,sans-serif;letter-spacing:.06em}',
    '.mp-browser-login small{display:block;margin-top:14px;text-align:center;color:#8b8e87}',
    memberAppShellCss(),
    memberHomeCss(),
    memberMemoriesCss(),
    memberCreativeCreateCss(),
    memberShopCss(),
    memberMyCss()
  ].join('\n');
}

function response(body,status,headers={}){
  const h=new Headers({
    'cache-control':'public, max-age=3600',
    'x-content-type-options':'nosniff',
    'x-frame-options':'DENY',
    'x-robots-tag':'noindex, nofollow, noarchive',
    'referrer-policy':'no-referrer',
    'cross-origin-resource-policy':'same-origin',
    'x-member-public-asset-delivery-build':BUILD,
    ...headers
  });
  return new Response(body,{status,headers:h});
}

function validPublicPath(pathname){
  const raw=text(pathname);
  if(!raw.startsWith(PREFIX)||raw.length>PREFIX.length+300)return false;
  if(raw===PREFIX)return false;
  if(/[\u0000-\u001f\u007f]/.test(raw))return false;
  if(raw.includes('..')||raw.includes('\\')||raw.includes('%'))return false;
  if(raw.includes('?')||raw.includes('#'))return false;
  return /^\/member-assets\/[A-Za-z0-9._~!$&'()*+,;=:@\/-]+$/.test(raw);
}

function validAdapter(adapter){
  return !!adapter&&typeof adapter==='object'&&typeof adapter.get==='function';
}

async function first(env,sql,params=[]){
  if(!env?.DB||typeof env.DB.prepare!=='function')return null;
  let q=env.DB.prepare(sql);
  if(params.length)q=q.bind(...params);
  return (await q.first())||null;
}

async function registryRow(env,pathname){
  try{
    const table=await first(
      env,
      "SELECT name FROM sqlite_master WHERE type='table' AND name='member_public_assets' LIMIT 1"
    );
    if(!table)return {status:'public_asset_schema_not_applied',row:null};

    const sql='SELECT asset_id,asset_kind,local_path,mime_type,width,height '
      +'FROM member_public_assets '
      +'WHERE local_path=? '
      +"AND published=1 AND COALESCE(deleted_at,'')='' "
      +'LIMIT 1';
    const row=await first(env,sql,[pathname]);
    if(!row)return {status:'public_asset_not_found',row:null};

    const mime=text(row.mime_type);
    const kind=text(row.asset_kind);
    if(!MIME_TYPES.has(mime))return {status:'public_asset_invalid_registry_row',row:null};
    if(kind!=='image'&&kind!=='video')return {status:'public_asset_invalid_registry_row',row:null};
    if(kind==='image'&&!mime.startsWith('image/'))return {status:'public_asset_invalid_registry_row',row:null};
    if(kind==='video'&&mime!=='video/mp4')return {status:'public_asset_invalid_registry_row',row:null};

    return {
      status:'ok',
      row:{
        asset_id:text(row.asset_id),
        asset_kind:kind,
        local_path:text(row.local_path),
        mime_type:mime
      }
    };
  }catch{
    return {status:'public_asset_registry_unavailable',row:null};
  }
}

function normalizeAdapterObject(value,expectedMime){
  if(!value||typeof value!=='object')return null;
  const body=value.body;
  const size=Number(value.size);
  const contentType=text(value.content_type||value.contentType);
  if(body==null)return null;
  if(!Number.isInteger(size)||size<0||size>MAX_PUBLIC_ASSET_BYTES)return null;
  if(contentType!==expectedMime)return null;
  return {body,size,content_type:contentType};
}

export async function handleMemberPublicAssetDeliveryRequest(
  request,
  env,
  {asset_adapter=null}={}
){
  const url=new URL(request.url);
  if(!url.pathname.startsWith(PREFIX))return null;

  if(request.method!=='GET'&&request.method!=='HEAD'){
    return response(JSON.stringify({ok:false,error:'method_not_allowed'}),405,{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store'
    });
  }

  if(request.headers.get('range')){
    return response(JSON.stringify({ok:false,error:'range_not_supported'}),416,{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'accept-ranges':'none'
    });
  }

  if(url.pathname===CSS_PATH){
    const css=builtInCss();
    return response(request.method==='HEAD'?null:css,200,{
      'content-type':'text/css; charset=utf-8',
      'content-length':String(new TextEncoder().encode(css).byteLength)
    });
  }

  if(url.pathname===JS_PATH){
    return response(request.method==='HEAD'?null:CLIENT_JS,200,{
      'content-type':'text/javascript; charset=utf-8',
      'content-length':String(new TextEncoder().encode(CLIENT_JS).byteLength)
    });
  }

  if(!validPublicPath(url.pathname)){
    return response(JSON.stringify({ok:false,error:'not_found'}),404,{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store'
    });
  }

  const registry=await registryRow(env,url.pathname);
  if(registry.status==='public_asset_schema_not_applied'){
    return response(JSON.stringify({ok:false,error:registry.status}),409,{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store'
    });
  }
  if(registry.status==='public_asset_registry_unavailable'){
    return response(JSON.stringify({ok:false,error:registry.status}),503,{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store'
    });
  }
  if(registry.status!=='ok'){
    return response(JSON.stringify({ok:false,error:'not_found'}),404,{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store'
    });
  }

  if(!validAdapter(asset_adapter)){
    return response(JSON.stringify({ok:false,error:'public_asset_adapter_unavailable'}),503,{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store'
    });
  }

  let object;
  try{
    object=normalizeAdapterObject(
      await asset_adapter.get(registry.row.local_path),
      registry.row.mime_type
    );
  }catch{
    object=null;
  }

  if(!object){
    return response(JSON.stringify({ok:false,error:'public_asset_content_unavailable'}),503,{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store'
    });
  }

  return response(request.method==='HEAD'?null:object.body,200,{
    'content-type':object.content_type,
    'content-length':String(object.size),
    'content-disposition':'inline',
    'accept-ranges':'none',
    'cache-control':'public, max-age=86400'
  });
}

export function memberPublicAssetDeliveryHealth(){
  return {
    member_public_asset_delivery:true,
    build:BUILD,
    source_only:true,
    prefix:PREFIX,
    built_in_assets:[CSS_PATH,JS_PATH],
    built_in_asset_exact_allowlist:true,
    public_registry_required_for_catalog_assets:true,
    published_only:true,
    deleted_hidden:true,
    explicit_asset_adapter_required:true,
    implicit_env_asset_binding:false,
    supported_mime_types:[...MIME_TYPES],
    max_public_asset_bytes:MAX_PUBLIC_ASSET_BYTES,
    range_requests_supported:false,
    private_customer_media:false,
    private_storage_key_exposed:false,
    external_redirect:false,
    production_route_wired:false,
    production_asset_binding:false,
    production_asset_fetch:false,
    production_write:false
  };
}

export const __test={
  BUILD,
  PREFIX,
  CSS_PATH,
  JS_PATH,
  CLIENT_JS,
  builtInCss,
  validPublicPath,
  validAdapter,
  registryRow,
  normalizeAdapterObject,
  MAX_PUBLIC_ASSET_BYTES
};
