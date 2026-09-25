import {
  handleMemberPublicAssetDeliveryRequest,
  memberPublicAssetDeliveryHealth,
  __test
} from '../src/member-public-asset-delivery.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const origin='https://member.example.test';

function makeDb(row,{schema=true,throwRead=false}={}){
  return {
    prepare(sql){
      return {
        bind(...params){
          return {
            async first(){
              if(throwRead)throw new Error('db fail');
              if(sql.includes('sqlite_master'))return schema?{name:'member_public_assets'}:null;
              if(sql.includes('FROM member_public_assets')){
                if(row&&params[0]===row.local_path)return row;
                return null;
              }
              return null;
            }
          };
        },
        async first(){
          if(throwRead)throw new Error('db fail');
          if(sql.includes('sqlite_master'))return schema?{name:'member_public_assets'}:null;
          return null;
        }
      };
    }
  };
}

const outside=await handleMemberPublicAssetDeliveryRequest(
  new Request(origin+'/favicon.ico'),
  {}
);
pass('non Member asset path is not claimed',outside===null);

const css=await handleMemberPublicAssetDeliveryRequest(
  new Request(origin+__test.CSS_PATH),
  {}
);
const cssText=await css.text();
pass('built-in CSS is served without external asset binding',css.status===200&&css.headers.get('content-type').startsWith('text/css'));
pass('built-in CSS contains canonical shell and tab styles',cssText.includes('.mp-shell')&&cssText.includes('.mp-home')&&cssText.includes('.mp-memories')&&cssText.includes('.mp-create')&&cssText.includes('.mp-shop')&&cssText.includes('.mp-my'));

const js=await handleMemberPublicAssetDeliveryRequest(
  new Request(origin+__test.JS_PATH),
  {}
);
const jsText=await js.text();
pass('built-in browser JS is served from exact allowlist',js.status===200&&js.headers.get('content-type').startsWith('text/javascript'));
pass('browser JS only binds local tab navigation',jsText.includes("new Set(['home','memories','create','shop','my'])")&&jsText.includes('history.pushState')&&!jsText.includes('fetch('));

const badMethod=await handleMemberPublicAssetDeliveryRequest(
  new Request(origin+__test.CSS_PATH,{method:'POST'}),
  {}
);
pass('Member assets are GET/HEAD only',badMethod.status===405);

const ranged=await handleMemberPublicAssetDeliveryRequest(
  new Request(origin+'/member-assets/creative/hero.webp',{headers:{range:'bytes=0-10'}}),
  {}
);
pass('Range is rejected',ranged.status===416&&ranged.headers.get('accept-ranges')==='none');

pass('public path validation accepts safe local asset',
  __test.validPublicPath('/member-assets/creative/hero.webp')===true
);
pass('public path validation rejects traversal and encoded forms',
  __test.validPublicPath('/member-assets/../secret')===false
  && __test.validPublicPath('/member-assets/%2e%2e/secret')===false
  && __test.validPublicPath('/member-assets/')===false
);

const noSchema=await handleMemberPublicAssetDeliveryRequest(
  new Request(origin+'/member-assets/creative/hero.webp'),
  {DB:makeDb(null,{schema:false})},
  {asset_adapter:{get(){throw new Error('must not fetch')}}}
);
pass('catalog asset requires public registry schema',noSchema.status===409);

const row={
  asset_id:'creative.hero',
  asset_kind:'image',
  local_path:'/member-assets/creative/hero.webp',
  mime_type:'image/webp',
  width:1200,
  height:800
};

const noAdapter=await handleMemberPublicAssetDeliveryRequest(
  new Request(origin+row.local_path),
  {DB:makeDb(row)}
);
pass('catalog asset requires explicit adapter',noAdapter.status===503&&(await noAdapter.clone().json()).error==='public_asset_adapter_unavailable');

let keySeen='';
const delivered=await handleMemberPublicAssetDeliveryRequest(
  new Request(origin+row.local_path),
  {DB:makeDb(row)},
  {
    asset_adapter:{
      async get(key){
        keySeen=key;
        return {
          body:new Uint8Array([1,2,3,4]),
          size:4,
          content_type:'image/webp'
        };
      }
    }
  }
);
pass('catalog asset fetches exact registry local path',delivered.status===200&&keySeen===row.local_path);
pass('catalog asset MIME must match validated registry',delivered.headers.get('content-type')==='image/webp');
pass('catalog asset response is same-origin and nosniff',delivered.headers.get('cross-origin-resource-policy')==='same-origin'&&delivered.headers.get('x-content-type-options')==='nosniff');

const mimeMismatch=await handleMemberPublicAssetDeliveryRequest(
  new Request(origin+row.local_path),
  {DB:makeDb(row)},
  {
    asset_adapter:{
      async get(){
        return {body:new Uint8Array([1]),size:1,content_type:'image/png'};
      }
    }
  }
);
pass('adapter MIME mismatch fails closed',mimeMismatch.status===503);

const tooLarge=__test.normalizeAdapterObject({
  body:new Uint8Array([1]),
  size:__test.MAX_PUBLIC_ASSET_BYTES+1,
  content_type:'image/webp'
},'image/webp');
pass('oversized public asset is rejected',tooLarge===null);

const missing=await handleMemberPublicAssetDeliveryRequest(
  new Request(origin+'/member-assets/creative/missing.webp'),
  {DB:makeDb(row)},
  {asset_adapter:{async get(){throw new Error('must not fetch missing registry path')}}}
);
pass('unregistered public asset is hidden as 404',missing.status===404);

const health=memberPublicAssetDeliveryHealth();
pass('asset delivery health is source-only',health.member_public_asset_delivery===true&&health.source_only===true);
pass('built-in asset delivery is exact allowlist only',health.built_in_asset_exact_allowlist===true&&health.built_in_assets.length===2);
pass('catalog delivery requires published registry and explicit adapter',health.public_registry_required_for_catalog_assets===true&&health.published_only===true&&health.deleted_hidden===true&&health.explicit_asset_adapter_required===true);
pass('asset delivery never discovers env binding implicitly',health.implicit_env_asset_binding===false);
pass('public delivery excludes private customer media and storage keys',health.private_customer_media===false&&health.private_storage_key_exposed===false&&health.external_redirect===false);
pass('asset delivery remains Production-unwired and fetch/write inactive',health.production_route_wired===false&&health.production_asset_binding===false&&health.production_asset_fetch===false&&health.production_write===false);

console.log('MEMBER_PUBLIC_ASSET_DELIVERY='+n+'/'+n+' PASS');
