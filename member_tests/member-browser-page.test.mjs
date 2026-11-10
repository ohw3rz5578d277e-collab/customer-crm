import {
  loadMemberBrowserData,
  renderMemberBrowserApp,
  handleMemberBrowserPageRequest,
  memberBrowserPageHealth,
  __test
} from '../src/member-browser-page.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;
function pass(name,ok){assert(ok,name);n++;console.log('PASS',name)}

const origin='https://member.example.test';

pass('member root maps to HOME',__test.activeTabFromPath('/member')==='home'&&__test.activeTabFromPath('/member/')==='home');
pass('canonical tab paths map exactly',
  __test.activeTabFromPath('/member/memories')==='memories'
  && __test.activeTabFromPath('/member/create')==='create'
  && __test.activeTabFromPath('/member/shop')==='shop'
  && __test.activeTabFromPath('/member/my')==='my'
);
pass('unknown Member page path is rejected',__test.activeTabFromPath('/member/admin')===null);

const unrelated=await handleMemberBrowserPageRequest(
  new Request(origin+'/admin'),
  {},
  {verify_session:async()=>{throw new Error('must not verify unrelated route')}}
);
pass('non-Member page is not claimed',unrelated===null);

const method=await handleMemberBrowserPageRequest(
  new Request(origin+'/member',{method:'POST'}),
  {}
);
pass('Member browser page is GET-only',method.status===405);

const login=await handleMemberBrowserPageRequest(
  new Request(origin+'/member/memories'),
  {},
  {verify_session:async()=>({status:'member_session_required',verified:false})}
);
const loginHtml=await login.text();
pass('unauthenticated Member page renders local LINE login',login.status===200&&loginHtml.includes('/api/member/login/line/start?return_to=%2Fmember%2Fmemories'));
pass('login page uses external CSS and JS only',loginHtml.includes('/member-assets/member-app.css')&&loginHtml.includes('/member-assets/member-app.js')&&!loginHtml.includes('<style>'));
pass('login page is hardened by CSP',login.headers.get('content-security-policy').includes("script-src 'self'")&&login.headers.get('x-frame-options')==='DENY');

const expired=await handleMemberBrowserPageRequest(
  new Request(origin+'/member'),
  {},
  {verify_session:async()=>({status:'member_session_expired',verified:false})}
);
pass('expired session produces re-login page',(await expired.text()).includes('有効期限'));

const unconfigured=await handleMemberBrowserPageRequest(
  new Request(origin+'/member'),
  {},
  {verify_session:async()=>({status:'member_session_secret_not_configured',verified:false})}
);
pass('missing Member session secret fails closed',unconfigured.status===503);

const baseData={
  status:'ok',
  security_status:null,
  home:{status:'ok',home:{recent_memories:[],visible_memory_count:0,unavailable_sections:[],partial:false}},
  memories:{status:'ok',memories:[],favorites_available:false,favorite_mutation_ready:false},
  creative:{status:'ok',templates:[],memory_count:0,unlocked_count:0},
  shop:{status:'ok',products:[],available_count:0,pricing_authoritative:false,checkout_ready:false,discount_enforcement_ready:false},
  my:{status:'ok',my:{unavailable_sections:[],partial:false}},
  family_pass:{status:'ok',family_pass:{effective_black:false}},
  read_only:true
};

const rendered=renderMemberBrowserApp(baseData,{active_tab:'shop'});
pass('authenticated render produces canonical five-tab shell',rendered.status==='ok'&&['home','memories','create','shop','my'].every(tab=>rendered.html.includes('data-member-mount="'+tab+'"')));
pass('requested tab is active in server-rendered shell',rendered.html.includes('data-active-tab="shop"'));
pass('rendered page references only same-origin built-in app assets',rendered.html.includes('href="/member-assets/member-app.css"')&&rendered.html.includes('src="/member-assets/member-app.js"'));

const memoriesWithAuthorizedMetadata={
  ...baseData,
  memories:{
    status:'ok',
    family_id:'family-secret',
    favorites_available:false,
    favorite_mutation_ready:false,
    memories:[
      {memory_id:'mem-2026-01',title:'Family & Friends <Story>',
        shoot_date:'2026-09-01',genre:'ファミリー',preview_count:12,
        cover:{storage_key:'must-not-leak',media_id:'media-secret'}},
      {memory_id:'mem-2025-02',title:'七五三',
        shoot_date:'2025-11-15',genre:'七五三',preview_count:0}
    ]
  }
};
const memoriesDocument=renderMemberBrowserApp(memoriesWithAuthorizedMetadata,{active_tab:'memories'}).html;
pass('server-rendered browser MEMORIES offers native read-only details',
  (memoriesDocument.match(/<details class="mp-memories__card mp-memories__read-only-card">/g)||[]).length===2
  &&(memoriesDocument.match(/<summary class="mp-memories__read-only-summary">/g)||[]).length===2);
pass('browser MEMORIES has no inert detail buttons or private-media actions',
  !memoriesDocument.includes('data-memory-open')
  &&!memoriesDocument.includes('data-memory-detail')
  &&memoriesDocument.includes('個別写真の表示・お気に入り変更・注文はこの画面ではできません'));
pass('MEMORIES summary escapes titles and strips identity/media internals',
  memoriesDocument.includes('Family &amp; Friends &lt;Story&gt;')
  &&!memoriesDocument.includes('family-secret')
  &&!memoriesDocument.includes('media-secret')
  &&!memoriesDocument.includes('must-not-leak')
  &&!memoriesDocument.includes('mem-2026-01'));
pass('read-only browser MEMORY still displays authorized shoot dates and genres',
  memoriesDocument.includes('2026-09-01')
  &&memoriesDocument.includes('2025-11-15')
  &&memoriesDocument.includes('ファミリー')
  &&memoriesDocument.includes('七五三'));

const shopWithAuthorizedCatalog={
  ...baseData,
  shop:{
    status:'ok',
    family_id:'family-secret',
    customer_id:'26000001',
    available_count:2,
    pricing_authoritative:false,
    checkout_ready:false,
    discount_enforcement_ready:false,
    products:[
      {product_id:'product-album-1',product_type:'album',
        title:'KAHO <Album>',description:'家族の&記録',
        season_tag:'evergreen',hero_asset_ref:{public_asset:{public_path:'/member-assets/shop/album.jpg'},storage_key:'private-r2-key'},
        pricing:{authoritative:false,amount:980000},checkout:{ready:false}},
      {product_id:'product-canvas-2',product_type:'canvas',
        title:'CANVAS',description:'写真を飾る',
        season_tag:'evergreen',
        hero_asset_ref:{public_asset:{public_path:'https://evil.example/remote.jpg'}}}
    ]
  }
};
const shopDocument=renderMemberBrowserApp(shopWithAuthorizedCatalog,{active_tab:'shop'}).html;
pass('real browser SHOP exposes native category and product information rather than inert buttons',
  (shopDocument.match(/<details class="mp-shop__card mp-shop__read-only-card">/g)||[]).length===2
  &&shopDocument.includes('aria-label="取り扱い商品カテゴリ"')
  &&!shopDocument.includes('data-shop-category')
  &&!shopDocument.includes('data-shop-product')
  &&!shopDocument.includes('data-shop-close'));
pass('real browser SHOP provides pricing and discount boundary without checkout or order actions',
  shopDocument.includes('価格・購入は準備中です。注文処理やFamily Pass特典の割引適用は行いません。')
  &&!shopDocument.includes('980000')
  &&!shopDocument.includes('data-shop-detail'));
pass('real browser SHOP strips identity and unsafe media URLs and escapes titles',
  shopDocument.includes('KAHO &lt;Album&gt;')
  &&shopDocument.includes('家族の&amp;記録')
  &&shopDocument.includes('/member-assets/shop/album.jpg')
  &&!shopDocument.includes('family-secret')
  &&!shopDocument.includes('26000001')
  &&!shopDocument.includes('private-r2-key')
  &&!shopDocument.includes('product-album-1')
  &&!shopDocument.includes('evil.example'));

const combinedReadonlyDocument=renderMemberBrowserApp({...shopWithAuthorizedCatalog,memories:memoriesWithAuthorizedMetadata.memories},{active_tab:'memories'}).html;
pass('combined browser MEMORIES and SHOP preserve independent native disclosure views',
  (combinedReadonlyDocument.match(/<details class="mp-memories__card mp-memories__read-only-card">/g)||[]).length===2
  &&(combinedReadonlyDocument.match(/<details class="mp-shop__card mp-shop__read-only-card">/g)||[]).length===2
  &&combinedReadonlyDocument.includes('data-active-tab="memories"'));
pass('combined read-only tabs expose no inert detail, mutation, pricing or identity payloads',
  !combinedReadonlyDocument.includes('data-memory-open')
  &&!combinedReadonlyDocument.includes('data-shop-product')
  &&!combinedReadonlyDocument.includes('data-shop-detail')
  &&!combinedReadonlyDocument.includes('980000')
  &&!combinedReadonlyDocument.includes('family-secret')
  &&!combinedReadonlyDocument.includes('private-r2-key')
  &&!combinedReadonlyDocument.includes('must-not-leak')
  &&!combinedReadonlyDocument.includes('evil.example'));

const combinedTabIds=['home','memories','create','shop','my'];
pass('combined read-only browser links both desktop/mobile tabs to canonical panels',
  combinedTabIds.every(tab=>
    combinedReadonlyDocument.split('aria-controls="member-panel-'+tab+'"').length===3
    &&combinedReadonlyDocument.includes('id="member-panel-'+tab+'" data-member-panel="'+tab+'"')
  ));
pass('combined browser renders selected-only roving keyboard stops for both navs',
  (combinedReadonlyDocument.match(/tabindex="0" role="tab"/g)||[]).length===2
  &&(combinedReadonlyDocument.match(/tabindex="-1" role="tab"/g)||[]).length===8
  &&combinedReadonlyDocument.split('data-member-tab="memories" aria-selected="true" aria-controls="member-panel-memories" tabindex="0" role="tab"').length===3
  &&shopDocument.split('data-member-tab="shop" aria-selected="true" aria-controls="member-panel-shop" tabindex="0" role="tab"').length===3);
pass('combined read-only browser retains served keyboard-capable asset without private data',
  combinedReadonlyDocument.includes('src="/member-assets/member-app.js"')
  &&combinedReadonlyDocument.includes('data-member-app-shell')
  &&!combinedReadonlyDocument.includes('must-not-leak')
  &&!combinedReadonlyDocument.includes('private-r2-key')
  &&!combinedReadonlyDocument.includes('family-secret'));

const app=await handleMemberBrowserPageRequest(
  new Request(origin+'/member/shop'),
  {},
  {
    verify_session:async()=>({
      status:'ok',
      verified:true,
      session:{customer_id:'26000001',family_id:'family-secret'}
    }),
    load_data:async()=>baseData
  }
);
const appHtml=await app.text();
pass('verified session receives Member app document',app.status===200&&appHtml.includes('data-member-app-shell'));
pass('browser document does not expose session Customer or Family IDs',!appHtml.includes('26000001')&&!appHtml.includes('family-secret'));
pass('Member page is always no-store and non-indexable',app.headers.get('cache-control').includes('no-store')&&app.headers.get('x-robots-tag').includes('noindex'));

const denied=await handleMemberBrowserPageRequest(
  new Request(origin+'/member'),
  {},
  {
    verify_session:async()=>({status:'ok',verified:true,session:{customer_id:'26000001',family_id:'family-secret'}}),
    load_data:async()=>({status:'family_access_denied',security_status:'family_access_denied'})
  }
);
pass('identity authorization conflict denies whole browser app',denied.status===403&&!((await denied.text()).includes('family-secret')));

let calls=0;
const loaded=await loadMemberBrowserData(
  {},
  {customer_id:'26000001',family_id:'family-a'},
  {
    read_home:async()=>{calls++;return {status:'ok'}},
    read_memories:async()=>{calls++;return {status:'ok'}},
    read_creative:async()=>{calls++;return {status:'ok'}},
    read_shop:async()=>{calls++;return {status:'ok'}},
    read_my:async()=>{calls++;return {status:'family_access_denied'}},
    read_family_pass:async()=>{calls++;return {status:'ok'}}
  }
);
pass('browser loader evaluates all canonical page sources',calls===6);
pass('browser loader elevates identity conflict to whole-page denial',loaded.security_status==='family_access_denied');

const health=memberBrowserPageHealth();
pass('browser page health is source-only',health.member_browser_page===true&&health.source_only===true);
pass('browser page requires signed session for app content',health.signed_member_session_required_for_app===true&&health.unauthenticated_login_page===true);
pass('browser page server-renders canonical five tabs',health.server_rendered_canonical_five_tabs===true);
pass('browser page has no auto-fetch or auto-write',health.client_auto_fetch===false&&health.client_auto_write===false);
pass('browser page exposes no identity or private storage key',health.customer_id_exposed===false&&health.family_id_exposed===false&&health.private_storage_key_exposed===false);
pass('browser page remains Production-unwired and write-free',health.production_route_wired===false&&health.production_write===false&&health.line_send===false&&health.customer_id_generation===false);

console.log('MEMBER_BROWSER_PAGE='+n+'/'+n+' PASS');
