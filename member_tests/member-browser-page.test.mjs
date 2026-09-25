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
