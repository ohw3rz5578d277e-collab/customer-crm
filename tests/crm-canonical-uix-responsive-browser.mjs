import assert from 'node:assert/strict';
import fs from 'node:fs';
import http from 'node:http';
import { chromium } from 'playwright';
import { composeCustomer360AdminHtml } from '../src/production-index-crm-customer360-entry.js';

const customers=[
 {customer_id:'26000101',name:'山田 花子',line_display_name:'はなこ',line_linked:true,realized_ltv:128000,shoot_count:3,last_shoot_date:'2026-08-20',recommendation:{next_offer:'七五三'},next_opportunity:{label:'七五三',days:30}},
 {customer_id:'26000102',name:'佐藤 未来',line_display_name:'みらい',line_linked:true,realized_ltv:35000,shoot_count:1,last_shoot_date:'2026-06-10',recommendation:{next_offer:'家族写真'},next_opportunity:{label:'誕生日',days:60}}
];
const linePage1=[...customers,...Array.from({length:98},(_,i)=>({customer_id:String(26001000+i),name:'LINE顧客 '+(i+3),line_display_name:'line-'+(i+3),line_linked:true}))];
const linePage2Customer={customer_id:'26999999',name:'LINE 101件目',line_display_name:'page-two',line_linked:true};
const facets={prefectures:['大阪府'],cities:['大阪市'],genres:['七五三'],sources:['Instagram'],campaigns:[],school_stages:[]};
const base=`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><style>html,body{margin:0}.app{width:100%;box-sizing:border-box;padding:16px}#lineOpsPanel{display:none;position:fixed;inset:20px;background:#fff;z-index:9999}#lineOpsPanel.open{display:block}</style><style id="crm-admin-users-style">#crmUserFab{position:fixed;left:12px;right:12px;bottom:12px;z-index:99999}</style></head><body><button id="lineOpsOpen">旧LINE</button><section id="lineOpsPanel"><h2>LINE送信・反応管理</h2></section><div class="crm-user-fab" id="crmUserFab"><button id="crmOpenUsers">ユーザー管理</button></div><div id="crmUserBackdrop"></div><div id="crmUserModal">旧管理ユーザー</div><main class="app"><h1>顧客管理</h1><section id="crmTodayDashboard"><h2>今日やること</h2></section></main><script>document.getElementById('lineOpsOpen').onclick=()=>document.getElementById('lineOpsPanel').classList.add('open')</script><script id="crm-admin-users-script">document.getElementById('crmOpenUsers').onclick=()=>document.getElementById('crmUserModal').classList.add('show')</script></body></html>`;
const html=composeCustomer360AdminHtml(base);
assert.equal(html.includes('const navObserver=new MutationObserver'),false,'canonical shell must not install a permanent document-wide nav observer');
assert.equal(html.includes("new MutationObserver(()=>reconcileDesktopLayout()).observe(document.documentElement"),false,'legacy desktop hotfix must not install a permanent document-wide observer');
const out='artifacts/crm-canonical-uix-responsive';fs.mkdirSync(out,{recursive:true});
const requests=[],notFoundRequests=[];
let delayNextLineHistory=false,delayNextLineList=false,failNextLineList=false,malformedNextLineList=false,stalledNextLineList=null,malformedNextLineHistory=null;
let holdNextLineAvatar=false,releaseHeldLineAvatar=null;
function send(res,status,data,type='application/json; charset=utf-8'){res.writeHead(status,{'content-type':type,'cache-control':'no-store'});res.end(type.startsWith('application/json')?JSON.stringify(data):data)}
const server=http.createServer(async(req,res)=>{
 const u=new URL(req.url,'http://127.0.0.1');requests.push(req.method+' '+u.pathname+u.search);
 if(u.pathname==='/'||u.pathname==='/admin')return send(res,200,html,'text/html; charset=utf-8');
 if(u.pathname==='/api/customer360/marketing-home')return send(res,200,{ok:true,kpis:{customers:2,average_realized_ltv:81500,repeat_rate_pct:50,vip_high_ltv:1,event_90d:2,dormant_180:0,line_link_rate_pct:100,line_additions_this_month:7,approach_this_month:2},top_opportunities:customers,facets});
 if(u.pathname==='/__fixture/delay-next-line-list'){delayNextLineList=true;return send(res,200,{ok:true})}
 if(u.pathname==='/__fixture/fail-next-line-list'){failNextLineList=true;return send(res,200,{ok:true})}
 if(u.pathname==='/__fixture/malformed-next-line-list'){malformedNextLineList=true;return send(res,200,{ok:true})}
 if(u.pathname==='/__fixture/stall-next-line-list'){stalledNextLineList=u.searchParams.get('mode')||'empty';return send(res,200,{ok:true})}
 if(u.pathname==='/api/customer360/customers'){
  if(u.searchParams.get('line')==='linked'){
   const page=Number(u.searchParams.get('page')||1);
   if(page===1){
    if(delayNextLineList){delayNextLineList=false;await new Promise(resolve=>setTimeout(resolve,700));return send(res,200,{ok:true,total:0,all_total:0,page:1,page_size:100,has_next:false,items:[],facets})}
    if(failNextLineList){failNextLineList=false;return send(res,200,{ok:false,error:'sensitive_internal_database_failure_do_not_render'})}
    if(malformedNextLineList){malformedNextLineList=false;return send(res,200,{ok:true,has_next:false,items:null})}
    return send(res,200,{ok:true,total:101,all_total:101,page:1,page_size:100,has_next:true,items:linePage1,facets,meta:{privacy_safe_list_dto:true}});
   }
   if(page===2){
     if(stalledNextLineList){const mode=stalledNextLineList;stalledNextLineList=null;return send(res,200,{ok:true,total:101,all_total:101,page:2,page_size:100,has_next:true,items:mode==='duplicate'?linePage1:[],facets})}
     return send(res,200,{ok:true,total:101,all_total:101,page:2,page_size:100,has_next:false,items:[linePage2Customer],facets,meta:{privacy_safe_list_dto:true}});
    }
  }
  return send(res,200,{ok:true,total:2,all_total:2,page:1,page_size:100,has_next:false,items:customers,facets,meta:{privacy_safe_list_dto:true}});
 }
 if(u.pathname==='/api/customer360/analytics')return send(res,200,{ok:true,available:true,period:{from:'2026-09-01',to:'2026-09-30',span_days:30},current:{revenue:163000,completed_shoots:4,unique_customers:2,average_order_value:40750,repeat_customers_in_period:1,repeat_rate_pct:50,line_additions:7,genres:[],monthly:[]},previous:{line_additions:5},change_pct:{line_additions:40}});
 if(u.pathname==='/api/customer360/approach-queue')return send(res,200,{ok:true,items:[],summary:{total:0,ready:0,review_required:0,opted_out:0}});
 if(u.pathname==='/__fixture/delay-next-line-history'){delayNextLineHistory=true;return send(res,200,{ok:true})}
 if(u.pathname==='/__fixture/malformed-next-line-history'){malformedNextLineHistory=u.searchParams.get('shape')||'missing';return send(res,200,{ok:true})}
 if(u.pathname==='/api/customers/26000101/line-history'){if(malformedNextLineHistory){const shape=malformedNextLineHistory;malformedNextLineHistory=null;return send(res,200,shape==='object'?{ok:true,connected:true,messages:{unexpected:'object'}}:shape==='null-item'?{ok:true,connected:true,messages:[null]}:shape==='string-item'?{ok:true,connected:true,messages:['invalid']}:shape==='array-item'?{ok:true,connected:true,messages:[[]]}:{ok:true,connected:true})}if(delayNextLineHistory){delayNextLineHistory=false;await new Promise(resolve=>setTimeout(resolve,650))}return send(res,200,{ok:true,connected:true,count:2,messages:[{direction:'inbound',message_text:'七五三の撮影について相談したいです',sent_at:'2026-09-14 10:00'},{direction:'outbound',message_text:'ありがとうございます。候補日をご案内します。',sent_at:'2026-09-14 10:03'}]})}
 if(u.pathname==='/api/customers/26000102/line-history')return send(res,200,{ok:true,connected:true,count:0,messages:[]});
 if(u.pathname==='/api/customers/26999999/line-history')return send(res,200,{ok:false,error:'line_history_unavailable'});
 if(u.pathname==='/__fixture/hold-next-line-avatar'){holdNextLineAvatar=true;return send(res,200,{ok:true})}
 if(u.pathname==='/__fixture/release-line-avatar'){releaseHeldLineAvatar?.();releaseHeldLineAvatar=null;return send(res,200,{ok:true})}
 if(u.pathname.startsWith('/api/customer360/media/')){
  const id=decodeURIComponent(u.pathname.split('/').pop());let held=false;
  if(id==='26000101'&&holdNextLineAvatar){holdNextLineAvatar=false;held=true;await new Promise(resolve=>{releaseHeldLineAvatar=resolve})}
  const avatar=held?'data:image/svg+xml,%3Csvg%20xmlns=%22http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%22%20width=%221%22%20height=%221%22%3E%3C%2Fsvg%3E':'';
  return send(res,200,{ok:true,media:{customer_id:id,avatar_data_url:avatar,avatar_updated_at:'',latest_delivery_link:null,delivery_links:[]}})
 }
 if(u.pathname.startsWith('/api/customer360/customer/')){const id=decodeURIComponent(u.pathname.split('/').pop());const c=customers.find(x=>x.customer_id===id)||customers[0];return send(res,200,{ok:true,customer:{...c,address:{},family:[],opportunities:[],reservations:[],line_history:[],marketing_history:[],consent:{},raw:{}}})}
 notFoundRequests.push(req.method+' '+u.pathname+u.search);
 return send(res,404,{ok:false,error:'not_found'});
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({headless:true});
try{
 for(const viewport of [{width:320,height:720},{width:360,height:800},{width:390,height:844},{width:412,height:915},{width:767,height:900},{width:768,height:900},{width:1024,height:768},{width:1440,height:900}]){
  const mobile=viewport.width<=767;
  const context=await browser.newContext({viewport,hasTouch:mobile,isMobile:mobile});
  const page=await context.newPage(),errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  page.on('console',m=>{if(m.type()==='error')errors.push(m.text())});
  await page.goto(origin+'/admin',{waitUntil:'domcontentloaded'});
  await page.waitForFunction(()=>window.__crmOwnerView&&document.getElementById('crmOwnerAppShell')&&document.querySelector('#crmMktList tbody tr'));
  await page.waitForFunction(()=>window.__crmOwnerView.getCurrentView()==='customers');
  const assertCurrentView=async key=>{
   const navStates=await page.evaluate(()=>({
    desktop:[...document.querySelectorAll('#crmOwnerDesktopSidebar [data-crm-shell-nav]')].map(el=>({key:el.dataset.crmShellNav,active:el.classList.contains('active'),current:el.getAttribute('aria-current')})),
    mobile:[...document.querySelectorAll('#crmOwnerMobileNav [data-owner-tab]')].map(el=>({key:el.dataset.ownerTab,active:el.classList.contains('active'),current:el.getAttribute('aria-current')}))
   }));
   for(const [surface,items] of Object.entries(navStates)){
    assert.equal(items.length,5,viewport.width+': '+surface+' navigation destinations changed');
    assert.deepEqual(items.filter(x=>x.active).map(x=>x.key),[key],viewport.width+': '+surface+' active view mismatch');
    assert.deepEqual(items.filter(x=>x.current==='page').map(x=>x.key),[key],viewport.width+': '+surface+' aria-current mismatch');
    assert.ok(items.every(x=>x.current===(x.active?'page':null)),viewport.width+': '+surface+' inactive navigation incorrectly announced as current');
   }
  };
  await assertCurrentView('customers');
  assert.equal(await page.locator('#crmTodayDashboard').isVisible(),false,viewport.width+': Today dashboard must stay removed');
  assert.equal(await page.locator('#lineOpsPanel').isVisible(),false,viewport.width+': legacy LINE panel must stay hidden');
  assert.equal(await page.locator('#crmUserFab').isVisible(),false,viewport.width+': legacy user-management overlay must stay quarantined');
  assert.equal(await page.locator('#crmUserModal').isVisible(),false,viewport.width+': legacy user-management modal must stay quarantined');
  assert.ok((await page.locator('#crmOwnerWorkspaceTitle').textContent()).includes('顧客'),viewport.width+': Customer must be initial view');
  assert.equal(await page.locator('#crmMktNav').isVisible(),false,viewport.width+': duplicate inner CRM nav must stay hidden');
  assert.equal(await page.locator('#crmOwnerWorkspaceContent > .app > h1').isVisible().catch(()=>false),false,viewport.width+': legacy Customer Management heading leaked');
  assert.equal(await page.locator('#crmCsvImportOpen').count(),0,viewport.width+': floating CSV import button leaked into canonical shell');
  assert.equal(await page.locator('#crmCsvImportOpenInline').isVisible(),true,viewport.width+': inline CSV import action missing from customer toolbar');
  const overflow=await page.evaluate(()=>Math.max(document.documentElement.scrollWidth-document.documentElement.clientWidth,document.body.scrollWidth-document.body.clientWidth));
  assert.ok(overflow<=1,viewport.width+': horizontal overflow '+overflow);
  const fontFamily=(await page.evaluate(()=>getComputedStyle(document.body).fontFamily)).toLowerCase();
  const fontTokens=fontFamily.split(',').map(x=>x.trim().replace(/^["']|["']$/g,''));
  assert.equal(fontTokens.includes('serif')||fontTokens.some(x=>x.includes('mincho')),false,viewport.width+': serif/mincho font leaked '+fontFamily);
  assert.ok(fontTokens.includes('sans-serif'),viewport.width+': canonical sans stack missing '+fontFamily);
  const firstCustomer=page.locator('#crmMktList [data-open],#crmMktList [data-direct-customer]').first();
  const firstCustomerId=(await firstCustomer.getAttribute('data-open'))||(await firstCustomer.getAttribute('data-direct-customer'))||'';
  await firstCustomer.click();
  await page.locator('#crmMktDetail.open').waitFor();
  await page.waitForTimeout(300);
  const mediaDiagnostic=await page.evaluate(()=>({
    media_flag:!!window.__crmCustomerMediaUi20260917,
    media_script:!!document.getElementById('crm-customer360-media-ui-20260917-02'),
    detail_open:document.getElementById('crmMktDetail')?.classList.contains('open')||false,
    detail_has_customer_id:/Customer ID\s+\d{8}/.test(document.getElementById('crmMktDetailBody')?.innerText||''),
    hero:!!document.getElementById('crmCustomerAvatarHero')
  }));
  const naturalHero=mediaDiagnostic.hero;
  let manualDiagnostic=null;
  if(!naturalHero&&/^\d{8}$/.test(firstCustomerId)){
    await page.evaluate(id=>document.dispatchEvent(new CustomEvent('crm:customer-detail-opened',{detail:{customer_id:id}})),firstCustomerId);
    await page.waitForTimeout(300);
    manualDiagnostic={
      hero:await page.locator('#crmCustomerAvatarHero').count()>0,
      media_gets:requests.filter(x=>x.includes('/api/customer360/media/')).length
    };
  }
  console.log('CRM_MEDIA_UI_DIAGNOSTIC='+JSON.stringify({...mediaDiagnostic,media_gets:requests.filter(x=>x.includes('/api/customer360/media/')).length,page_errors:errors.length,manual_event:manualDiagnostic}));
  assert.equal(naturalHero,true,viewport.width+': avatar hero did not render from the natural detail-open path');
  await page.locator('#crmCustomerAvatarHero').waitFor();
  assert.equal(await page.locator('#crmCustomerAvatarHero #crmCmHeroChoose').isVisible(),true,viewport.width+': profile image change action missing');
  await page.locator('#crmCustomerMediaCard summary').click();
  assert.equal(await page.locator('#crmCmChoose').isVisible(),true,viewport.width+': library avatar control missing');
  assert.equal(await page.locator('#crmCmCamera').isVisible(),true,viewport.width+': camera avatar control missing');
  await page.locator('.crm-detail-close').click();
  await page.locator('#crmMktDetail.open').waitFor({state:'hidden'});
  if(mobile){
   assert.equal(await page.locator('#crmOwnerDesktopSidebar').isVisible(),false,viewport.width+': desktop sidebar leaked to mobile');
   assert.equal(await page.locator('#crmOwnerMobileNav').isVisible(),true,viewport.width+': canonical mobile nav missing');
   const labels=(await page.locator('#crmOwnerMobileNav').innerText()).replace(/\s+/g,'');
   for(const label of ['顧客','検索','分析','LINE','予約'])assert.ok(labels.includes(label),viewport.width+': nav missing '+label);
   assert.equal(labels.includes('今日'),false,viewport.width+': Today navigation leaked');
   const navTargets=await page.locator('#crmOwnerMobileNav button, #crmOwnerMobileNav a').evaluateAll(nodes=>nodes.map(el=>({label:el.textContent.trim(),height:el.getBoundingClientRect().height,width:el.getBoundingClientRect().width})));
   assert.equal(navTargets.length,5,viewport.width+': canonical mobile nav must retain five destinations');
   for(const target of navTargets)assert.ok(target.height>=44&&target.width>=44,viewport.width+': mobile nav touch target too small '+JSON.stringify(target));
   const navBounds=await page.locator('#crmOwnerMobileNav').evaluate(el=>{const r=el.getBoundingClientRect();return{left:r.left,right:r.right,bottom:r.bottom,viewportWidth:innerWidth,viewportHeight:innerHeight}});
   assert.ok(navBounds.left>=-1&&navBounds.right<=navBounds.viewportWidth+1,viewport.width+': mobile nav overflows horizontal viewport '+JSON.stringify(navBounds));
   assert.ok(navBounds.bottom<=navBounds.viewportHeight+1,viewport.width+': mobile nav extends below viewport '+JSON.stringify(navBounds));
   const reservation=await page.locator('#crmOwnerNavReservation').evaluate(el=>({href:el.getAttribute('href'),target:el.target,rel:el.rel}));
   assert.ok(reservation.href?.endsWith('/admin')&&reservation.target==='_blank'&&reservation.rel.includes('noopener'),viewport.width+': Reservation handoff changed');
  }else{
   assert.equal(await page.locator('#crmOwnerDesktopSidebar').isVisible(),true,viewport.width+': PC rendered as smartphone');
   assert.equal(await page.locator('#crmOwnerMobileNav').isVisible(),false,viewport.width+': mobile nav visible on desktop');
   const dims=await page.locator('#crmOwnerWorkspace').evaluate(el=>({w:el.getBoundingClientRect().width,sw:document.documentElement.scrollWidth,cw:document.documentElement.clientWidth}));
   assert.ok(dims.w>viewport.width*0.65,viewport.width+': desktop workspace too narrow '+JSON.stringify(dims));
  }
  if(viewport.width===390||viewport.width===1440)await page.screenshot({path:out+'/'+viewport.width+'-canonical-customer-list.png',fullPage:true});
  const analysisSelector=mobile?'#crmOwnerNavMarketing':'#crmOwnerDesktopSidebar [data-crm-shell-nav="marketing"]';
  await page.locator(analysisSelector).click();
  await page.waitForFunction(()=>window.__crmOwnerView.getCurrentView()==='marketing'&&document.getElementById('crmMktHome')&&getComputedStyle(document.getElementById('crmMktHome')).display!=='none');
  await assertCurrentView('marketing');
  if(mobile){
   await page.evaluate(()=>{
    document.getElementById('crmOwnerMobileNav').replaceChildren();
    document.dispatchEvent(new CustomEvent('crm:owner-view-change',{detail:{view:'marketing'}}));
   });
   await assertCurrentView('marketing');
   assert.equal(await page.locator('#crmOwnerNavMarketing').count(),1,viewport.width+': mobile navigation not rebuilt after reset');
  }
  await page.keyboard.press('Tab');
  const focusTarget=mobile?'#crmOwnerNavMarketing':'#crmOwnerDesktopSidebar [data-crm-shell-nav="marketing"]';
  await page.locator(focusTarget).focus();
  const focusVisual=await page.locator(focusTarget).evaluate(el=>({
   focusVisible:el.matches(':focus-visible'),
   outlineStyle:getComputedStyle(el).outlineStyle,
   outlineWidth:parseFloat(getComputedStyle(el).outlineWidth)
  }));
  assert.equal(focusVisual.focusVisible,true,viewport.width+': keyboard-focused navigation lacks focus-visible state');
  assert.ok(focusVisual.outlineStyle==='solid'&&focusVisual.outlineWidth>=3,viewport.width+': keyboard focus indicator not visible '+JSON.stringify(focusVisual));
  assert.equal(await page.locator('.crm-period-analytics').count()>0,true,viewport.width+': period analytics missing');
  if(mobile){
   const overflow=await page.evaluate(()=>({scrollWidth:document.documentElement.scrollWidth,clientWidth:document.documentElement.clientWidth}));
   assert.ok(overflow.scrollWidth<=overflow.clientWidth+1,viewport.width+': analysis view causes horizontal page overflow '+JSON.stringify(overflow));
  }
  assert.equal(await page.locator('#crmCsvImportOpenInline').isVisible(),false,viewport.width+': CSV import action must not float over analysis');
  assert.ok((await page.locator('#crmMktHome').innerText()).includes('今月LINE追加'),viewport.width+': monthly LINE additions KPI missing');
  await page.locator('[data-analytics-preset="month"]').click();
  await page.waitForFunction(()=>document.getElementById('crmMktHome')?.innerText.includes('LINE追加 +40%'));
  assert.ok((await page.locator('#crmMktHome').innerText()).includes('LINE追加 +40%'),viewport.width+': period LINE additions KPI missing');
  assert.equal(await page.locator('.crm-approach-queue').count()>0,true,viewport.width+': approach queue missing');
  if(viewport.width===390||viewport.width===1440)await page.screenshot({path:out+'/'+viewport.width+'-canonical-analysis.png',fullPage:true});
  const lineSelector=mobile?'#crmOwnerNavLine':'#crmOwnerDesktopSidebar [data-crm-shell-nav="line"]';
  await page.locator(lineSelector).click();
  await page.waitForFunction(()=>window.__crmOwnerView.getCurrentView()==='line'&&document.getElementById('crmOwnerLineChat')&&getComputedStyle(document.getElementById('crmOwnerLineChat')).display!=='none');
  await assertCurrentView('line');
  await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatCustomers [data-line-customer]').length===101);
  if(mobile){
   const overflow=await page.evaluate(()=>({scrollWidth:document.documentElement.scrollWidth,clientWidth:document.documentElement.clientWidth}));
   assert.ok(overflow.scrollWidth<=overflow.clientWidth+1,viewport.width+': line view causes horizontal page overflow '+JSON.stringify(overflow));
  }
  if(mobile){
   const touch=await page.evaluate(()=>['#crmLineChatSearch','.crm-line-chat-customer'].map(selector=>{const el=document.querySelector(selector);return{selector,height:el?.getBoundingClientRect().height||0}}));
   for(const target of touch)assert.ok(target.height>=44,viewport.width+': LINE control touch target too small '+JSON.stringify(target));
  }
  assert.equal(await page.locator('#lineOpsPanel').isVisible(),false,viewport.width+': old LINE ops opened instead of chat');
  assert.equal(await page.locator('#crmCsvImportOpenInline').isVisible(),false,viewport.width+': CSV import action must not float over LINE');
  await page.locator('#crmLineChatSearch').fill('26999999');
  await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatCustomers [data-line-customer]').length===1);
  assert.equal(await page.locator('#crmLineChatCustomers [data-line-customer="26999999"]').isVisible(),true,viewport.width+': page-2 LINE customer missing from local search');
  if(viewport.width===390){
   await page.evaluate(()=>window.__crmOwnerLineChat.loadCustomers(true));
   assert.equal((await page.locator('#crmLineChatSearch').inputValue()).trim(),'26999999','LINE list refresh cleared search input');
   assert.equal(await page.locator('#crmLineChatCustomers [data-line-customer]').count(),1,'LINE list refresh lost active search filter');
  }
  await page.locator('#crmLineChatSearch').fill('');
  await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatCustomers [data-line-customer]').length===101);
  if(viewport.width===390){
   await page.request.get(origin+'/__fixture/delay-next-line-list');
   const delayedRequest=page.waitForRequest(req=>req.url().includes('/api/customer360/customers?page=1&page_size=100&line=linked'));
   await page.evaluate(()=>{void window.__crmOwnerLineChat.loadCustomers(true)});
   await delayedRequest;
   await page.waitForTimeout(80);
   await page.evaluate(()=>window.__crmOwnerLineChat.loadCustomers(true));
   await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatCustomers [data-line-customer]').length===101);
   await page.waitForTimeout(800);
   assert.equal(await page.locator('#crmLineChatCustomers [data-line-customer]').count(),101,'outdated LINE list replaced newer full results');
   assert.equal(await page.evaluate(()=>window.__crmOwnerLineChat.getState().loading),false,'older LINE list request changed current loading state');
   await page.request.get(origin+'/__fixture/fail-next-line-list');
   await page.evaluate(()=>window.__crmOwnerLineChat.loadCustomers(true));
   const listFailureText=await page.locator('#crmLineChatCustomers').innerText();
   assert.ok(listFailureText.includes('一覧を取得できませんでした'),'LINE list failure lacks user-friendly guidance');
   assert.equal(listFailureText.includes('sensitive_internal_database_failure'),false,'raw LINE list error exposed');
   assert.equal(await page.locator('#crmLineChatListRetry').isVisible(),true,'failed LINE list lacks a visible retry control');
   await page.locator('#crmLineChatListRetry').click();
   await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatCustomers [data-line-customer]').length===101);
   assert.equal(await page.locator('#crmLineChatCustomers [data-line-customer]').count(),101,'LINE list does not recover from retry button');
   await page.request.get(origin+'/__fixture/malformed-next-line-list');
   await page.evaluate(()=>window.__crmOwnerLineChat.loadCustomers(true));
   assert.ok((await page.locator('#crmLineChatCustomers').innerText()).includes('一覧を取得できませんでした'),'malformed LINE list was incorrectly reported as zero linked customers');
   assert.equal(await page.locator('#crmLineChatListRetry').isVisible(),true,'malformed LINE list must show a retry action');
   await page.locator('#crmLineChatListRetry').click();
   await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatCustomers [data-line-customer]').length===101);
   assert.equal(await page.locator('#crmLineChatCustomers [data-line-customer]').count(),101,'LINE list did not recover after malformed payload');
   for(const mode of ['empty','duplicate']){
    const thirdPageBefore=requests.filter(x=>x.includes('/api/customer360/customers?page=3&page_size=100&line=linked')).length;
    await page.request.get(origin+'/__fixture/stall-next-line-list?mode='+mode);
    await page.evaluate(()=>window.__crmOwnerLineChat.loadCustomers(true));
    assert.ok((await page.locator('#crmLineChatCustomers').innerText()).includes('一覧を取得できませんでした'),'stalled LINE list pagination hid fetch error: '+mode);
    assert.equal(await page.locator('#crmLineChatListRetry').isVisible(),true,'stalled LINE list pagination lacks retry: '+mode);
    assert.equal(requests.filter(x=>x.includes('/api/customer360/customers?page=3&page_size=100&line=linked')).length,thirdPageBefore,'stalled LINE pagination sent an unnecessary third request: '+mode);
    await page.locator('#crmLineChatListRetry').click();
    await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatCustomers [data-line-customer]').length===101);
   }
   await page.locator('#crmLineChatCustomers [data-line-customer="26000101"]').focus();
   await page.evaluate(()=>window.__crmOwnerLineChat.loadCustomers(true));
   assert.equal(await page.evaluate(()=>document.activeElement?.getAttribute('data-line-customer')),'26000101','LINE list reload must restore keyboard focus to the same customer');
   assert.equal(await page.locator('#crmLineChatCustomers [data-line-customer]').count(),101,'LINE list reload changed results');
   await page.request.get(origin+'/__fixture/delay-next-line-list');
   const focusReloadRequest=page.waitForRequest(req=>req.url().includes('/api/customer360/customers?page=1&page_size=100&line=linked'));
   await page.locator('#crmLineChatCustomers [data-line-customer="26000101"]').focus();
   await page.evaluate(()=>{void window.__crmOwnerLineChat.loadCustomers(true)});
   await focusReloadRequest;
   await page.locator('#crmLineChatSearch').focus();
   await page.waitForFunction(()=>document.getElementById('crmLineChatCustomers')?.innerText.includes('LINE連携済みの顧客が見つかりません。'));
   assert.equal(await page.evaluate(()=>document.activeElement?.id),'crmLineChatSearch','LINE list reload stole focus from a different control');
   await page.evaluate(()=>window.__crmOwnerLineChat.loadCustomers(true));
   await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatCustomers [data-line-customer]').length===101);
  }
  await page.locator('#crmLineChatCustomers [data-line-customer="26000101"]').click();
  await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatMessages .crm-line-chat-row').length===2);
  if(mobile){
   assert.equal(await page.evaluate(()=>document.activeElement?.id),'crmLineChatBack',viewport.width+': opening conversation must focus visible back control');
   await page.locator('#crmLineChatBack').click();
   assert.equal(await page.locator('#crmLineChatCustomers [data-line-customer="26000101"]').isVisible(),true,viewport.width+': back must expose LINE customer list');
   assert.equal(await page.evaluate(()=>document.activeElement?.getAttribute('data-line-customer')),'26000101',viewport.width+': back must restore focus to selected customer');
   await page.keyboard.press('Enter');
   await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatMessages .crm-line-chat-row').length===2);
   assert.equal(await page.evaluate(()=>document.activeElement?.id),'crmLineChatBack',viewport.width+': keyboard reopening must focus conversation back control');
  }
  if(mobile){
   const conversationControls=await page.evaluate(()=>['#crmLineChatRefresh','#crmLineChatBack'].map(selector=>{const el=document.querySelector(selector);return{selector,height:el?.getBoundingClientRect().height||0}}));
   for(const target of conversationControls)assert.ok(target.height>=44,viewport.width+': LINE conversation control too small '+JSON.stringify(target));
  }
  if(mobile){
   const conversationOverflow=await page.evaluate(()=>({documentScrollWidth:document.documentElement.scrollWidth,bodyScrollWidth:document.body.scrollWidth,viewportWidth:document.documentElement.clientWidth}));
   assert.ok(Math.max(conversationOverflow.documentScrollWidth,conversationOverflow.bodyScrollWidth)<=conversationOverflow.viewportWidth+1,viewport.width+': LINE conversation causes horizontal page overflow '+JSON.stringify(conversationOverflow));
  }
  assert.ok((await page.locator('#crmLineChatMessages').innerText()).includes('七五三の撮影について相談したいです'),viewport.width+': inbound chat text missing');
  assert.ok((await page.locator('#crmLineChatMessages').innerText()).includes('候補日をご案内します'),viewport.width+': outbound chat text missing');
  assert.equal((await page.locator('#crmLineChatStatus').textContent()).trim(),'連携正常',viewport.width+': LINE connected status missing');
  await page.evaluate(()=>window.__crmOwnerLineChat.openConversation('26000102'));
  await page.waitForFunction(()=>document.getElementById('crmLineChatMessages')?.innerText.includes('LINE履歴はまだありません。'));
  await page.evaluate(()=>window.__crmOwnerLineChat.openConversation('26999999'));
  await page.waitForFunction(()=>document.getElementById('crmLineChatMessages')?.innerText.includes('LINE履歴を取得できませんでした。再取得してください。'));
  const lineFailureText=await page.locator('#crmLineChatMessages').innerText();
  assert.equal(/not_found|http\s*404|line_history_unavailable/i.test(lineFailureText),false,viewport.width+': raw LINE error exposed '+lineFailureText);
  await page.evaluate(()=>window.__crmOwnerLineChat.openConversation('26000101'));
  await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatMessages .crm-line-chat-row').length===2);
  if(viewport.width===390){
   for(const shape of ['missing','object','null-item','string-item','array-item']){
    await page.request.get(origin+'/__fixture/malformed-next-line-history?shape='+shape);
    await page.evaluate(()=>window.__crmOwnerLineChat.openConversation('26000101',true));
    assert.ok((await page.locator('#crmLineChatMessages').innerText()).includes('LINE履歴を取得できませんでした。再取得してください。'),'malformed LINE history was misreported as empty: '+shape);
    assert.equal(await page.locator('#crmLineChatMessages .crm-line-chat-row').count(),0,'malformed LINE history rendered fake messages: '+shape);
    assert.equal((await page.locator('#crmLineChatStatus').textContent()).trim(),'要確認','malformed LINE history showed connected status: '+shape);
    await page.locator('#crmLineChatRefresh').click();
    await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatMessages .crm-line-chat-row').length===2);
    assert.equal((await page.locator('#crmLineChatStatus').textContent()).trim(),'連携正常','LINE history did not recover after retry: '+shape);
   }
   await page.request.get(origin+'/__fixture/hold-next-line-avatar');
   const delayedAvatarRequest=page.waitForRequest(req=>req.url().includes('/api/customer360/media/26000101'));
   await page.evaluate(()=>{void window.__crmOwnerLineChat.openConversation('26000101',true)});
   await delayedAvatarRequest;
   await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatMessages .crm-line-chat-row').length===2,null,{timeout:4000});
   assert.equal((await page.locator('#crmLineChatStatus').textContent()).trim(),'連携正常','LINE history waited for unrelated avatar response');
   await page.request.get(origin+'/__fixture/release-line-avatar');
   await page.waitForFunction(()=>!!document.querySelector('#crmLineChatHeadAvatar img[src^="data:image/svg+xml,"]'));
   assert.equal(await page.locator('#crmLineChatMessages .crm-line-chat-row').count(),2,'avatar response erased loaded LINE history');
   await page.request.get(origin+'/__fixture/hold-next-line-avatar');
   const staleAvatarRequest=page.waitForRequest(req=>req.url().includes('/api/customer360/media/26000101'));
   await page.evaluate(()=>{void window.__crmOwnerLineChat.openConversation('26000101',true)});
   await staleAvatarRequest;
   await page.evaluate(()=>window.__crmOwnerLineChat.openConversation('26000102'));
   await page.waitForFunction(()=>document.getElementById('crmLineChatMessages')?.innerText.includes('LINE履歴はまだありません。'));
   await page.request.get(origin+'/__fixture/release-line-avatar');
   await page.waitForTimeout(120);
   assert.equal(await page.locator('#crmLineChatHeadAvatar img').count(),0,'old customer avatar replaced selected customer');
   assert.equal((await page.locator('#crmLineChatTitle').textContent()).trim(),'佐藤 未来','old media response changed LINE customer');
   await page.evaluate(()=>window.__crmOwnerLineChat.openConversation('26000101'));
   await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatMessages .crm-line-chat-row').length===2);
   await page.request.get(origin+'/__fixture/delay-next-line-history');
   await page.evaluate(()=>{void window.__crmOwnerLineChat.openConversation('26000101',true)});
   await page.waitForFunction(()=>document.getElementById('crmLineChatStatus')?.textContent==='取得中');
   await page.evaluate(()=>window.__crmOwnerLineChat.openConversation('26000102'));
   await page.waitForFunction(()=>document.getElementById('crmLineChatMessages')?.innerText.includes('LINE履歴はまだありません。'));
   await page.waitForTimeout(800);
   assert.equal((await page.locator('#crmLineChatTitle').textContent()).trim(),'佐藤 未来','slow previous LINE history replaced selected customer title');
   assert.ok((await page.locator('#crmLineChatMessages').innerText()).includes('LINE履歴はまだありません。'),'slow previous LINE history replaced selected customer messages');
   assert.equal((await page.locator('#crmLineChatStatus').textContent()).trim(),'履歴未確認','slow previous LINE history replaced selected customer status');
   await page.evaluate(()=>window.__crmOwnerLineChat.openConversation('26000101'));
   await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatMessages .crm-line-chat-row').length===2);
  }
  if(mobile){
   const navState=await page.evaluate(()=>({
    labels:(document.getElementById('crmOwnerMobileNav')?.innerText||'').replace(/\\s+/g,''),
    canonicalUix:document.getElementById('crmOwnerMobileNav')?.dataset.canonicalUix||'',
    legacyPreempted:Array.isArray(window.__crmCanonicalLegacyOwnerPreempted)&&window.__crmCanonicalLegacyOwnerPreempted.length>0
   }));
   assert.equal(navState.labels.includes('今日'),false,viewport.width+': legacy Today navigation leaked');
   assert.ok(navState.canonicalUix,viewport.width+': canonical mobile nav marker missing');
   assert.equal(navState.legacyPreempted,true,viewport.width+': legacy shell owners were not preempted before boot');
  }
  if(viewport.width===390||viewport.width===1440)await page.screenshot({path:out+'/'+viewport.width+'-canonical-line.png',fullPage:true});
  if(errors.length)console.log('HTTP_404_DIAGNOSTIC='+JSON.stringify(notFoundRequests));
  assert.equal(errors.length,0,viewport.width+': console/page errors '+errors.join(' | ')+' / 404='+notFoundRequests.join(' | '));
  await context.close();
 }
 assert.equal(requests.some(x=>!x.startsWith('GET ')&&!x.startsWith('HEAD ')),false,'unexpected HTTP write '+requests.join(' | '));
 assert.equal(requests.some(x=>x.includes('/api/customer360/customers?page=2&page_size=100&line=linked')),true,'LINE linked-customer pagination did not request page 2');
 console.log('CRM_CANONICAL_UIX_RESPONSIVE_BROWSER=PASS');
 console.log('TODAY_VISIBLE_UI=0');
 console.log('ANALYSIS_TAB=PASS');
 console.log('LINE_CHAT_HISTORY=PASS');
 console.log('LINE_CHAT_EMPTY_STATE=PASS');
 console.log('LINE_CHAT_FRIENDLY_ERROR=PASS');
 console.log('CRM_GOTHIC_FONT=PASS');
 console.log('CUSTOMER_AVATAR_UI=PASS');
 console.log('LEGACY_LINE_OPS_VISIBLE=0');
 console.log('DESKTOP_SMARTPHONE_COLLAPSE=0');
 console.log('LEGACY_MOBILE_NAV_OVERRIDE=0');
 for(const f of ['390-canonical-customer-list.png','390-canonical-analysis.png','390-canonical-line.png','1440-canonical-customer-list.png','1440-canonical-analysis.png','1440-canonical-line.png'])assert.ok(fs.existsSync(out+'/'+f),'canonical screenshot missing '+f);
 console.log('CANONICAL_SCREENSHOTS=6');
 console.log('CANONICAL_MOBILE_WIDTHS=320,360,390,412,767');
 console.log('CANONICAL_DESKTOP_WIDTHS=768,1024,1440');
 console.log('CANONICAL_NAV_BREAKPOINT=767_MOBILE_768_DESKTOP');
 console.log('HTTP_WRITES=0');
}finally{
 await browser.close();
 await new Promise(r=>server.close(r));
}
