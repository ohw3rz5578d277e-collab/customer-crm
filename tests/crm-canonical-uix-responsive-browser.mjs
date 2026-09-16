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
const requests=[];
function send(res,status,data,type='application/json; charset=utf-8'){res.writeHead(status,{'content-type':type,'cache-control':'no-store'});res.end(type.startsWith('application/json')?JSON.stringify(data):data)}
const server=http.createServer((req,res)=>{
 const u=new URL(req.url,'http://127.0.0.1');requests.push(req.method+' '+u.pathname+u.search);
 if(u.pathname==='/'||u.pathname==='/admin')return send(res,200,html,'text/html; charset=utf-8');
 if(u.pathname==='/api/customer360/marketing-home')return send(res,200,{ok:true,kpis:{customers:2,average_realized_ltv:81500,repeat_rate_pct:50,vip_high_ltv:1,event_90d:2,dormant_180:0,line_link_rate_pct:100,line_additions_this_month:7,approach_this_month:2},top_opportunities:customers,facets});
 if(u.pathname==='/api/customer360/customers'){
  if(u.searchParams.get('line')==='linked'){
   const page=Number(u.searchParams.get('page')||1);
   if(page===1)return send(res,200,{ok:true,total:101,all_total:101,page:1,page_size:100,has_next:true,items:linePage1,facets,meta:{privacy_safe_list_dto:true}});
   if(page===2)return send(res,200,{ok:true,total:101,all_total:101,page:2,page_size:100,has_next:false,items:[linePage2Customer],facets,meta:{privacy_safe_list_dto:true}});
  }
  return send(res,200,{ok:true,total:2,all_total:2,page:1,page_size:100,has_next:false,items:customers,facets,meta:{privacy_safe_list_dto:true}});
 }
 if(u.pathname==='/api/customer360/analytics')return send(res,200,{ok:true,available:true,period:{from:'2026-09-01',to:'2026-09-30',span_days:30},current:{revenue:163000,completed_shoots:4,unique_customers:2,average_order_value:40750,repeat_customers_in_period:1,repeat_rate_pct:50,line_additions:7,genres:[],monthly:[]},previous:{line_additions:5},change_pct:{line_additions:40}});
 if(u.pathname==='/api/customer360/approach-queue')return send(res,200,{ok:true,items:[],summary:{total:0,ready:0,review_required:0,opted_out:0}});
 if(u.pathname==='/api/customers/26000101/line-history')return send(res,200,{ok:true,connected:true,count:2,messages:[{direction:'inbound',message_text:'七五三の撮影について相談したいです',sent_at:'2026-09-14 10:00'},{direction:'outbound',message_text:'ありがとうございます。候補日をご案内します。',sent_at:'2026-09-14 10:03'}]});
 if(u.pathname==='/api/customers/26000102/line-history')return send(res,200,{ok:true,connected:true,count:0,messages:[]});
 if(u.pathname.startsWith('/api/customer360/media/')){const id=decodeURIComponent(u.pathname.split('/').pop());return send(res,200,{ok:true,media:{customer_id:id,avatar_data_url:'',avatar_updated_at:'',latest_delivery_link:null,delivery_links:[]}})}
 if(u.pathname.startsWith('/api/customer360/customer/')){const id=decodeURIComponent(u.pathname.split('/').pop());const c=customers.find(x=>x.customer_id===id)||customers[0];return send(res,200,{ok:true,customer:{...c,address:{},family:[],opportunities:[],reservations:[],line_history:[],marketing_history:[],consent:{},raw:{}}})}
 return send(res,404,{ok:false,error:'not_found'});
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({headless:true});
try{
 for(const viewport of [{width:390,height:844},{width:412,height:915},{width:1024,height:768},{width:1440,height:900}]){
  const mobile=viewport.width<=767;
  const context=await browser.newContext({viewport,hasTouch:mobile,isMobile:mobile});
  const page=await context.newPage(),errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  page.on('console',m=>{if(m.type()==='error')errors.push(m.text())});
  await page.goto(origin+'/admin',{waitUntil:'domcontentloaded'});
  await page.waitForFunction(()=>window.__crmOwnerView&&document.getElementById('crmOwnerAppShell')&&document.querySelector('#crmMktList tbody tr'));
  await page.waitForFunction(()=>window.__crmOwnerView.getCurrentView()==='customers');
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
  assert.equal(/serif|mincho/.test(fontFamily),false,viewport.width+': serif/mincho font leaked '+fontFamily);
  assert.ok(fontFamily.includes('sans-serif'),viewport.width+': canonical sans stack missing '+fontFamily);
  const firstCustomer=page.locator('#crmMktList [data-open],#crmMktList [data-direct-customer]').first();
  await firstCustomer.click();
  await page.locator('#crmMktDetail.open').waitFor();
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
  assert.equal(await page.locator('.crm-period-analytics').count()>0,true,viewport.width+': period analytics missing');
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
  await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatCustomers [data-line-customer]').length===101);
  assert.equal(await page.locator('#lineOpsPanel').isVisible(),false,viewport.width+': old LINE ops opened instead of chat');
  assert.equal(await page.locator('#crmCsvImportOpenInline').isVisible(),false,viewport.width+': CSV import action must not float over LINE');
  await page.locator('#crmLineChatSearch').fill('26999999');
  await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatCustomers [data-line-customer]').length===1);
  assert.equal(await page.locator('#crmLineChatCustomers [data-line-customer="26999999"]').isVisible(),true,viewport.width+': page-2 LINE customer missing from local search');
  await page.locator('#crmLineChatSearch').fill('');
  await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatCustomers [data-line-customer]').length===101);
  await page.locator('#crmLineChatCustomers [data-line-customer="26000101"]').click();
  await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatMessages .crm-line-chat-row').length===2);
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
  assert.equal(errors.length,0,viewport.width+': console/page errors '+errors.join(' | '));
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
 console.log('HTTP_WRITES=0');
}finally{
 await browser.close();
 await new Promise(r=>server.close(r));
}
