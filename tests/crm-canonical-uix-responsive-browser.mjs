import assert from 'node:assert/strict';
import http from 'node:http';
import { chromium } from 'playwright';
import { composeCustomer360AdminHtml } from '../src/production-index-crm-customer360-entry.js';

const customers=[
 {customer_id:'26000101',name:'山田 花子',line_display_name:'はなこ',line_linked:true,realized_ltv:128000,shoot_count:3,last_shoot_date:'2026-08-20',recommendation:{next_offer:'七五三'},next_opportunity:{label:'七五三',days:30}},
 {customer_id:'26000102',name:'佐藤 未来',line_display_name:'みらい',line_linked:true,realized_ltv:35000,shoot_count:1,last_shoot_date:'2026-06-10',recommendation:{next_offer:'家族写真'},next_opportunity:{label:'誕生日',days:60}}
];
const facets={prefectures:['大阪府'],cities:['大阪市'],genres:['七五三'],sources:['Instagram'],campaigns:[],school_stages:[]};
const base=`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><style>html,body{margin:0}.app{width:100%;box-sizing:border-box;padding:16px}#lineOpsPanel{display:none;position:fixed;inset:20px;background:#fff;z-index:9999}#lineOpsPanel.open{display:block}</style></head><body><button id="lineOpsOpen">旧LINE</button><section id="lineOpsPanel"><h2>LINE送信・反応管理</h2></section><main class="app"><h1>顧客管理</h1><section id="crmTodayDashboard"><h2>今日やること</h2></section></main><script>document.getElementById('lineOpsOpen').onclick=()=>document.getElementById('lineOpsPanel').classList.add('open')</script></body></html>`;
const html=composeCustomer360AdminHtml(base);
assert.equal(html.includes('const navObserver=new MutationObserver'),false,'canonical shell must not install a permanent document-wide nav observer');
const requests=[];
function send(res,status,data,type='application/json; charset=utf-8'){res.writeHead(status,{'content-type':type,'cache-control':'no-store'});res.end(type.startsWith('application/json')?JSON.stringify(data):data)}
const server=http.createServer((req,res)=>{
 const u=new URL(req.url,'http://127.0.0.1');requests.push(req.method+' '+u.pathname+u.search);
 if(u.pathname==='/'||u.pathname==='/admin')return send(res,200,html,'text/html; charset=utf-8');
 if(u.pathname==='/api/customer360/marketing-home')return send(res,200,{ok:true,kpis:{customers:2,average_realized_ltv:81500,repeat_rate_pct:50,vip_high_ltv:1,event_90d:2,dormant_180:0,line_link_rate_pct:100,approach_this_month:2},top_opportunities:customers,facets});
 if(u.pathname==='/api/customer360/customers')return send(res,200,{ok:true,total:2,all_total:2,page:1,page_size:100,has_next:false,items:customers,facets,meta:{privacy_safe_list_dto:true}});
 if(u.pathname==='/api/customer360/analytics')return send(res,200,{ok:true,available:true,period:{from:'2026-09-01',to:'2026-09-30',span_days:30},current:{revenue:163000,completed_shoots:4,unique_customers:2,average_order_value:40750,repeat_customers_in_period:1,repeat_rate_pct:50,genres:[],monthly:[]},change_pct:{}});
 if(u.pathname==='/api/customer360/approach-queue')return send(res,200,{ok:true,items:[],summary:{total:0,ready:0,review_required:0,opted_out:0}});
 if(u.pathname==='/api/customers/26000101/line-history')return send(res,200,{ok:true,connected:true,count:2,messages:[{direction:'inbound',message_text:'七五三の撮影について相談したいです',sent_at:'2026-09-14 10:00'},{direction:'outbound',message_text:'ありがとうございます。候補日をご案内します。',sent_at:'2026-09-14 10:03'}]});
 if(u.pathname==='/api/customers/26000102/line-history')return send(res,200,{ok:true,connected:true,count:0,messages:[]});
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
  assert.ok((await page.locator('#crmOwnerWorkspaceTitle').textContent()).includes('顧客'),viewport.width+': Customer must be initial view');
  const overflow=await page.evaluate(()=>Math.max(document.documentElement.scrollWidth-document.documentElement.clientWidth,document.body.scrollWidth-document.body.clientWidth));
  assert.ok(overflow<=1,viewport.width+': horizontal overflow '+overflow);
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
  const analysisSelector=mobile?'#crmOwnerNavMarketing':'#crmOwnerDesktopSidebar [data-crm-shell-nav="marketing"]';
  await page.locator(analysisSelector).click();
  await page.waitForFunction(()=>window.__crmOwnerView.getCurrentView()==='marketing'&&document.getElementById('crmMktHome')&&getComputedStyle(document.getElementById('crmMktHome')).display!=='none');
  assert.equal(await page.locator('.crm-period-analytics').count()>0,true,viewport.width+': period analytics missing');
  assert.equal(await page.locator('.crm-approach-queue').count()>0,true,viewport.width+': approach queue missing');
  const lineSelector=mobile?'#crmOwnerNavLine':'#crmOwnerDesktopSidebar [data-crm-shell-nav="line"]';
  await page.locator(lineSelector).click();
  await page.waitForFunction(()=>window.__crmOwnerView.getCurrentView()==='line'&&document.getElementById('crmOwnerLineChat')&&getComputedStyle(document.getElementById('crmOwnerLineChat')).display!=='none');
  await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatCustomers [data-line-customer]').length===2);
  assert.equal(await page.locator('#lineOpsPanel').isVisible(),false,viewport.width+': old LINE ops opened instead of chat');
  await page.locator('#crmLineChatCustomers [data-line-customer="26000101"]').click();
  await page.waitForFunction(()=>document.querySelectorAll('#crmLineChatMessages .crm-line-chat-row').length===2);
  assert.ok((await page.locator('#crmLineChatMessages').innerText()).includes('七五三の撮影について相談したいです'),viewport.width+': inbound chat text missing');
  assert.ok((await page.locator('#crmLineChatMessages').innerText()).includes('候補日をご案内します'),viewport.width+': outbound chat text missing');
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
  assert.equal(errors.length,0,viewport.width+': console/page errors '+errors.join(' | '));
  await context.close();
 }
 assert.equal(requests.some(x=>!x.startsWith('GET ')&&!x.startsWith('HEAD ')),false,'unexpected HTTP write '+requests.join(' | '));
 console.log('CRM_CANONICAL_UIX_RESPONSIVE_BROWSER=PASS');
 console.log('TODAY_VISIBLE_UI=0');
 console.log('ANALYSIS_TAB=PASS');
 console.log('LINE_CHAT_HISTORY=PASS');
 console.log('LEGACY_LINE_OPS_VISIBLE=0');
 console.log('DESKTOP_SMARTPHONE_COLLAPSE=0');
 console.log('LEGACY_MOBILE_NAV_OVERRIDE=0');
 console.log('HTTP_WRITES=0');
}finally{
 await browser.close();
 await new Promise(r=>server.close(r));
}
