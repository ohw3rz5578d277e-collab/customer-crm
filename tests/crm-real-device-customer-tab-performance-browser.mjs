import http from 'node:http';
import { chromium } from 'playwright';
import { composeCustomer360AdminHtml } from '../src/production-index-crm-customer360-entry.js';

function assert(ok,msg){if(!ok)throw new Error(msg)}
const customer={customer_id:'26000101',name:'山田 花子',line_linked:true,realized_ltv:128000,family_summary:'子1人',child_count:1,last_shoot_date:'2026-05-01',area_summary:'大阪府 豊中市',recommendation:{next_offer:'七五三'},next_opportunity:{label:'七五三',days:30},shoot_count:3};
const facets={prefectures:[],cities:[],genres:[],sources:[],campaigns:[],school_stages:[]};
const kpis={customers:1,average_realized_ltv:128000,repeat_rate_pct:100,vip_high_ltv:1,event_90d:1,dormant_180:0,line_link_rate_pct:100,approach_this_month:1};
const base=`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><style>html,body{margin:0;overflow-x:hidden}.app{padding:12px}.legacy{padding:16px;margin:12px 0;background:#fff;border:1px solid #ddd}.crm-growth-panel{display:block}</style></head><body><main class="app"><section id="crmTodayDashboard" class="crm-today-dash legacy"><h2>今日</h2></section></main><script>
window.addEventListener('DOMContentLoaded',()=>{
  setTimeout(()=>{const g=document.createElement('section');g.id='crmGrowthPanel';g.className='crm-growth-panel legacy';g.innerHTML='<h2>総合ダッシュボード</h2><div>VIP</div><div>リピーター</div><div>候補</div><div>休眠</div><div>今月売上</div>';document.body.appendChild(g)},120);
  setTimeout(()=>{const d=document.createElement('section');d.id='crmDeliveryDeadlinePanel';d.className='crm-delivery-panel legacy';d.innerHTML='<h2>納品</h2>';document.body.appendChild(d)},180);
});
</script></body></html>`;
const html=composeCustomer360AdminHtml(base);
let customerGets=0,totalFetches=0;
const server=http.createServer((req,res)=>{const u=new URL(req.url,'http://127.0.0.1');if(req.method==='GET')totalFetches++;res.setHeader('cache-control','no-store');if(u.pathname==='/'||u.pathname==='/admin'){res.setHeader('content-type','text/html; charset=utf-8');return res.end(html)}res.setHeader('content-type','application/json; charset=utf-8');if(u.pathname==='/api/customer360/marketing-home')return res.end(JSON.stringify({ok:true,kpis,top_opportunities:[customer],facets}));if(u.pathname==='/api/customer360/customers'){customerGets++;return res.end(JSON.stringify({ok:true,total:1,all_total:1,page:1,page_size:50,has_next:false,items:[customer],facets,meta:{privacy_safe_list_dto:true}}))}if(u.pathname.startsWith('/api/customer360/customer/'))return res.end(JSON.stringify({ok:true,customer:{...customer,address:{},family:[],opportunities:[],reservations:[],line_history:[],marketing_history:[],marketing_classes:[],consent:{},recommendation:{}}}));res.statusCode=404;res.end(JSON.stringify({ok:false,error:'not_found'}))});
await new Promise(r=>server.listen(0,'127.0.0.1',r));const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({headless:true});
try{
 const context=await browser.newContext({viewport:{width:390,height:844},screen:{width:390,height:844},isMobile:true,hasTouch:true});
 await context.addInitScript(()=>{
   window.__ownerMutationCallbacks=0;window.__ownerMutationRecords=0;
   const Native=window.MutationObserver;
   window.MutationObserver=class extends Native{constructor(cb){super((records,obs)=>{window.__ownerMutationCallbacks++;window.__ownerMutationRecords+=records.length;return cb(records,obs)})}};
 });
 const page=await context.newPage();const consoleErrors=[],pageErrors=[];page.on('console',m=>{if(m.type()==='error')consoleErrors.push(m.text())});page.on('pageerror',e=>pageErrors.push(String(e)));
 const start=Date.now();await page.goto(origin+'/admin',{waitUntil:'domcontentloaded'});await page.waitForFunction(()=>window.__crmOwnerView&&document.querySelector('#crmOwnerNavCustomers')&&document.querySelector('#crmMktList tbody tr'));const interactive=Date.now()-start;
 await page.waitForTimeout(250);
 const beforeGets=customerGets,beforeFetch=totalFetches;const tapStart=Date.now();await page.locator('#crmOwnerNavCustomers').click();await page.waitForFunction(()=>document.body.dataset.crmOwnerView==='customers'&&document.querySelector('#crmOwnerNavCustomers')?.classList.contains('active'));await page.locator('#crmMktList.open').waitFor();const tapMs=Date.now()-tapStart;
 const snapshot=await page.evaluate(()=>{const vis=e=>!!e&&getComputedStyle(e).display!=='none'&&getComputedStyle(e).visibility!=='hidden'&&e.getClientRects().length>0;return{navActive:document.querySelector('#crmOwnerNavCustomers')?.classList.contains('active')?1:0,listVisible:vis(document.querySelector('#crmMktList'))?1:0,headingVisible:vis(document.querySelector('#crmOwnerCustomerTitle'))?1:0,searchVisible:vis(document.querySelector('#crmGlobalSearch'))?1:0,dashboardVisible:vis(document.querySelector('#crmGrowthPanel'))?1:0,todayVisible:vis(document.querySelector('#crmTodayDashboard'))?1:0,deliveryVisible:vis(document.querySelector('#crmDeliveryDeadlinePanel'))?1:0,callbacks:window.__ownerMutationCallbacks,records:window.__ownerMutationRecords,nodes:document.getElementsByTagName('*').length,overflow:Math.max(document.documentElement.scrollWidth-document.documentElement.clientWidth,document.body.scrollWidth-document.body.clientWidth)}});
 console.log('BASE_NAV_ACTIVE='+snapshot.navActive);console.log('BASE_CUSTOMER_EXCLUSIVE='+(snapshot.dashboardVisible===0&&snapshot.todayVisible===0&&snapshot.deliveryVisible===0?1:0));console.log('BASE_DASHBOARD_VISIBLE='+snapshot.dashboardVisible);console.log('BASE_CUSTOMER_TAP_TO_VISIBLE_MS='+tapMs);console.log('BASE_OWNER_NAV_INTERACTIVE_MS='+interactive);console.log('BASE_CUSTOMER_API_GET_PER_TAP='+(customerGets-beforeGets));console.log('BASE_TOTAL_FETCH_DELTA_PER_TAP='+(totalFetches-beforeFetch));console.log('BASE_MUTATION_CALLBACKS='+snapshot.callbacks);console.log('BASE_MUTATION_RECORDS='+snapshot.records);console.log('BASE_DOM_NODES='+snapshot.nodes);
 assert(snapshot.navActive===1,'BASE RED invalid: Customer nav did not become active');
 assert(snapshot.listVisible===1,'BASE RED invalid: Customer list not visible');
 assert(snapshot.dashboardVisible===0,'REAL_DEVICE_FALSE_POSITIVE_REPRODUCED: nav active but #crmGrowthPanel / 総合ダッシュボード remains visible');
 assert(snapshot.todayVisible===0&&snapshot.deliveryVisible===0,'operational surfaces visible');
 assert(snapshot.headingVisible===1&&snapshot.searchVisible===1,'Customer primary controls not visible');
 assert(tapMs<=250,'Customer state switch too slow '+tapMs+'ms');
 assert(customerGets-beforeGets===0,'one tab tap caused duplicate Customer list fetch');
 assert(snapshot.overflow<=1,'horizontal overflow '+snapshot.overflow);
 assert(consoleErrors.length===0,'console errors '+consoleErrors.join(' | '));assert(pageErrors.length===0,'page errors '+pageErrors.join(' | '));
 await page.waitForTimeout(2000);assert(!(await page.locator('#crmGrowthPanel').isVisible()),'2-second stability failure');await page.waitForTimeout(3000);assert(!(await page.locator('#crmGrowthPanel').isVisible()),'5-second stability failure');
 await context.close();
} finally {await browser.close();server.close()}
