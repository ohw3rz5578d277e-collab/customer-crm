import assert from 'node:assert/strict';
import http from 'node:http';
import { chromium } from 'playwright';
import { injectCustomer360Marketing } from '../src/crm-customer360-ui.mjs';

const base='<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><style>html,body{margin:0;overflow-x:hidden}.app{padding:12px}</style></head><body><main class="app"><h1>CRM</h1></main></body></html>';
const html=injectCustomer360Marketing(base);
const requests=[],writes=[];
const item={customer_id:'26000001',name:'山田 花子',line_linked:true,realized_ltv:50000,shoot_count:2,family_summary:'子1人',last_shoot_date:'2026-08-20',area_summary:'大阪',recommendation:{next_offer:'七五三'},next_opportunity:{label:'七五三',days:20}};
const facets={prefectures:[],cities:[],genres:[],sources:[],campaigns:[],school_stages:[]};
function send(res,status,data,type='application/json; charset=utf-8'){res.writeHead(status,{'content-type':type,'cache-control':'no-store'});res.end(type.startsWith('application/json')?JSON.stringify(data):data)}
function analytics(u){
  const from=u.searchParams.get('from')||'2026-09-01',to=u.searchParams.get('to')||'2026-09-05';
  return {ok:true,available:true,period:{from,to,as_of:'2026-09-05',span_days:5,previous:{from:'2026-08-27',to:'2026-08-31'}},current:{from,to,revenue:45000,completed_shoots:3,unique_customers:2,average_order_value:15000,repeat_customers_in_period:1,repeat_rate_pct:50,genres:[{genre:'七五三',shoots:2,revenue:30000,unique_customers:1},{genre:'お宮参り',shoots:1,revenue:15000,unique_customers:1}],monthly:[{month:from.slice(0,7),shoots:3,revenue:45000,unique_customers:2}]},previous:{revenue:30000,completed_shoots:2,unique_customers:2,average_order_value:15000},change_pct:{revenue:50,completed_shoots:50,unique_customers:0,average_order_value:0},meta:{read_only:true,identity_key:'customer_id',customer_id_generation:false,customer_write:false,line_send:false}};
}
const server=http.createServer((req,res)=>{const u=new URL(req.url,'http://127.0.0.1');requests.push(req.method+' '+u.pathname+u.search);if(!['GET','HEAD'].includes(req.method))writes.push(req.method+' '+u.pathname);if(u.pathname==='/'||u.pathname==='/admin')return send(res,200,html,'text/html; charset=utf-8');if(u.pathname==='/api/customer360/marketing-home')return send(res,200,{ok:true,kpis:{customers:1,average_realized_ltv:50000,repeat_rate_pct:100,vip_high_ltv:0,event_90d:1,dormant_180:0,line_link_rate_pct:100,approach_this_month:1},top_opportunities:[item],facets});if(u.pathname==='/api/customer360/analytics')return send(res,200,analytics(u));if(u.pathname==='/api/customer360/customers')return send(res,200,{ok:true,total:1,all_total:1,page:1,page_size:50,has_next:false,items:[item],facets,meta:{privacy_safe_list_dto:true}});if(u.pathname.startsWith('/api/customer360/customer/'))return send(res,200,{ok:true,customer:{...item,address:{},family:[],opportunities:[],reservations:[],line_history:[],marketing_history:[],marketing_classes:[],consent:{},recommendation:{}}});return send(res,404,{ok:false})});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({headless:true});
try{
  for(const viewport of [{width:390,height:844},{width:1440,height:900}]){
    const context=await browser.newContext({viewport});
    const page=await context.newPage();
    const analyticsBefore=requests.filter(x=>x.includes('/api/customer360/analytics')).length;
    await page.goto(origin+'/admin',{waitUntil:'domcontentloaded'});
    await page.locator('#crmMktNav [data-view="home"]').click();
    await page.waitForSelector('#crmAnalyticsApply');
    await page.waitForFunction(()=>document.querySelector('#crmMktHome')?.textContent.includes('期間分析'));
    assert.equal(requests.filter(x=>x.includes('/api/customer360/analytics')).length,analyticsBefore,'analytics must not auto-fetch');
    assert.equal(await page.locator('#crmAnalyticsFrom').inputValue(),'2026-09-01');
    assert.equal(await page.locator('#crmAnalyticsTo').inputValue(),'2026-09-05');
    await page.locator('#crmAnalyticsApply').click();
    await page.waitForFunction(()=>document.querySelector('#crmMktHome')?.textContent.includes('¥45,000'));
    assert.ok((await page.locator('#crmMktHome').innerText()).includes('期間内リピート率'));
    await page.locator('[data-analytics-preset="prev"]').click();
    await page.waitForTimeout(50);
    assert.ok(requests.some(x=>x.includes('/api/customer360/analytics?from=')),'preset analytics request missing');
    await page.locator('#crmAnalyticsFrom').fill('2026-08-01');
    await page.locator('#crmAnalyticsTo').fill('2026-08-31');
    await page.locator('#crmAnalyticsApply').click();
    await page.waitForFunction(()=>document.querySelector('#crmAnalyticsFrom')?.value==='2026-08-01');
    const overflow=await page.evaluate(()=>document.documentElement.scrollWidth-document.documentElement.clientWidth);
    assert.ok(overflow<=1,'horizontal overflow '+viewport.width+' '+overflow);
    await context.close();
  }
  assert.equal(writes.length,0,'HTTP writes '+writes.join(','));
  console.log('CUSTOMER360_PERIOD_ANALYTICS_BROWSER=PASS');
  console.log('PERIOD_ANALYTICS_HTTP_WRITES=0');
  console.log('PERIOD_ANALYTICS_390_1440_OVERFLOW=0');
} finally {
  await browser.close();
  await new Promise(r=>server.close(r));
}
