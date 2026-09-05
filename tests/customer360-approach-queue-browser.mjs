import assert from 'node:assert/strict';
import http from 'node:http';
import { chromium } from 'playwright';
import { injectCustomer360Marketing } from '../src/crm-customer360-ui.mjs';

const base='<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><style>html,body{margin:0;overflow-x:hidden}.app{padding:12px}</style></head><body><main class="app"><h1>CRM</h1></main></body></html>';
const html=injectCustomer360Marketing(base);
const requests=[],writes=[];
const customer={customer_id:'26000001',name:'山田 花子',line_linked:true,realized_ltv:150000,shoot_count:3,last_shoot_date:'2026-06-01',family_summary:'子1人',area_summary:'大阪',recommendation:{priority_score:1080,next_offer:'七五三 + 家族写真'},next_opportunity:{type:'shichigosan',label:'七五三',days:20}};
const facets={prefectures:[],cities:[],genres:[],sources:[],campaigns:[],school_stages:[]};
const queueItem={customer_id:'26000001',name:'山田 花子',priority_score:1080,priority_reason:'high_ltv+event',priority_reason_label:'高LTV + 家族イベント',next_offer:'七五三 + 家族写真',next_opportunity:{type:'shichigosan',label:'七五三',days:20,member_name:'太郎'},contact:{code:'manual_contact_ready',label:'手動連絡候補',ready:true,review_required:false,suggested_channel:'LINE',available:{line:true,phone:true,email:false}},draft_text:'山田 花子様、七五三の時期が近づいてきました。',marketing_classes:['VIP'],realized_ltv:150000,shoot_count:3,last_shoot_date:'2026-06-01'};
function send(res,status,data,type='application/json; charset=utf-8'){res.writeHead(status,{'content-type':type,'cache-control':'no-store'});res.end(type.startsWith('application/json')?JSON.stringify(data):data)}
const server=http.createServer((req,res)=>{const u=new URL(req.url,'http://127.0.0.1');requests.push(req.method+' '+u.pathname+u.search);if(!['GET','HEAD'].includes(req.method))writes.push(req.method+' '+u.pathname);if(u.pathname==='/'||u.pathname==='/admin')return send(res,200,html,'text/html; charset=utf-8');if(u.pathname==='/api/customer360/marketing-home')return send(res,200,{ok:true,kpis:{customers:1,average_realized_ltv:150000,repeat_rate_pct:100,vip_high_ltv:1,event_90d:1,dormant_180:0,line_link_rate_pct:100,approach_this_month:1},top_opportunities:[customer],facets});if(u.pathname==='/api/customer360/customers')return send(res,200,{ok:true,total:1,all_total:1,page:1,page_size:50,has_next:false,items:[customer],facets,meta:{privacy_safe_list_dto:true}});if(u.pathname==='/api/customer360/approach-queue')return send(res,200,{ok:true,items:[queueItem],total:1,summary:{total:1,ready:1,review_required:0,opted_out:0,no_contact:0},filters:{horizon_days:Number(u.searchParams.get('horizon_days')||90),limit:50,status:u.searchParams.get('status')||'all'},meta:{read_only:true,line_send:false,automatic_contact:false,contact_details_exposed:false}});if(u.pathname.startsWith('/api/customer360/customer/'))return send(res,200,{ok:true,customer:{...customer,address:{},family:[],opportunities:[],reservations:[],line_history:[],marketing_history:[],marketing_classes:[],consent:{},recommendation:{}}});return send(res,404,{ok:false})});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({headless:true});
try{
  for(const viewport of [{width:390,height:844},{width:1440,height:900}]){
    const context=await browser.newContext({viewport});
    const page=await context.newPage();
    const queueBefore=requests.filter(x=>x.includes('/api/customer360/approach-queue')).length;
    await page.goto(origin+'/admin',{waitUntil:'domcontentloaded'});
    await page.locator('#crmMktNav [data-view="home"]').click();
    await page.waitForSelector('#crmApproachLoad');
    assert.equal(await page.locator('#crmMktHome > table.crm-mkt-table').count(),0,'consent-blind duplicate opportunity table must be removed');
    assert.equal(requests.filter(x=>x.includes('/api/customer360/approach-queue')).length,queueBefore,'approach queue must not auto-fetch');
    await page.locator('#crmApproachLoad').click();
    await page.waitForFunction(()=>document.querySelector('.crm-approach-row')?.textContent.includes('手動連絡候補'));
    const txt=await page.locator('.crm-approach-queue').innerText();
    assert.ok(txt.includes('高LTV + 家族イベント'));
    assert.ok(txt.includes('七五三 + 家族写真'));
    assert.ok(txt.includes('候補チャネル LINE'));
    assert.ok(txt.includes('文案を見る'));
    assert.ok(!txt.includes('090'));
    await page.locator('.crm-approach-draft summary').click();
    assert.ok((await page.locator('.crm-approach-draft').innerText()).includes('自動送信しません'));
    await page.locator('[data-approach-horizon="30"]').click();
    await page.waitForTimeout(50);
    assert.ok(requests.some(x=>x.includes('horizon_days=30')));
    const overflow=await page.evaluate(()=>document.documentElement.scrollWidth-document.documentElement.clientWidth);
    assert.ok(overflow<=1,'overflow '+viewport.width+' '+overflow);
    await context.close();
  }
  assert.equal(writes.length,0,'HTTP writes '+writes.join(','));
  console.log('CUSTOMER360_APPROACH_QUEUE_BROWSER=PASS');
  console.log('MARKETING_HOME_DUPLICATE_OPPORTUNITY_TABLE=0');
  console.log('APPROACH_QUEUE_AUTO_FETCH=0');
  console.log('APPROACH_QUEUE_HTTP_WRITES=0');
  console.log('APPROACH_QUEUE_390_1440_OVERFLOW=0');
} finally {
  await browser.close();
  await new Promise(r=>server.close(r));
}
