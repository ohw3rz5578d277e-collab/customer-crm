import assert from 'node:assert/strict';
import http from 'node:http';
import { chromium } from 'playwright';
import { composeCustomer360AdminHtml } from '../src/production-index-crm-customer360-entry.js';

const customer={
  customer_id:'26000101',
  name:'山田 花子',
  line_linked:true,
  line_display_name:'hanako',
  customer_rank:'A',
  realized_ltv:128000,
  shoot_count:3,
  avg_order_value:42667,
  last_shoot_date:'2026-07-01',
  recommendation:{next_offer:'七五三'},
  address:{prefecture:'大阪府',city:'豊中市'},
  family:[{relation:'子',name:'太郎',birthdate:'2021-10-10',school_stage:'年中',memo:'人見知り'}],
  opportunities:[{label:'七五三',days:32}],
  reservations:[
    {reservation_id:'R-1',customer_id:'26000101',shoot_date:'2026-07-01',genre:'Birthday',place:'公園',status:'撮影済み',total_amount:42000},
    {reservation_id:'R-2',customer_id:'26000101',shoot_date:'2025-11-03',genre:'七五三',place:'神社',status:'撮影済み',total_amount:43000}
  ],
  line_history:[
    {created_at:'2026-08-01 10:00',message_text:'次回撮影についてご相談',direction:'incoming'}
  ],
  marketing_history:[
    {created_at:'2026-07-02',summary:'納品後フォロー候補'}
  ],
  profile:{marketing_contact_permission:'allowed'},
  raw:{memo:'最初はパパと一緒だと安心。',acquisition_source:'Instagram'}
};
const listItem={
  customer_id:customer.customer_id,name:customer.name,line_linked:true,realized_ltv:128000,
  family_summary:'子1人',child_count:1,last_shoot_date:'2026-07-01',area_summary:'大阪府 豊中市',
  recommendation:{next_offer:'七五三',priority_score:90},next_opportunity:{label:'七五三',days:32},shoot_count:3
};
const facets={prefectures:['大阪府'],cities:['豊中市'],genres:['七五三'],sources:['Instagram'],campaigns:[],school_stages:['年中']};
const kpis={customers:1,average_realized_ltv:128000,repeat_rate_pct:100,vip_high_ltv:1,event_90d:1,dormant_180:0,line_link_rate_pct:100,approach_this_month:1};

const base=`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<style>html,body{margin:0;max-width:100%;overflow-x:hidden}.app{width:min(calc(100vw - 24px),1500px);margin:12px auto}.crm-lineops-panel{display:none;position:fixed;inset:12px;background:#fff;z-index:2300}.crm-lineops-panel.open{display:block}</style></head>
<body><button id="lineOpsOpen">LINE</button><section id="lineOpsPanel" class="crm-lineops-panel"><button id="lineOpsClose">閉じる</button><h2>LINE</h2></section><main class="app"><h1>顧客管理</h1><section id="crmTodayDashboard"><h2>今日やること</h2></section></main>
<script>document.getElementById('lineOpsOpen').onclick=()=>document.getElementById('lineOpsPanel').classList.add('open');document.getElementById('lineOpsClose').onclick=()=>document.getElementById('lineOpsPanel').classList.remove('open');</script></body></html>`;
const html=composeCustomer360AdminHtml(base);

const requests=[],writes=[];
function send(res,status,data,type='application/json; charset=utf-8'){
  res.writeHead(status,{'content-type':type,'cache-control':'no-store'});
  res.end(type.startsWith('application/json')?JSON.stringify(data):data);
}
const server=http.createServer((req,res)=>{
  const u=new URL(req.url,'http://127.0.0.1');
  requests.push(req.method+' '+u.pathname);
  if(!['GET','HEAD'].includes(req.method))writes.push(req.method+' '+u.pathname);
  if(u.pathname==='/'||u.pathname==='/admin')return send(res,200,html,'text/html; charset=utf-8');
  if(u.pathname==='/api/customer360/marketing-home')return send(res,200,{ok:true,kpis,top_opportunities:[listItem],facets});
  if(u.pathname==='/api/customer360/customers')return send(res,200,{ok:true,total:1,all_total:1,page:1,page_size:50,has_next:false,items:[listItem],facets,meta:{privacy_safe_list_dto:true}});
  if(u.pathname==='/api/customer360/analytics')return send(res,200,{ok:true,available:false,period:{from:'2026-09-01',to:'2026-09-09',previous:{from:'2026-08-23',to:'2026-08-31'},span_days:9},current:{revenue:0,completed_shoots:0,unique_customers:0,average_order_value:0,repeat_customers_in_period:0,repeat_rate_pct:0,genres:[],monthly:[]},previous:{revenue:0,completed_shoots:0,unique_customers:0,average_order_value:0},change_pct:{revenue:null,completed_shoots:null,unique_customers:null,average_order_value:null}});
  if(u.pathname==='/api/customer360/approach-queue')return send(res,200,{ok:true,items:[],total:0,summary:{total:0,ready:0,review_required:0,opted_out:0,no_contact:0},filters:{horizon_days:90,status:'all',limit:50},meta:{read_only:true,line_send:false}});
  if(u.pathname==='/api/customer360/customer/26000101')return send(res,200,{ok:true,customer});
  if(u.pathname==='/api/customer360/status')return send(res,200,{ok:true,read_only:true,bindings:{DB:true,LINE_SERVICE:true,RESERVATION_SERVICE:true}});
  return send(res,404,{ok:false,error:'not_found'});
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin='http://127.0.0.1:'+server.address().port;

const browser=await chromium.launch({headless:true});
try{
  for(const viewport of [{width:390,height:844},{width:1440,height:900}]){
    const context=await browser.newContext({viewport});
    const page=await context.newPage();
    const errors=[];
    page.on('pageerror',e=>errors.push(String(e)));
    page.on('console',m=>{if(m.type()==='error')errors.push(m.text())});
    await page.addInitScript(()=>{window.__openedReservation='';const originalOpen=window.open;window.open=(url,...args)=>{window.__openedReservation=String(url||'');return originalOpen?null:null}});
    await page.goto(origin+'/admin',{waitUntil:'domcontentloaded'});
    await page.waitForFunction(()=>window.__crmOwnerView&&window.__crmCustomer360UI&&document.querySelector('#crmMktList [data-open],#crmMktList [data-direct-customer]'));

    await page.evaluate(()=>window.__crmOwnerView.showCustomers());
    const open=page.locator('#crmMktList [data-open],#crmMktList [data-direct-customer]').first();
    await open.click();
    await page.waitForFunction(()=>document.getElementById('crmMktDetail')?.classList.contains('open'));

    const detailText=await page.locator('#crmMktDetail').innerText();
    assert(detailText.includes('山田 花子'),viewport.width+': customer name missing');
    assert(detailText.includes('Customer ID 26000101'),viewport.width+': Customer ID missing');
    assert(detailText.includes('LINE連携済み'),viewport.width+': LINE status missing');
    assert(detailText.includes('連絡許可あり'),viewport.width+': explicit contact permission missing');
    assert(detailText.includes('¥128,000'),viewport.width+': LTV missing');
    assert(detailText.includes('3回'),viewport.width+': shoot count missing');
    assert(detailText.includes('七五三'),viewport.width+': next offer missing');
    assert(detailText.includes('R-1')===false,'reservation internal ID should not be required in visible copy');
    assert(detailText.includes('Birthday'),viewport.width+': reservation history missing');
    assert(detailText.includes('次回撮影についてご相談'),viewport.width+': LINE history missing');
    assert(detailText.includes('この画面からLINEは自動送信されません。'),viewport.width+': LINE safety note missing');

    assert.equal(await page.locator('.crm-360-profile-hero').count(),1);
    assert.equal(await page.locator('.crm-360-stat-grid .crm-360-card').count(),4);
    assert.equal(await page.locator('#crmDetailBack').count(),1);
    assert.equal(await page.locator('#crmDetailLine').count(),1);
    assert.equal(await page.locator('#crmDetailReservation').count(),1);
    assert.equal(await page.locator('#crmDetailMarketing').count(),1);

    await page.locator('#crmDetailReservation').click();
    assert((await page.evaluate(()=>window.__openedReservation)).includes('reservation-app-api'),viewport.width+': reservation app shortcut missing');

    await page.locator('#crmDetailLine').click();
    await page.waitForFunction(()=>document.body.dataset.crmOwnerView==='line');
    assert(await page.locator('#lineOpsPanel').isVisible(),viewport.width+': LINE workspace did not open');

    await page.evaluate(()=>window.__crmOwnerView.showCustomers());
    await open.click();
    await page.waitForFunction(()=>document.getElementById('crmMktDetail')?.classList.contains('open'));
    await page.locator('#crmDetailMarketing').click();
    await page.waitForFunction(()=>document.body.dataset.crmOwnerView==='marketing');

    const overflow=await page.evaluate(()=>Math.max(
      document.documentElement.scrollWidth-document.documentElement.clientWidth,
      document.body.scrollWidth-document.body.clientWidth
    ));
    assert(overflow<=1,viewport.width+': horizontal overflow '+overflow);
    assert.deepEqual(errors,[],viewport.width+': browser errors '+errors.join(' | '));
    await context.close();
  }
}finally{
  await browser.close();
  await new Promise(resolve=>server.close(resolve));
}

assert.equal(writes.length,0,'Customer360 detail browser must remain read-only: '+writes.join(','));
assert(requests.some(x=>x==='GET /api/customer360/customer/26000101'),'detail GET missing');

console.log('CUSTOMER360_ONE_SCREEN_DETAIL=PASS');
console.log('CUSTOMER360_DETAIL_CONTACT_PERMISSION_EXPLICIT=PASS');
console.log('CUSTOMER360_DETAIL_RESERVATION_HISTORY=PASS');
console.log('CUSTOMER360_DETAIL_LINE_HISTORY=PASS');
console.log('CUSTOMER360_DETAIL_MOBILE_390=PASS');
console.log('CUSTOMER360_DETAIL_HTTP_WRITE=0');
console.log('CUSTOMER360_DETAIL_AUTOMATIC_LINE_SEND=0');
