import assert from 'node:assert/strict';
import http from 'node:http';
import { chromium } from 'playwright';
import { injectTodayDashboardUi } from '../src/production-index-crm-today-dashboard.js';
import { injectTodayActionUi } from '../src/production-index-crm-today-actions.js';
import { injectHomeDashboard } from '../src/production-index-crm-home-dashboard.js';
import { injectFetchSafeUi } from '../src/production-index-crm-fetch-safe-fix.js';
import { composeCustomer360AdminHtml } from '../src/production-index-crm-customer360-entry.js';

let todayReads=0;
let failToday=false;
const payload={
  ok:true,
  date_jst:'2026-09-09',
  tomorrow_jst:'2026-09-10',
  counts:{
    today_shoots:1,tomorrow_shoots:1,reservation_alerts:1,reservation_danger:1,
    line_pending:1,line_high:1,follow_due:1,follow_overdue:1,immediate_total:3,
    sent_today:2,created_today:1,cancelled_today:0,history_synced_today:1
  },
  today_shoots:[
    {reservation_id:'R-TODAY',customer_id:'26000123',customer_name:'山田 花子',genre:'七五三',shoot_date:'2026-09-09',start_time:'10:00',place:'神社',status:'confirmed'}
  ],
  tomorrow_shoots:[
    {reservation_id:'R-TOMORROW',customer_id:'26000456',customer_name:'佐藤 未来',genre:'Family',shoot_date:'2026-09-10',start_time:'13:00',place:'公園',status:'confirmed'}
  ],
  next_shoot:{reservation_id:'R-TODAY',customer_id:'26000123',customer_name:'山田 花子',genre:'七五三',shoot_date:'2026-09-09',start_time:'10:00',place:'神社',status:'confirmed'},
  priority_items:[
    {type:'reservation_alert',severity:'danger',label:'CRM履歴未反映',customer_id:'26000123',customer_name:'山田 花子',title:'予約履歴を確認',meta:'予約ID R-TODAY'},
    {type:'line_pending',severity:'warn',label:'LINE未送信',customer_id:'26000456',customer_name:'佐藤 未来',title:'七五三のご案内',meta:'優先度 high'}
  ],
  follow_tasks:[
    {id:3,customer_id:'26000456',customer_name:'佐藤 未来',title:'納品後フォロー',due_date:'2020-01-01',priority:'high'}
  ],
  line_pending:[
    {id:4,customer_id:'26000456',customer_name:'佐藤 未来',action_label:'七五三のご案内',priority:'high',created_at:'2026-09-09T01:00:00Z'}
  ],
  sales_focus:[
    {customer_id:'26000123',customer_name:'山田 花子',total_revenue:128000,repeat_count:3,dormant_days:40,genre_history:'お宮参り / 七五三'}
  ]
};

const base=`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body data-crm-owner-view="customers">
<input id="crmGlobalSearch" value="">
<script>
window.__ownerCalls={search:0,line:0,marketing:0,input:0};
window.__crmOwnerView={
 showSearch(){window.__ownerCalls.search++},
 showLine(){window.__ownerCalls.line++},
 showMarketing(){window.__ownerCalls.marketing++}
};
document.getElementById('crmGlobalSearch').addEventListener('input',()=>window.__ownerCalls.input++);
</script>
<main id="app"></main></body></html>`;

const html=injectTodayDashboardUi(base);
const composedBase='<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body><button id="lineOpsOpen">LINE</button><section id="lineOpsPanel"></section><main class="app"><h1>顧客管理</h1></main></body></html>';
const composedHtml=injectFetchSafeUi(injectHomeDashboard(injectTodayActionUi(injectTodayDashboardUi(composeCustomer360AdminHtml(composedBase)))));
const legacyHomeHtml=injectHomeDashboard('<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body data-crm-owner-view="today"><main></main></body></html>');
assert(composedHtml.includes('crmTodayDashboardScript'));
assert(composedHtml.includes('crm-today-action-panel'));
assert(composedHtml.includes('crm-home-dashboard-script'));
assert(html.includes('crmTodayDashboardScript'));
assert(html.includes('OWNER DAILY CONTROL'));
assert.equal(injectTodayDashboardUi(html),html,'Today UI injector must be idempotent');

const server=http.createServer((req,res)=>{
  if(req.url==='/api/today-dashboard'){
    todayReads++;
    if(failToday){
      res.writeHead(503,{'content-type':'application/json; charset=utf-8','cache-control':'no-store'});
      return res.end(JSON.stringify({ok:false,error:'today_dashboard_read_unavailable',message:'今日やることを読み込めません。'}));
    }
    res.writeHead(200,{'content-type':'application/json; charset=utf-8','cache-control':'no-store'});
    return res.end(JSON.stringify(payload));
  }
  if(req.url==='/api/today-dashboard.csv'){
    res.writeHead(200,{'content-type':'text/csv; charset=utf-8'});
    return res.end('ok');
  }
  if(req.url==='/composed'){res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-store'});return res.end(composedHtml)}
  if(req.url==='/legacy-home'){res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-store'});return res.end(legacyHomeHtml)}
  if(req.url.startsWith('/api/customer360/marketing-home')){res.writeHead(200,{'content-type':'application/json'});return res.end(JSON.stringify({ok:true,kpis:{customers:0,average_realized_ltv:0,repeat_rate_pct:0,vip_high_ltv:0,event_90d:0,dormant_180:0,line_link_rate_pct:0,approach_this_month:0},top_opportunities:[],facets:{prefectures:[],cities:[],genres:[],sources:[],campaigns:[],school_stages:[]}}))}
  if(req.url.startsWith('/api/customer360/customers')){res.writeHead(200,{'content-type':'application/json'});return res.end(JSON.stringify({ok:true,total:0,all_total:0,page:1,page_size:50,has_next:false,items:[],facets:{prefectures:[],cities:[],genres:[],sources:[],campaigns:[],school_stages:[]},meta:{privacy_safe_list_dto:true}}))}
  res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-store'});
  res.end(html);
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({headless:true});

try{
  {
    todayReads=0;
    const context=await browser.newContext({viewport:{width:390,height:844}});
    await context.addInitScript(()=>{
      const real=window.setInterval.bind(window);
      window.setInterval=(fn,ms,...args)=>real(fn,ms===120000?40:ms,...args);
    });
    const page=await context.newPage();
    await page.goto(origin+'/composed',{waitUntil:'domcontentloaded'});
    await page.waitForFunction(()=>window.__crmOwnerView&&window.__crmCustomer360UI);
    await page.evaluate(()=>window.__crmOwnerView.showCustomers());
    await page.waitForFunction(()=>document.body.dataset.crmOwnerView==='customers');
    await page.waitForTimeout(80);
    todayReads=0;
    await page.waitForTimeout(180);
    assert.equal(todayReads,0,'full Production composition fetched /api/today-dashboard off-view');
    await page.evaluate(()=>window.__crmOwnerView.showMarketing());
    await page.waitForTimeout(140);
    assert.equal(todayReads,0,'marketing view fetched /api/today-dashboard');

    failToday=true;
    const propagated=await page.evaluate(async()=>{const r=await fetch('/api/today-dashboard');let j={};try{j=await r.json()}catch{}return{status:r.status,ok:r.ok,body:j}});
    assert.equal(propagated.status,503,'browser fetch-safe wrapper must preserve Today 503');
    assert.equal(propagated.ok,false,'browser fetch-safe wrapper converted Today failure to success');
    assert.equal(propagated.body?.error,'today_dashboard_read_unavailable');

    failToday=false;
    await context.close();
  }

  {
    failToday=true;
    const context=await browser.newContext({viewport:{width:390,height:844}});
    const page=await context.newPage();
    const errors=[];
    page.on('pageerror',e=>errors.push(String(e)));
    await page.goto(origin+'/legacy-home',{waitUntil:'domcontentloaded'});
    await page.waitForFunction(()=>window.__crmHomeDashboard);
    await page.evaluate(async()=>{window.__crmHomeDashboard.mount();await window.__crmHomeDashboard.load()});
    await page.waitForFunction(()=>document.getElementById('crmHomeCards')?.innerText.includes('読み込み不可'),null,{timeout:5000});
    const legacyHomeText=await page.locator('#crmHomeDash').innerText();
    assert(legacyHomeText.includes('今日の優先順位を判定できません'),'legacy home did not surface Today outage');
    assert(!legacyHomeText.includes('今すぐの高優先タスクはありません'),'legacy home converted Today outage to empty success');
    assert.deepEqual(errors,[],'legacy home outage path raised page errors: '+errors.join(' | '));
    failToday=false;
    await context.close();
  }
  for(const viewport of [{width:390,height:844},{width:1440,height:900}]){
    todayReads=0;
    const context=await browser.newContext({viewport});
    const page=await context.newPage();
    const errors=[];
    page.on('pageerror',e=>errors.push(String(e)));
    page.on('console',m=>{if(m.type()==='error')errors.push(m.text())});

    await page.goto(origin+'/admin',{waitUntil:'domcontentloaded'});
    await page.waitForFunction(()=>document.getElementById('crmTodayDashboard'));
    await page.waitForTimeout(80);
    assert.equal(todayReads,0,viewport.width+': hidden Today view fetched data');

    await page.evaluate(()=>{
      document.body.dataset.crmOwnerView='today';
      document.body.classList.add('crm-owner-view-today');
      document.dispatchEvent(new CustomEvent('crm:owner-view-change',{detail:{view:'today'}}));
    });
    await page.waitForFunction(()=>document.querySelector('#crmTodayFocus .crm-today-focus-value')?.textContent.includes('3件'));
    assert.equal(todayReads,1,viewport.width+': Today open did not fetch exactly once');

    assert((await page.locator('#crmTodayFocus').innerText()).includes('3件を先に確認'));
    assert.equal((await page.locator('#crmTodayNextShoot').textContent()).trim(),'10:00');
    assert((await page.locator('#crmTodayShoots').innerText()).includes('山田 花子'));
    assert((await page.locator('#crmTomorrowShoots').innerText()).includes('佐藤 未来'));
    assert((await page.locator('#crmTodayKpis').innerText()).includes('今日の撮影'));

    await page.locator('#crmTodayShoots [data-today-customer="26000123"]').click();
    await page.waitForFunction(()=>document.getElementById('crmGlobalSearch').value==='26000123');
    const searchState=await page.evaluate(()=>({calls:window.__ownerCalls.search,input:window.__ownerCalls.input,value:document.getElementById('crmGlobalSearch').value}));
    assert.equal(searchState.calls,1,viewport.width+': customer shortcut did not open search');
    assert.equal(searchState.value,'26000123');
    assert(searchState.input>=1,viewport.width+': customer shortcut did not trigger search input');

    await page.locator('#crmTodayLinePending').click();
    await page.locator('#crmTodayMarketing').click();
    const nav=await page.evaluate(()=>window.__ownerCalls);
    assert.equal(nav.line,1,viewport.width+': LINE shortcut missing');
    assert.equal(nav.marketing,1,viewport.width+': marketing shortcut missing');

    await page.evaluate(()=>{
      document.body.dataset.crmOwnerView='customers';
      document.body.classList.remove('crm-owner-view-today');
      document.dispatchEvent(new CustomEvent('crm:owner-view-change',{detail:{view:'customers'}}));
    });
    const before=todayReads;
    await page.waitForTimeout(60);
    assert.equal(todayReads,before,viewport.width+': leaving Today triggered an extra fetch');

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

console.log('OWNER_DAILY_CONTROL_CENTER_UI=PASS');
console.log('OWNER_DAILY_CONTROL_CENTER_MOBILE_390=PASS');
console.log('OWNER_DAILY_CONTROL_CENTER_OFF_VIEW_FETCH=0');
console.log('OWNER_DAILY_CONTROL_CENTER_FULL_COMPOSITION_OFF_VIEW_FETCH=0');
console.log('OWNER_DAILY_CONTROL_CENTER_BROWSER_503_PRESERVED=PASS');
console.log('OWNER_DAILY_CONTROL_CENTER_LEGACY_HOME_503_VISIBLE=PASS');
console.log('OWNER_DAILY_CONTROL_CENTER_CUSTOMER_SHORTCUT=PASS');
console.log('OWNER_DAILY_CONTROL_CENTER_AUTOMATIC_LINE_SEND=0');
