import assert from 'node:assert/strict';
import http from 'node:http';
import { chromium } from 'playwright';
import { composeCustomer360AdminHtml } from '../src/production-index-crm-customer360-entry.js';

const legacyBase=`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><style>html,body{margin:0}.app{max-width:1200px;margin:0 auto;padding:16px}</style></head><body>
<div id="crmMobileBar">legacy mobile nav</div><div class="crm-mf-bottom">legacy nav</div><div class="crm-bottom-nav">legacy nav</div><button id="crmPriorityFab">重要</button><button class="crm-mf-fab">FAB</button><button class="crm-top-menu-btn">☰</button><div class="crmUxQuickHint">legacy hint</div><button id="crmStableAuditBtn">状態確認</button>
<div class="crm-lineops-fab"><button id="lineOpsOpen">LINE運用</button></div><section id="lineOpsPanel"><button id="lineOpsClose">閉じる</button><h2>旧LINE</h2></section>
<div id="crmUserFab"><button id="crmOpenUsers">ユーザー管理</button></div><div id="crmUserBackdrop"></div><div id="crmUserModal">旧管理ユーザー</div>
<main class="app"><h1>顧客管理</h1><section id="crmTodayDashboard"><h2>今日やること</h2></section><section id="crmReservationStatus">旧予約状態</section><section id="crmDeliveryDeadlinePanel">旧納品期限</section><section id="crmLegacyOperations">旧運用</section><section id="crmTodayFilterPanel">旧フィルター</section></main>
</body></html>`;
const html=composeCustomer360AdminHtml(legacyBase);
const customer={customer_id:'26000001',name:'山田 花子',line_display_name:'hanako',line_linked:true,realized_ltv:50000,shoot_count:2,family_summary:'子1人',last_shoot_date:'2026-08-20',area_summary:'大阪府豊中市',recommendation:{next_offer:'七五三'},next_opportunity:{label:'七五三',days:20}};
const facets={prefectures:['大阪府'],cities:['豊中市'],genres:['七五三'],sources:['Instagram'],campaigns:['秋'],school_stages:[]};
const requests=[];
function send(res,status,data,type='application/json; charset=utf-8'){res.writeHead(status,{'content-type':type,'cache-control':'no-store'});res.end(type.startsWith('application/json')?JSON.stringify(data):data)}
const server=http.createServer((req,res)=>{
  const u=new URL(req.url,'http://127.0.0.1');requests.push(req.method+' '+u.pathname+u.search);
  if(u.pathname==='/'||u.pathname==='/admin')return send(res,200,html,'text/html; charset=utf-8');
  if(u.pathname==='/api/customer360/status')return send(res,200,{ok:true,read_only:true,bindings:{DB:true,RESERVATION_SERVICE:true,LINE_SERVICE:true},customer360_status_read_only:true,d1_write:false,schema_repair:false,customer_write:false,customer_id_generation:false,line_send:false});
  if(u.pathname==='/api/customer360/marketing-home')return send(res,200,{ok:true,kpis:{customers:1,average_realized_ltv:50000,repeat_rate_pct:100,vip_high_ltv:0,event_90d:1,dormant_180:0,line_link_rate_pct:100,approach_this_month:1},top_opportunities:[customer],facets});
  if(u.pathname==='/api/customer360/analytics')return send(res,200,{ok:true,available:true,period:{from:'2026-09-01',to:'2026-09-30',previous:{from:'2026-08-02',to:'2026-08-31'},span_days:30},current:{revenue:50000,completed_shoots:1,unique_customers:1,average_order_value:50000,repeat_customers_in_period:0,repeat_rate_pct:0,genres:[],monthly:[]},previous:{revenue:0,completed_shoots:0,unique_customers:0,average_order_value:0,repeat_customers_in_period:0,repeat_rate_pct:0,genres:[],monthly:[]},change_pct:{revenue:null,completed_shoots:null,unique_customers:null,average_order_value:null}});
  if(u.pathname==='/api/customer360/approach-queue')return send(res,200,{ok:true,items:[],total:0,summary:{total:0,ready:0,review_required:0,opted_out:0,no_contact:0},filters:{horizon_days:90,status:'all',limit:50},meta:{read_only:true,automatic_contact:false,line_send:false}});
  if(u.pathname==='/api/customer360/customers')return send(res,200,{ok:true,total:1,all_total:1,page:1,page_size:100,has_next:false,items:[customer],facets,meta:{privacy_safe_list_dto:true}});
  if(u.pathname==='/api/customer360/customer/26000001')return send(res,200,{ok:true,customer:{...customer,address:{prefecture:'大阪府',city:'豊中市'},family:[],opportunities:[],reservations:[],line_history:[],marketing_history:[],marketing_classes:[],consent:{},recommendation:{}}});
  if(u.pathname==='/api/customers/26000001/line-history')return send(res,200,{ok:true,connected:true,contact_found:true,messages:[{direction:'inbound',message_text:'こんにちは',created_at:'2026-10-09T00:00:00Z'}]});
  if(u.pathname==='/api/customer360/media/26000001')return send(res,200,{ok:true,media:{}});
  if(req.method==='GET')return send(res,200,{ok:true,items:[],rows:[],facets});
  return send(res,405,{ok:false,error:'write_not_expected'});
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({headless:true});
const legacySelectors=['#crmTodayDashboard','#crmReservationStatus','#crmDeliveryDeadlinePanel','#crmLegacyOperations','#crmTodayFilterPanel','#lineOpsOpen','.crm-lineops-fab','#lineOpsPanel','#crmMobileBar','#crmPriorityFab','.crmUxQuickHint','.crm-mf-bottom','.crm-mf-fab','.crm-mf-scrolltop','.crm-bottom-nav','.crm-top-menu-btn','.crm-side-menu','#crmStableAuditBtn','.crm-stable-audit-btn','#crmSettingsMenuBtn','#crmLogoutMenuBtn','#crmUxFab','#crmUserFab','#crmUserBackdrop','#crmUserModal'];
try{
  for(const viewport of [{width:390,height:844},{width:1440,height:900}]){
    const context=await browser.newContext({viewport,hasTouch:viewport.width<=767,isMobile:viewport.width<=430});
    const page=await context.newPage(),errors=[];
    page.on('pageerror',e=>errors.push('pageerror:'+e.message));
    page.on('console',m=>{if(m.type()==='error')errors.push('console:'+m.text())});
    await page.goto(origin+'/admin',{waitUntil:'domcontentloaded'});
    await page.waitForFunction(()=>document.getElementById('crmOwnerAppShell')&&window.__crmOwnerView&&window.__crmCustomer360UI&&document.querySelector('#crmMktList tbody tr'));
    await page.waitForFunction(()=>document.body.dataset.crmOwnerView==='customers');

    for(const sel of legacySelectors)assert.equal(await page.locator(sel).count(),0,viewport.width+': legacy UI remains '+sel);
    const duplicates=await page.evaluate(()=>{const seen=new Set(),dups=[];for(const el of document.querySelectorAll('[id]')){if(seen.has(el.id))dups.push(el.id);seen.add(el.id)}return [...new Set(dups)]});
    assert.deepEqual(duplicates,[],viewport.width+': duplicate DOM ids '+duplicates.join(','));
    const visibleLegacyText=await page.locator('button:visible,a:visible').evaluateAll(els=>els.map(x=>(x.textContent||'').trim()).filter(t=>['LINE運用','ユーザー管理','状態確認','今日やること'].includes(t)));
    assert.deepEqual(visibleLegacyText,[],viewport.width+': legacy button text visible');

    await page.locator('#crmShellStatusTop').click();
    await page.locator('#crmOwnerStatusSheet.open').waitFor();
    await page.waitForFunction(()=>document.querySelector('#crmOwnerStatusBody')?.textContent.includes('CRM'));
    await page.locator('#crmOwnerStatusClose').click();
    await page.waitForFunction(()=>!document.getElementById('crmOwnerStatusSheet')?.classList.contains('open'));

    await page.locator('#crmShellSettingsTop').click();
    await page.locator('#crmOwnerCanonicalSettings.open').waitFor();
    const settingsClose=page.locator('#crmOwnerCanonicalSettings .crm-owner-close').first();
    assert.equal(await settingsClose.count(),1,viewport.width+': canonical settings close missing');
    await settingsClose.click();
    await page.waitForFunction(()=>!document.getElementById('crmOwnerCanonicalSettings')?.classList.contains('open'));

    const nav=async name=>{
      if(viewport.width<=767)await page.locator('#crmOwnerNav'+({customers:'Customers',search:'Search',marketing:'Marketing',line:'Line'}[name])).click();
      else await page.locator('#crmOwnerDesktopSidebar [data-crm-shell-nav="'+name+'"]').click();
      await page.waitForFunction(v=>document.body.dataset.crmOwnerView===v,name);
    };
    await nav('search');
    await page.waitForFunction(()=>document.activeElement?.id==='crmGlobalSearch');
    await page.locator('#crmGlobalSearch').fill('山田');
    await nav('marketing');
    assert.equal(await page.locator('#crmMktHome').isVisible(),true,viewport.width+': marketing view not visible');
    await nav('line');
    await page.waitForFunction(()=>document.querySelector('#crmLineChatCustomers [data-line-customer="26000001"]'));
    await page.locator('#crmLineChatCustomers [data-line-customer="26000001"]').click();
    await page.waitForFunction(()=>document.getElementById('crmLineChatMessages')?.textContent.includes('こんにちは'));
    await page.locator('#crmLineChatRefresh').click();
    await page.waitForFunction(()=>document.getElementById('crmLineChatMessages')?.textContent.includes('こんにちは'));
    if(viewport.width<=767){await page.locator('#crmLineChatBack').click();await page.waitForFunction(()=>document.querySelector('.crm-line-chat-shell')?.getAttribute('data-chat-open')==='0')}
    await nav('customers');
    assert.equal(await page.locator('#crmMktList').isVisible(),true,viewport.width+': customer list not restored');

    if(viewport.width<=767){
      const reservation=page.locator('#crmOwnerNavReservation');
      assert.equal(await reservation.count(),1,'mobile reservation link missing');
      assert.match(await reservation.getAttribute('href'),/^https:\/\/reservation-app-api\.ohw3rz5578d277e\.workers\.dev\/admin$/);
    }else{
      const reservation=page.locator('#crmOwnerDesktopSidebar a[href="https://reservation-app-api.ohw3rz5578d277e.workers.dev/admin"]');
      assert.equal(await reservation.count(),1,'desktop reservation link missing');
    }

    const controls=await page.locator('button:visible').count();
    assert.ok(controls>=6,viewport.width+': canonical visible button inventory unexpectedly small '+controls);
    assert.equal(errors.length,0,viewport.width+': '+errors.join(' | '));
    await context.close();
  }
  assert.equal(requests.some(x=>!x.startsWith('GET ')&&!x.startsWith('HEAD ')),false,'unexpected HTTP write '+requests.join(' | '));
  assert.ok(requests.some(x=>x.includes('/api/customer360/status')),'status button did not reach read-only endpoint');
  assert.ok(requests.some(x=>x.includes('/api/customers/26000001/line-history')),'LINE conversation control did not fetch history');
  console.log('CRM_CANONICAL_OWNER_UI_BUTTON_AUDIT=PASS');
  console.log('CRM_CANONICAL_NAV_CUSTOMERS_SEARCH_MARKETING_LINE=PASS');
  console.log('CRM_CANONICAL_STATUS_SETTINGS=PASS');
  console.log('CRM_CANONICAL_RESERVATION_LINK=PASS');
  console.log('CRM_CANONICAL_LINE_REFRESH_BACK=PASS');
  console.log('CRM_LEGACY_VISIBLE_DOM=0');
  console.log('CRM_DUPLICATE_DOM_IDS=0');
  console.log('CRM_UNEXPECTED_HTTP_WRITES=0');
} finally {
  await browser.close();
  await new Promise(r=>server.close(r));
}
