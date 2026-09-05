import assert from 'node:assert/strict';
import http from 'node:http';
import { chromium } from 'playwright';
import { composeCustomer360AdminHtml } from '../src/production-index-crm-customer360-entry.js';

const base='<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><style>html,body{margin:0;overflow-x:hidden}.app{max-width:1200px;margin:0 auto;padding:16px}.ops{min-height:180px;background:#fff;border:1px solid #ddd;margin:12px 0;padding:16px}#lineOpsPanel,#crmSettingsPanel{display:none;position:fixed;background:#fff;z-index:2500}#lineOpsPanel.open,#crmSettingsPanel.open{display:block}</style></head><body><button id="lineOpsOpen" type="button">LINE</button><section id="lineOpsPanel"><button id="lineOpsClose" type="button">閉じる</button><h2>LINE</h2></section><section id="crmSettingsPanel"><button class="crm-settings-close" type="button">閉じる</button><h3>設定</h3></section><main class="app"><h1>顧客管理</h1><section id="crmTodayDashboard" class="crm-today-dash ops"><h2>今日やることダッシュボード</h2></section><section id="crmReservationStatus" class="ops"><h2>予約管理との連携</h2></section><section id="crmDeliveryDeadlinePanel" class="crm-delivery-panel ops"><h2>納品期限</h2></section></main><script>document.getElementById("lineOpsOpen").onclick=()=>document.getElementById("lineOpsPanel").classList.add("open");document.getElementById("lineOpsClose").onclick=()=>document.getElementById("lineOpsPanel").classList.remove("open");document.querySelector(".crm-settings-close").onclick=()=>document.getElementById("crmSettingsPanel").classList.remove("open");</script></body></html>';
const html=composeCustomer360AdminHtml(base);
const item={customer_id:'26000001',name:'山田 花子',line_linked:true,realized_ltv:50000,shoot_count:2,family_summary:'子1人',last_shoot_date:'2026-08-20',area_summary:'大阪',recommendation:{next_offer:'七五三'},next_opportunity:{label:'七五三',days:20}};
const facets={prefectures:[],cities:[],genres:[],sources:[],campaigns:[],school_stages:[]};
const requests=[];let delayNextCustomers=false;
function send(res,status,data,type='application/json; charset=utf-8'){res.writeHead(status,{'content-type':type,'cache-control':'no-store'});res.end(type.startsWith('application/json')?JSON.stringify(data):data)}
const server=http.createServer(async(req,res)=>{
  const u=new URL(req.url,'http://127.0.0.1');requests.push(req.method+' '+u.pathname+u.search);
  if(u.pathname==='/'||u.pathname==='/admin')return send(res,200,html,'text/html; charset=utf-8');
  if(u.pathname==='/__fixture/delay-next-customers'){delayNextCustomers=true;return send(res,200,{ok:true})}
  if(u.pathname==='/api/customer360/marketing-home')return send(res,200,{ok:true,kpis:{customers:1,average_realized_ltv:50000,repeat_rate_pct:100,vip_high_ltv:0,event_90d:1,dormant_180:0,line_link_rate_pct:100,approach_this_month:1},top_opportunities:[item],facets});
  if(u.pathname==='/api/customer360/customers'){if(delayNextCustomers){delayNextCustomers=false;await new Promise(r=>setTimeout(r,350))}return send(res,200,{ok:true,total:1,all_total:1,page:1,page_size:50,has_next:false,items:[item],facets,meta:{privacy_safe_list_dto:true}})}
  if(u.pathname.startsWith('/api/customer360/customer/'))return send(res,200,{ok:true,customer:{...item,address:{},family:[],opportunities:[],reservations:[],line_history:[],marketing_history:[],marketing_classes:[],consent:{},recommendation:{}}});
  return send(res,404,{ok:false});
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({headless:true});
try{
  {
    const context=await browser.newContext({viewport:{width:390,height:844},hasTouch:true,isMobile:true});
    const page=await context.newPage(),errors=[];
    page.on('pageerror',e=>errors.push('pageerror:'+e.message));
    page.on('console',m=>{if(m.type()==='error')errors.push('console:'+m.text())});
    await page.request.get(origin+'/__fixture/delay-next-customers');
    await page.goto(origin+'/admin',{waitUntil:'domcontentloaded'});
    await page.waitForFunction(()=>window.__crmOwnerView&&document.getElementById('crmMktHome')?.textContent.includes('期間分析'));
    await page.evaluate(()=>window.__crmOwnerView.showMarketing());
    await page.waitForFunction(()=>window.__crmOwnerView.getCurrentView()==='marketing'&&document.getElementById('crmMktHome')?.classList.contains('open'));
    await page.waitForFunction(()=>document.querySelector('#crmMktList tbody tr'),null,{timeout:1500});
    await page.waitForTimeout(60);
    assert.equal(await page.evaluate(()=>window.__crmOwnerView.getCurrentView()),'marketing','background initial list load stole Owner marketing navigation');
    assert.equal(await page.locator('#crmMktHome').isVisible(),true,'marketing home hidden after background list completion');
    assert.equal(await page.locator('#crmMktList').isVisible(),false,'customer list surfaced after background list completion');
    assert.equal(errors.length,0,'initial navigation race: '+errors.join(' | '));
    await context.close();
  }
  for(const viewport of [{width:390,height:844},{width:1440,height:900}]){
    const context=await browser.newContext({viewport,hasTouch:viewport.width<=900,isMobile:viewport.width<=430});
    const page=await context.newPage(),errors=[];
    page.on('pageerror',e=>errors.push('pageerror:'+e.message));
    page.on('console',m=>{if(m.type()==='error')errors.push('console:'+m.text())});
    await page.goto(origin+'/admin',{waitUntil:'domcontentloaded'});
    await page.waitForFunction(()=>document.getElementById('crmOwnerAppShell')&&window.__crmOwnerView&&window.__crmCustomer360UI&&document.querySelector('#crmMktList tbody tr'));
    await page.waitForFunction(()=>document.body.dataset.crmOwnerView==='today');
    assert.equal(await page.locator('#crmOwnerAppShell').count(),1);
    assert.equal(await page.locator('#crmTodayDashboard').isVisible(),true,viewport.width+': today dashboard hidden on initial view');
    assert.equal(await page.locator('#crmMktList').isVisible(),false,viewport.width+': customer list visible on Today');
    assert.equal(await page.locator('#lineOpsOpen').isVisible(),false,viewport.width+': legacy LINE entry visible under V2');
    assert.equal(await page.locator('#crmShellSettingsTop').isVisible(),true,viewport.width+': canonical Settings action missing');
    if(viewport.width>900){
      assert.equal(await page.locator('#crmOwnerDesktopSidebar').isVisible(),true,'desktop sidebar missing');
      assert.equal(await page.locator('#crmOwnerMobileNav').isVisible(),false,'mobile nav visible on desktop');
      await page.locator('[data-crm-shell-nav="customers"]').click();
    }else{
      assert.equal(await page.locator('#crmOwnerDesktopSidebar').isVisible(),false,'desktop sidebar visible on mobile');
      assert.equal(await page.locator('#crmOwnerMobileNav').isVisible(),true,'mobile nav missing');
      await page.locator('#crmOwnerNavCustomers').click();
    }
    await page.waitForFunction(()=>document.body.dataset.crmOwnerView==='customers'&&document.getElementById('crmMktList')?.classList.contains('open'));
    assert.equal(await page.locator('#crmMktList').isVisible(),true,viewport.width+': customer list hidden');
    assert.equal(await page.locator('#crmTodayDashboard').isVisible(),false,viewport.width+': Today leaked into Customer view');
    if(viewport.width<=900){
      const toolbar=await page.evaluate(()=>{const box=id=>{const r=document.getElementById(id)?.getBoundingClientRect();return r?{x:r.x,y:r.y,w:r.width,h:r.height}:null};return{saved:box('crmSavedViews'),save:box('crmSaveCurrent'),sort:box('crmSort')}});
      assert.ok(toolbar.saved?.w>=120&&toolbar.save?.w>=120,'mobile saved-condition controls too narrow '+JSON.stringify(toolbar));
      assert.ok(toolbar.sort?.w>=300,'mobile sort control must use full row '+JSON.stringify(toolbar));
      assert.ok(toolbar.sort.y>toolbar.saved.y,'mobile sort control must stack below saved controls '+JSON.stringify(toolbar));
    }
    if(viewport.width>900)await page.locator('[data-crm-shell-nav="marketing"]').click();
    else await page.locator('#crmMktNav [data-view="home"]').click();
    await page.waitForFunction(()=>document.body.dataset.crmOwnerView==='marketing'&&document.getElementById('crmMktHome')?.classList.contains('open'));
    assert.equal(await page.locator('#crmMktHome').isVisible(),true,viewport.width+': analytics/approach home hidden');
    assert.equal(await page.locator('#crmMktList').isVisible(),false,viewport.width+': list leaked into analytics view');
    assert.ok((await page.locator('#crmOwnerWorkspaceTitle').textContent()).includes('分析・アプローチ'));
    await page.locator('#crmMktNav [data-view="list"]').click();
    await page.waitForFunction(()=>document.body.dataset.crmOwnerView==='customers'&&document.getElementById('crmMktList')?.classList.contains('open'));
    const overflow=await page.evaluate(()=>Math.max(document.documentElement.scrollWidth-document.documentElement.clientWidth,document.body.scrollWidth-document.body.clientWidth));
    assert.ok(overflow<=1,viewport.width+': horizontal overflow '+overflow);
    assert.equal(errors.length,0,viewport.width+': '+errors.join(' | '));
    await context.close();
  }
  assert.equal(requests.some(x=>!x.startsWith('GET ')&&!x.startsWith('HEAD ')),false,'unexpected HTTP write: '+requests.join(' | '));
  console.log('OWNER_APP_SHELL_V2_BROWSER=PASS');
  console.log('OWNER_APP_SHELL_DESKTOP_SIDEBAR=PASS');
  console.log('OWNER_APP_SHELL_MOBILE_NAV=PASS');
  console.log('OWNER_VIEW_EXCLUSIVE_ROUTING=PASS');
  console.log('OWNER_LEGACY_ENTRY_DUPLICATION=0');
  console.log('OWNER_MOBILE_SETTINGS_VISIBLE=PASS');
  console.log('OWNER_MOBILE_TOOLBAR_STACK=PASS');
  console.log('OWNER_INITIAL_BACKGROUND_LOAD_NAVIGATION_STABLE=PASS');
  console.log('HTTP_WRITES=0');
} finally {
  await browser.close();
  await new Promise(r=>server.close(r));
}
