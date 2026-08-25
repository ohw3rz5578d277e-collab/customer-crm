import fs from 'node:fs';
import path from 'node:path';
import { chromium } from 'playwright';

function assert(ok, message){ if(!ok) throw new Error(message); }
function extractStyle(file){
  const src=fs.readFileSync(file,'utf8');
  const m=src.match(/const style = `([\s\S]*?<\/style>)`;/);
  if(!m) throw new Error(`style_not_found:${file}`);
  return m[1].replaceAll('${RESPONSIVE_MARKER}','crm-production-desktop-owner-hotfix-20260825');
}

const srcDir=path.resolve('src');
const styles=[
  'production-index-crm-final-layout-cleanup.js',
  'production-index-crm-mobile-first-ux.js',
  'production-index-crm-usability-hub.js',
  'production-index-crm-customer-list-return.js',
  'production-index-crm-stable-customer-list.js',
  'production-index-crm-customer-list-detail-v2.js',
  'production-index-crm-instagram-nav-v4.js',
  'production-index-crm-browser-root-entry.js'
].map(f=>extractStyle(path.join(srcDir,f))).join('\n');

const kpis=Array.from({length:8},(_,i)=>`<div class="card kpi-card"><b>${i+1}</b><span>KPI ${i+1}</span></div>`).join('');
const rows=Array.from({length:8},(_,i)=>`<tr><td>顧客 ${i+1}</td><td>2600000${i+1}</td><td>お宮参り</td><td>¥35,000</td></tr>`).join('');
const html=`<!doctype html><html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style>
body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Noto Sans JP",sans-serif;background:#f7f8fa}.app{max-width:1180px;margin:0 auto;padding:16px}.home-box,.crm-box{background:#fff;border:1px solid #e2e8f0;border-radius:20px;padding:20px;margin-bottom:18px}.grid{display:grid;gap:12px}.card{border:1px solid #e2e8f0;border-radius:16px;padding:16px;min-width:0}.marketing{display:grid;grid-template-columns:1fr;gap:12px}.marketing>div{min-height:130px;border:1px solid #e2e8f0;border-radius:16px;padding:16px}.toolbar{display:flex;gap:10px;flex-wrap:wrap}.toolbar input{min-height:44px;border:1px solid #cbd5e1;border-radius:12px;padding:0 12px}.toolbar button{min-height:44px;border:0;border-radius:12px;padding:0 16px}.table-wrap{overflow-x:auto}.table-wrap table{min-width:760px}.crm-stable-customer-panel.open{display:flex}.crm-v2-detail-panel.open{display:flex}
</style>${styles}</head><body>
<div class="app">
  <section id="homeDashboard" class="crm-home-wrap crm-final-shell home-box"><h1>HOME</h1><p>今日のダッシュボード</p></section>
  <main id="crmMain" class="crm-admin-wrap crm-final-shell crm-box">
    <h1>顧客管理CRM</h1><p>顧客一覧・検索・予約履歴連携</p>
    <section id="summary" class="grid">${kpis}</section>
    <section class="marketing"><div id="marketingMetrics"><h2>顧客マーケティング指標</h2></div><div id="itemRanking"><h2>購入アイテムランキング</h2></div></section>
    <section class="toolbar"><input id="keywordInput" value="" placeholder="名前・LINE・IDで検索"><select id="sort"><option>新しい順</option></select><select id="filter"><option>すべて</option></select><button>検索</button></section>
    <section class="table-wrap"><table><thead><tr><th>顧客名</th><th>ID</th><th>ジャンル</th><th>累計</th></tr></thead><tbody>${rows}</tbody></table></section>
    <section id="crmStableCustomerPanel" class="crm-stable-customer-panel open"><div class="crm-stable-customer-head"><h2>顧客リスト</h2></div><div class="crm-stable-customer-search"><input placeholder="顧客名 / ID / LINEで検索"><button>更新</button></div><div class="crm-stable-customer-list"><div class="crm-stable-customer-card">顧客 1</div></div></section>
  </main>
</div>
<button id="crmUxFab">メニュー</button><button id="crmCustomerReturnFab" class="crm-customer-return-fab">顧客リストへ</button><div id="crmCustomerReturnTopCard" class="crm-customer-return-top-card">顧客リストを開く</div><a id="crmReconciliationLink">顧客ID照合</a><nav id="crmInstaNav"><button class="crm-insta-tab">今日</button><button class="crm-insta-tab">検索</button><button class="crm-insta-tab">LINE</button><button class="crm-insta-tab">顧客</button><button class="crm-insta-tab">設定</button></nav><div class="crm-mf-bottom crm-legacy-hidden-by-insta"></div><button class="crm-mf-fab crm-legacy-hidden-by-insta">+</button><button class="crm-mf-scrolltop crm-legacy-hidden-by-insta">↑</button>
</body></html>`;

fs.mkdirSync('artifacts/customer-production-owner-layout',{recursive:true});
const browser=await chromium.launch({headless:true});
const results={};

async function inspect(width,height,label,screenshot){
  const context=await browser.newContext({viewport:{width,height},screen:{width,height}});
  const page=await context.newPage();
  await page.setContent(html,{waitUntil:'domcontentloaded'});
  const r=await page.evaluate(()=>{
    const box=s=>document.querySelector(s)?.getBoundingClientRect();
    const display=s=>{const e=document.querySelector(s);return e?getComputedStyle(e).display:'missing'};
    const position=s=>{const e=document.querySelector(s);return e?getComputedStyle(e).position:'missing'};
    const main=box('.app'),home=box('#homeDashboard'),crm=box('#crmMain'),stable=box('#crmStableCustomerPanel');
    const k=[...document.querySelectorAll('#summary .kpi-card')].map(e=>e.getBoundingClientRect());
    const marketing=getComputedStyle(document.querySelector('.marketing')).gridTemplateColumns;
    const toolbar=[...document.querySelectorAll('.toolbar > *')].map(e=>e.getBoundingClientRect());
    return {
      viewport:innerWidth,mainWidth:main?.width||0,ratio:(main?.width||0)/innerWidth,
      homeBottom:home?.bottom||0,crmTop:crm?.top||0,crmBottom:crm?.bottom||0,stableTop:stable?.top||0,
      stablePosition:position('#crmStableCustomerPanel'),
      uxFab:display('#crmUxFab'),customerFab:display('#crmCustomerReturnFab'),returnCard:display('#crmCustomerReturnTopCard'),reconcile:display('#crmReconciliationLink'),insta:display('#crmInstaNav'),mfBottom:display('.crm-mf-bottom'),mfFab:display('.crm-mf-fab'),mfScroll:display('.crm-mf-scrolltop'),
      kpiXs:[...new Set(k.map(x=>Math.round(x.x)))],kpiMin:k.length?Math.min(...k.map(x=>x.width)):0,
      marketingColumns:marketing,toolbarYs:[...new Set(toolbar.map(x=>Math.round(x.y)))],toolbarInput:box('#keywordInput')?.width||0,
      tableWidth:box('.table-wrap')?.width||0,scrollWidth:document.documentElement.scrollWidth,clientWidth:document.documentElement.clientWidth
    };
  });
  if(width>=1024){
    assert(r.mainWidth>=width*.75,`${label}: main width ${r.mainWidth} < 75%`);
    if(width===1920) assert(r.mainWidth>=1400,`${label}: main width ${r.mainWidth} < 1400`);
    assert(r.uxFab==='none',`${label}: crmUxFab ${r.uxFab}`);
    assert(r.customerFab==='none',`${label}: customer return FAB ${r.customerFab}`);
    assert(r.returnCard==='none',`${label}: duplicate return card ${r.returnCard}`);
    assert(r.reconcile==='none',`${label}: reconciliation floating link ${r.reconcile}`);
    assert(r.insta==='none',`${label}: instagram mobile nav ${r.insta}`);
    assert(r.mfBottom==='none'&&r.mfFab==='none'&&r.mfScroll==='none',`${label}: mobile nav controls visible`);
    assert(r.stablePosition!=='fixed',`${label}: stable customer panel still fixed`);
    assert(r.homeBottom<=r.crmTop+1,`${label}: HOME/CRM overlap`);
    assert(r.kpiXs.length===4,`${label}: KPI columns ${r.kpiXs.length}`);
    assert(r.kpiMin>=190,`${label}: KPI min width ${r.kpiMin}`);
    assert(r.marketingColumns.trim().split(/\s+/).length===2,`${label}: marketing not 2 columns: ${r.marketingColumns}`);
    assert(r.toolbarYs.length===1,`${label}: toolbar wrapped to ${r.toolbarYs.length} rows`);
    assert(r.toolbarInput>=280,`${label}: search input too narrow ${r.toolbarInput}`);
    assert(r.tableWidth>=width*.70,`${label}: customer table area too narrow ${r.tableWidth}`);
  }else if(width>=768){
    assert(r.mainWidth>=width*.9,`${label}: tablet main too narrow ${r.mainWidth}`);
    assert(r.uxFab==='none'&&r.insta==='none',`${label}: tablet floating desktop-inappropriate nav visible`);
    assert(r.stablePosition!=='fixed',`${label}: tablet stable customer panel fixed`);
  }else{
    assert(r.mainWidth>=width*.95,`${label}: mobile main too narrow ${r.mainWidth}`);
    assert(r.insta!=='none',`${label}: mobile instagram nav missing`);
    assert(r.scrollWidth<=r.clientWidth+1,`${label}: mobile horizontal overflow ${r.scrollWidth}/${r.clientWidth}`);
  }
  if(screenshot) await page.screenshot({path:`artifacts/customer-production-owner-layout/${screenshot}`,fullPage:true});
  results[label]=r;
  await context.close();
}

await inspect(1920,1080,'1920x1080','1920x1080.png');
await inspect(1440,900,'1440x900','1440x900.png');
await inspect(1024,768,'1024x768',null);
await inspect(390,844,'390x844','390x844.png');
await browser.close();
fs.writeFileSync('artifacts/customer-production-owner-layout/layout-results.json',JSON.stringify(results,null,2));
console.log(JSON.stringify(results,null,2));
console.log('CUSTOMER_PRODUCTION_OWNER_LAYOUT=PASS');
