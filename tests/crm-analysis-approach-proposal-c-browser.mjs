import assert from 'node:assert/strict';
import http from 'node:http';
import fs from 'node:fs';
import { chromium } from 'playwright';
import { injectProposalCAnalysisApproach, proposalCAnalysisApproachContract } from '../src/crm-analysis-approach-proposal-c.mjs';

const source=fs.readFileSync(new URL('../src/crm-analysis-approach-proposal-c.mjs',import.meta.url),'utf8');
assert.equal(proposalCAnalysisApproachContract.mode,'analysis-approach-strengthened');
assert.equal(proposalCAnalysisApproachContract.mobile_first,true);
assert.equal(proposalCAnalysisApproachContract.automatic_contact,false);
assert.equal(proposalCAnalysisApproachContract.automatic_line_send,false);
assert.equal(proposalCAnalysisApproachContract.owner_review_required,true);
assert.equal(proposalCAnalysisApproachContract.contact_permission_fail_closed,true);
assert.equal(proposalCAnalysisApproachContract.no_mutation_observer,true);
assert(!source.includes('MutationObserver('),'Proposal C must not add a MutationObserver');
assert(!source.includes("fetch("),'Proposal C UI must not add a new network write/read path directly');

const base=`<!doctype html><html><head><meta charset="utf-8"></head><body data-crm-owner-view="today">
<div id="crmMktHome">
  <div class="crm-mkt-hero">
    <div class="crm-mkt-focus"><div class="crm-mkt-eyebrow">MARKETING CRM</div><h1>分析・アプローチ</h1><p>old</p></div>
    <div class="crm-mkt-next"><div class="crm-mkt-eyebrow">NEXT</div><h2>次の家族イベント</h2><p>候補</p></div>
  </div>
  <div class="crm-mkt-kpis">
    <div class="crm-mkt-kpi"><span>顧客数</span><b>74</b></div>
    <div class="crm-mkt-kpi"><span>今月アプローチ候補</span><b>18</b></div>
    <div class="crm-mkt-kpi"><span>リピート率</span><b>58%</b></div>
    <div class="crm-mkt-kpi"><span>90日以内イベント候補</span><b>28</b></div>
  </div>
  <section class="crm-period-analytics"><div class="crm-period-head"><div><div class="crm-mkt-eyebrow">PERIOD ANALYTICS</div><h2>期間分析</h2></div></div><button id="crmAnalyticsApply">分析</button></section>
  <section class="crm-approach-queue">
    <div class="crm-period-head"><div><div class="crm-mkt-eyebrow">APPROACH QUEUE</div><h2>アプローチキュー</h2><p class="crm-mkt-sub">old queue</p></div></div>
    <button id="crmApproachLoad">候補を読み込む</button>
    <div class="crm-approach-list">
      <article class="crm-approach-row">
        <div class="crm-approach-score">920</div>
        <div class="crm-approach-main">
          <div class="crm-mkt-name">山田 花子</div>
          <div class="crm-mkt-sub">Customer ID 26001234 / 30日以内の家族イベント</div>
          <div class="crm-approach-offer">提案: ファミリーフォト</div>
          <details class="crm-approach-draft"><summary>文案を見る</summary><p>山田 花子さん、いつもありがとうございます。</p><div class="crm-mkt-sub">表示のみ</div></details>
        </div>
        <div class="crm-approach-contact"><span class="crm-status good">手動連絡候補</span><div class="crm-mkt-sub">候補チャネル LINE</div></div>
      </article>
      <article class="crm-approach-row">
        <div class="crm-approach-score">600</div>
        <div class="crm-approach-main">
          <div class="crm-mkt-name">佐藤 一郎</div>
          <div class="crm-mkt-sub">Customer ID 26005678 / 確認必要</div>
          <details class="crm-approach-draft"><summary>文案を見る</summary><p>佐藤 一郎さんへの文案</p></details>
        </div>
        <div class="crm-approach-contact"><span class="crm-status soon">連絡可否の確認が必要</span><div class="crm-mkt-sub">候補チャネル LINE</div></div>
      </article>
      <article class="crm-approach-row">
        <div class="crm-approach-score">580</div>
        <div class="crm-approach-main">
          <div class="crm-mkt-name">高橋 次郎</div>
          <div class="crm-mkt-sub">Customer ID 26009999 / 手動連絡候補</div>
          <details class="crm-approach-draft"><summary>文案を見る</summary><p>高橋 次郎さんへの別文案</p></details>
        </div>
        <div class="crm-approach-contact"><span class="crm-status good">手動連絡候補</span><div class="crm-mkt-sub">候補チャネル LINE</div></div>
      </article>
    </div>
  </section>
</div>
<script>
window.__calls={analytics:0,approach:0,line:0,copied:''};
const analyticsBtn=document.getElementById('crmAnalyticsApply'),approachBtn=document.getElementById('crmApproachLoad');
analyticsBtn.onclick=()=>window.__calls.analytics++;
approachBtn.onclick=()=>window.__calls.approach++;
analyticsBtn.remove();approachBtn.remove();
window.__crmOwnerView={showLine(){window.__calls.line++;document.body.dataset.crmOwnerView='line';return true}};
setTimeout(()=>{
  document.body.dataset.crmOwnerView='marketing';
  document.dispatchEvent(new CustomEvent('crm:owner-view-change',{detail:{view:'marketing'}}));
  setTimeout(()=>{
    document.querySelector('.crm-period-analytics').appendChild(analyticsBtn);
    document.querySelector('.crm-approach-queue').appendChild(approachBtn);
    document.dispatchEvent(new CustomEvent('crm:marketing-home-rendered'));
  },20);
},0);
</script>
</body></html>`;

const injected=injectProposalCAnalysisApproach(base);
assert(injected.includes('crm-proposal-c-analysis-approach-style'));
assert(injected.includes('crm-proposal-c-analysis-approach-script'));
assert.equal(injectProposalCAnalysisApproach(injected),injected,'injector must be idempotent');
// Production Customer360 normalizes raw injected script-close markers after the injection chain.
const html=injected.split('<\\/script>').join('</script>');

const raceBase=`<!doctype html><html><head><meta charset="utf-8"></head><body data-crm-owner-view="today">
<div id="crmMktHome"><section class="crm-period-analytics"><button id="crmAnalyticsApply">分析</button></section><section class="crm-approach-queue"><button id="crmApproachLoad">候補</button></section></div>
<script>
window.__raceCalls={analytics:0,approach:0};
document.getElementById('crmAnalyticsApply').onclick=()=>window.__raceCalls.analytics++;
document.getElementById('crmApproachLoad').onclick=()=>window.__raceCalls.approach++;
setTimeout(()=>{
  document.body.dataset.crmOwnerView='marketing';
  document.dispatchEvent(new CustomEvent('crm:owner-view-change',{detail:{view:'marketing'}}));
  document.body.dataset.crmOwnerView='today';
  document.dispatchEvent(new CustomEvent('crm:owner-view-change',{detail:{view:'today'}}));
},0);
</script></body></html>`;
const raceHtml=injectProposalCAnalysisApproach(raceBase).split('<\\/script>').join('</script>');

const server=http.createServer((req,res)=>{
  res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-store'});
  res.end(req.url==='/race'?raceHtml:html);
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
const origin='http://127.0.0.1:'+server.address().port;
const browser=await chromium.launch({headless:true});

try{
  {
    const context=await browser.newContext({viewport:{width:390,height:844}});
    const page=await context.newPage();
    await page.goto(origin+'/race',{waitUntil:'domcontentloaded'});
    await page.waitForTimeout(80);
    assert.equal(await page.evaluate(()=>document.body.dataset.crmOwnerView),'today','rapid navigation did not leave marketing');
    assert.equal(await page.evaluate(()=>window.__raceCalls.analytics),0,'deferred analytics load escaped marketing view');
    assert.equal(await page.evaluate(()=>window.__raceCalls.approach),0,'deferred approach load escaped marketing view');
    await context.close();
  }
  for(const viewport of [{width:390,height:844},{width:1440,height:900}]){
    const context=await browser.newContext({viewport});
    await context.addInitScript(()=>{
      window.__clipboardDelay=false;window.__clipboardPending=[];
      Object.defineProperty(navigator,'clipboard',{configurable:true,value:{writeText:text=>{
        if(!window.__clipboardDelay){window.__calls.copied=text;return Promise.resolve()}
        return new Promise(resolve=>window.__clipboardPending.push(()=>{window.__calls.copied=text;resolve()}))
      }}});
    });
    const page=await context.newPage();
    const errors=[];
    page.on('pageerror',e=>errors.push(String(e)));
    page.on('console',m=>{if(m.type()==='error')errors.push(m.text())});
    await page.goto(origin,{waitUntil:'domcontentloaded'});
    await page.waitForFunction(()=>document.body.classList.contains('crm-proposal-c')&&window.__calls.analytics===1&&window.__calls.approach===1);
    assert.equal(await page.evaluate(()=>window.__calls.analytics),1,viewport.width+': cold analytics auto-load duplicated');
    assert.equal(await page.evaluate(()=>window.__calls.approach),1,viewport.width+': cold approach auto-load duplicated');

    const hero=await page.locator('.crm-mkt-focus p').textContent();
    assert(hero.includes('今、連絡すべきお客様'),viewport.width+': Proposal C hero copy missing');
    await page.evaluate(()=>{document.querySelector('.crm-mkt-focus p').textContent='old rerender copy';document.dispatchEvent(new CustomEvent('crm:marketing-home-rendered'))});
    assert((await page.locator('.crm-mkt-focus p').textContent()).includes('今、連絡すべきお客様'),viewport.width+': Proposal C copy not reapplied after marketing rerender');
    assert.equal(await page.locator('#crmProposalCComposer').count(),1,viewport.width+': composer missing');

    await page.locator('.crm-approach-row').first().locator('summary').click();
    await page.waitForFunction(()=>document.getElementById('crmProposalCComposer').classList.contains('open'));
    assert.equal((await page.locator('#crmProposalCName').textContent()).trim(),'山田 花子');
    assert((await page.locator('#crmProposalCMeta').textContent()).includes('26001234'));
    assert.equal(await page.locator('#crmProposalCLine').isDisabled(),false);
    assert((await page.locator('#crmProposalCText').inputValue()).includes('いつもありがとうございます'));

    await page.evaluate(()=>{window.__clipboardDelay=true});
    await page.locator('#crmProposalCLine').click();
    await page.locator('.crm-c-compose-close').click();
    await page.locator('.crm-approach-row').nth(2).locator('summary').click();
    await page.waitForFunction(()=>document.getElementById('crmProposalCComposer').classList.contains('open')&&document.getElementById('crmProposalCName').textContent.includes('高橋'));
    await page.evaluate(()=>{window.__clipboardDelay=false;const done=window.__clipboardPending.shift();if(done)done()});
    await page.waitForTimeout(40);
    assert.equal(await page.evaluate(()=>window.__calls.line),0,viewport.width+': stale clipboard completion navigated to LINE');
    assert.equal((await page.locator('#crmProposalCName').textContent()).trim(),'高橋 次郎',viewport.width+': stale completion replaced active customer context');
    assert.equal(await page.locator('#crmProposalCComposer').isVisible(),true,viewport.width+': stale completion closed active composer');
    await page.locator('.crm-c-compose-close').click();

    await page.locator('.crm-approach-row').first().locator('summary').click();
    await page.waitForFunction(()=>document.getElementById('crmProposalCComposer').classList.contains('open'));
    await page.locator('#crmProposalCLine').click();
    await page.waitForFunction(()=>window.__calls.line===1);
    const copied=await page.evaluate(()=>window.__calls.copied);
    assert(copied.includes('山田 花子さん'),viewport.width+': draft was not copied');
    assert.equal(await page.locator('#crmProposalCComposer').isVisible(),false,viewport.width+': composer remained open after LINE handoff');

    await page.evaluate(()=>{document.body.dataset.crmOwnerView='marketing';document.dispatchEvent(new CustomEvent('crm:owner-view-change',{detail:{view:'marketing'}}))});
    await page.locator('.crm-approach-row').nth(1).locator('summary').click();
    await page.waitForFunction(()=>document.getElementById('crmProposalCComposer').classList.contains('open'));
    assert.equal(await page.locator('#crmProposalCLine').isDisabled(),true,viewport.width+': permission review candidate must not proceed to LINE');
    assert((await page.locator('#crmProposalCResult').textContent()).includes('連絡許可'),viewport.width+': fail-closed permission message missing');

    const overflow=await page.evaluate(()=>Math.max(document.documentElement.scrollWidth-document.documentElement.clientWidth,document.body.scrollWidth-document.body.clientWidth));
    assert(overflow<=1,viewport.width+': horizontal overflow '+overflow);
    assert.deepEqual(errors,[],viewport.width+': browser errors '+errors.join(' | '));
    await context.close();
  }
}finally{
  await browser.close();
  await new Promise(resolve=>server.close(resolve));
}

console.log('PROPOSAL_C_STALE_LINE_HANDOFF_CANCEL=PASS');
console.log('PROPOSAL_C_DEFERRED_VIEW_SCOPE=PASS');
console.log('PROPOSAL_C_ANALYSIS_APPROACH_UI=PASS');
console.log('PROPOSAL_C_OWNER_REVIEW_COMPOSER=PASS');
console.log('PROPOSAL_C_CONTACT_PERMISSION_FAIL_CLOSED=PASS');
console.log('PROPOSAL_C_AUTOMATIC_LINE_SEND=0');
console.log('PROPOSAL_C_MUTATION_OBSERVER=0');
