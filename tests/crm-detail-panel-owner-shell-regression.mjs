import assert from 'node:assert/strict';
import {chromium} from 'playwright';
import {injectDetailPanelFix} from '../src/production-index-crm-detail-panel-fix.js';

const browser=await chromium.launch({headless:true});

async function ownerShellRecovery(){
  const context=await browser.newContext({viewport:{width:390,height:844}});
  const page=await context.newPage();
  const base=`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style>
  html,body{margin:0}#crmOwnerAppShell{width:390px;min-height:844px;background:#fff}#crmOwnerWorkspaceContent{width:100%}.app{width:100%}#crmMktList{display:block;width:100%;min-height:620px;background:#fff}.crm-mkt-table{width:100%}
  </style></head><body class="crm-detail-open">
  <div id="crmOwnerAppShell" class="crm-detail-managed-panel crm-detail-force-visible-close crm-force-hide-floating" style="display:none;visibility:hidden" aria-hidden="true">
    <button class="crm-detail-close-btn" type="button">×</button>
    <section id="crmOwnerWorkspace">
      <div id="crmOwnerWorkspaceContent">
        <main class="app crm-detail-managed-panel crm-detail-force-visible-close crm-force-hide-floating" style="display:none;visibility:hidden" aria-hidden="true">
          <button class="crm-detail-close-btn" type="button">×</button>
          <div id="crmMktNav">顧客一覧 分析・アプローチ</div>
          <section id="crmMktList" class="crm-mkt-shell open">
            <table class="crm-mkt-table"><tbody><tr><td>LINE履歴</td><td>撮影履歴</td><td>購入履歴</td><td>タイムライン</td><td>累計売上</td><td>平均顧客単価</td></tr></tbody></table>
          </section>
          <section id="crmMktHome" class="crm-mkt-shell"></section>
        </main>
      </div>
    </section>
  </div>
  </body></html>`;
  await page.setContent(injectDetailPanelFix(base),{waitUntil:'domcontentloaded'});
  await page.waitForTimeout(1200);
  const state=await page.evaluate(()=>{
    const info=el=>{const s=getComputedStyle(el),r=el.getBoundingClientRect();return{display:s.display,visibility:s.visibility,width:r.width,height:r.height,managed:el.classList.contains('crm-detail-managed-panel'),force:el.classList.contains('crm-detail-force-visible-close'),hiddenClass:el.classList.contains('crm-force-hide-floating'),aria:el.getAttribute('aria-hidden')}};
    return{
      bodyOpen:document.body.classList.contains('crm-detail-open'),
      shell:info(document.getElementById('crmOwnerAppShell')),
      app:info(document.querySelector('.app')),
      list:info(document.getElementById('crmMktList')),
      managedInside:document.querySelectorAll('#crmOwnerAppShell .crm-detail-managed-panel').length,
      closeInside:document.querySelectorAll('#crmOwnerAppShell .crm-detail-close-btn').length
    };
  });
  assert.equal(state.bodyOpen,false,'stale detail-open must be cleared');
  for(const [name,item] of [['shell',state.shell],['app',state.app]]){
    assert.notEqual(item.display,'none',name+' display must recover');
    assert.notEqual(item.visibility,'hidden',name+' visibility must recover');
    assert.equal(item.managed,false,name+' managed class must clear');
    assert.equal(item.force,false,name+' force-visible class must clear');
    assert.equal(item.hiddenClass,false,name+' force-hide class must clear');
    assert.notEqual(item.aria,'true',name+' aria-hidden must clear');
    assert.ok(item.width>0&&item.height>0,name+' must have layout');
  }
  assert.equal(state.list.managed,false,'Customer360 list must never become legacy managed panel');
  assert.notEqual(state.list.visibility,'hidden','Customer360 list must remain visible');
  assert.ok(state.list.width>0&&state.list.height>0,'Customer360 list must have layout');
  assert.equal(state.managedInside,0,'Owner shell subtree must contain no legacy managed panels');
  assert.equal(state.closeInside,0,'stale legacy close buttons must be removed from protected roots');
  await context.close();
}

async function realLegacyPanelStillWorks(){
  const context=await browser.newContext({viewport:{width:390,height:844}});
  const page=await context.newPage();
  const base=`<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style>
  html,body{margin:0}#crmOwnerAppShell{width:390px;min-height:844px;background:#f8fafc}.app{min-height:600px;background:#fff}#legacyDetail{position:fixed;right:0;top:0;width:360px;height:700px;background:#fff}
  </style></head><body>
  <div id="crmOwnerAppShell"><section id="crmOwnerWorkspace"><div id="crmOwnerWorkspaceContent"><main class="app"><div id="crmMktNav">顧客一覧</div><section id="crmMktList" class="crm-mkt-shell open">顧客一覧</section><section id="crmMktHome"></section></main></div></section></div>
  <aside id="legacyDetail"><h2>顧客詳細</h2><div>LINE履歴</div><div>撮影履歴</div><div>購入履歴</div><div>タイムライン</div><div>累計売上</div><div>平均顧客単価</div></aside>
  </body></html>`;
  await page.setContent(injectDetailPanelFix(base),{waitUntil:'domcontentloaded'});
  await page.waitForFunction(()=>document.getElementById('legacyDetail')?.classList.contains('crm-detail-managed-panel'),null,{timeout:3000});
  let state=await page.evaluate(()=>({
    bodyOpen:document.body.classList.contains('crm-detail-open'),
    managed:document.getElementById('legacyDetail').classList.contains('crm-detail-managed-panel'),
    close:!!document.querySelector('#legacyDetail > .crm-detail-close-btn'),
    shellManaged:document.getElementById('crmOwnerAppShell').classList.contains('crm-detail-managed-panel')
  }));
  assert.equal(state.bodyOpen,true,'real legacy detail should still enter detail-open state');
  assert.equal(state.managed,true,'real legacy detail should remain managed');
  assert.equal(state.close,true,'real legacy detail should receive close button');
  assert.equal(state.shellManaged,false,'Owner shell must stay unclassified');
  await page.locator('#legacyDetail > .crm-detail-close-btn').click();
  await page.waitForFunction(()=>getComputedStyle(document.getElementById('legacyDetail')).display==='none');
  state=await page.evaluate(()=>({
    bodyOpen:document.body.classList.contains('crm-detail-open'),
    shellDisplay:getComputedStyle(document.getElementById('crmOwnerAppShell')).display
  }));
  assert.equal(state.bodyOpen,false,'detail-open must clear after closing real legacy detail');
  assert.notEqual(state.shellDisplay,'none','closing legacy detail must not hide Owner shell');
  await context.close();
}

try{
  await ownerShellRecovery();
  await realLegacyPanelStillWorks();
  console.log('CRM_DETAIL_PANEL_OWNER_SHELL_REGRESSION=PASS');
}finally{
  await browser.close();
}
