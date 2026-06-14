// ======================================================
// CUSTOMER CRM / UI POLISH WRAPPER
// build: customer-crm-api-ui-polish-20260614-01
// - モーダル / カード / ボタン / 入力欄の見た目を統一
// - スマホ操作性を改善
// - 既存機能は壊さずCSSと軽量JSだけで補正
// ======================================================

import app from "./production-index-crm-home-dashboard.js";

const BUILD = "customer-crm-api-ui-polish-20260614-01";

function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" }
  });
}

function injectUiPolish(html){
  if(!html || html.includes("crm-ui-polish-script")) return html;
  const style = `<style id="crm-ui-polish-style">
:root{--crmux-green:#028760;--crmux-green-dark:#026c4d;--crmux-ink:#111827;--crmux-muted:#6b7280;--crmux-line:#e5e7eb;--crmux-soft:#f8fafc;--crmux-card:#ffffff;--crmux-red:#dc2626;--crmux-orange:#f59e0b;--crmux-blue:#2563eb;--crmux-shadow:0 18px 50px rgba(15,23,42,.14)}
html{scroll-behavior:smooth}body{background:#f7f8fa!important;color:var(--crmux-ink);-webkit-text-size-adjust:100%}button,input,select,textarea{font-family:inherit}input,select,textarea{font-size:16px!important}.crm-ui-hidden-legacy{display:none!important}
#crmUnifiedPanel,#crmLineOpsPanel,#crmBulkSafePanel,#crmListWorkbenchPanel,#crmCustomerSmartPanel,#crmInquiryRowPanel,#crmInquiryActionsPanel,#crmFollowTemplatePanel,#crmTemplatePanel,#crmMarketingPanel,#crmCampaignPanel,#crmSourcePanel{box-shadow:var(--crmux-shadow)!important;border:1px solid var(--crmux-line)!important;border-radius:24px!important;background:#fff!important;color:var(--crmux-ink)!important;box-sizing:border-box!important;overflow:hidden!important}
#crmUnifiedPanel::before,#crmLineOpsPanel::before,#crmBulkSafePanel::before,#crmListWorkbenchPanel::before,#crmCustomerSmartPanel::before,#crmInquiryRowPanel::before,#crmInquiryActionsPanel::before,#crmFollowTemplatePanel::before,#crmTemplatePanel::before,#crmMarketingPanel::before,#crmCampaignPanel::before,#crmSourcePanel::before{content:"";display:block;height:6px;background:linear-gradient(90deg,var(--crmux-green),#34d399)}
#crmUnifiedPanel h1,#crmUnifiedPanel h2,#crmUnifiedPanel h3,#crmLineOpsPanel h1,#crmLineOpsPanel h2,#crmLineOpsPanel h3,#crmBulkSafePanel h1,#crmBulkSafePanel h2,#crmBulkSafePanel h3,#crmListWorkbenchPanel h1,#crmListWorkbenchPanel h2,#crmListWorkbenchPanel h3,#crmCustomerSmartPanel h1,#crmCustomerSmartPanel h2,#crmCustomerSmartPanel h3,#crmInquiryRowPanel h1,#crmInquiryRowPanel h2,#crmInquiryRowPanel h3{letter-spacing:-.02em;color:var(--crmux-ink)!important;font-weight:950!important}
#crmUnifiedPanel button,#crmLineOpsPanel button,#crmBulkSafePanel button,#crmListWorkbenchPanel button,#crmCustomerSmartPanel button,#crmInquiryRowPanel button,#crmInquiryActionsPanel button,#crmFollowTemplatePanel button,#crmTemplatePanel button,#crmMarketingPanel button,#crmCampaignPanel button,#crmSourcePanel button{min-height:40px;border-radius:999px!important;font-weight:900!important;border:1px solid var(--crmux-line)!important;cursor:pointer!important;transition:transform .12s ease,box-shadow .12s ease,background .12s ease!important}
#crmUnifiedPanel button:hover,#crmLineOpsPanel button:hover,#crmBulkSafePanel button:hover,#crmListWorkbenchPanel button:hover,#crmCustomerSmartPanel button:hover,#crmInquiryRowPanel button:hover{transform:translateY(-1px);box-shadow:0 8px 18px rgba(15,23,42,.10)}
#crmUnifiedPanel button:active,#crmLineOpsPanel button:active,#crmBulkSafePanel button:active,#crmListWorkbenchPanel button:active,#crmCustomerSmartPanel button:active,#crmInquiryRowPanel button:active{transform:translateY(0);box-shadow:none}
#crmUnifiedPanel input,#crmUnifiedPanel select,#crmUnifiedPanel textarea,#crmLineOpsPanel input,#crmLineOpsPanel select,#crmLineOpsPanel textarea,#crmBulkSafePanel input,#crmBulkSafePanel select,#crmBulkSafePanel textarea,#crmListWorkbenchPanel input,#crmListWorkbenchPanel select,#crmListWorkbenchPanel textarea,#crmCustomerSmartPanel input,#crmCustomerSmartPanel select,#crmCustomerSmartPanel textarea,#crmInquiryRowPanel input,#crmInquiryRowPanel select,#crmInquiryRowPanel textarea{border:1px solid var(--crmux-line)!important;border-radius:14px!important;padding:11px 12px!important;background:#fff!important;box-sizing:border-box!important;outline:none!important}
#crmUnifiedPanel input:focus,#crmUnifiedPanel select:focus,#crmUnifiedPanel textarea:focus,#crmLineOpsPanel input:focus,#crmLineOpsPanel select:focus,#crmLineOpsPanel textarea:focus,#crmBulkSafePanel input:focus,#crmBulkSafePanel select:focus,#crmBulkSafePanel textarea:focus,#crmListWorkbenchPanel input:focus,#crmListWorkbenchPanel select:focus,#crmListWorkbenchPanel textarea:focus,#crmCustomerSmartPanel input:focus,#crmCustomerSmartPanel select:focus,#crmCustomerSmartPanel textarea:focus,#crmInquiryRowPanel input:focus,#crmInquiryRowPanel select:focus,#crmInquiryRowPanel textarea:focus{border-color:var(--crmux-green)!important;box-shadow:0 0 0 4px rgba(2,135,96,.10)!important}
.crm-ui-polish-card,#crmLineOpsPanel li,#crmBulkSafePanel li,#crmListWorkbenchPanel li,#crmCustomerSmartPanel li,#crmInquiryRowPanel li{border:1px solid var(--crmux-line)!important;border-radius:16px!important;background:#fff!important;padding:12px!important;box-shadow:0 6px 16px rgba(15,23,42,.05)!important;line-height:1.65!important}.crm-ui-polish-muted{color:var(--crmux-muted)!important;font-size:12px!important}.crm-ui-polish-badge{display:inline-flex;align-items:center;gap:4px;border-radius:999px;padding:4px 9px;font-size:11px;font-weight:900;background:#ecfdf5;color:var(--crmux-green);border:1px solid #bbf7d0}.crm-ui-polish-badge.warn{background:#fffbeb;color:#b45309;border-color:#fde68a}.crm-ui-polish-badge.alert{background:#fef2f2;color:#b91c1c;border-color:#fecaca}
.crm-home-wrap{padding-bottom:24px}.crm-home-card{transition:transform .12s ease,box-shadow .12s ease}.crm-home-card:hover{transform:translateY(-2px);box-shadow:0 16px 34px rgba(15,23,42,.09)}.crm-home-btn,.crm-home-open{min-height:42px!important}.crm-home-title{letter-spacing:-.03em}.crm-home-sub{font-size:13px!important}.crm-home-panel{border-radius:22px!important}.crm-home-task{background:#fff!important}
.crm-ux-toast{position:fixed;left:50%;bottom:24px;transform:translateX(-50%);z-index:2147483647;background:#111827;color:#fff;border-radius:999px;padding:11px 16px;font-size:13px;font-weight:900;box-shadow:var(--crmux-shadow);opacity:0;pointer-events:none;transition:.18s}.crm-ux-toast.show{opacity:1;bottom:34px}.crm-ux-focus-ring{outline:4px solid rgba(2,135,96,.18)!important;outline-offset:3px!important}
@media(max-width:760px){body{padding-bottom:76px!important}#crmUnifiedPanel,#crmLineOpsPanel,#crmBulkSafePanel,#crmListWorkbenchPanel,#crmCustomerSmartPanel,#crmInquiryRowPanel,#crmInquiryActionsPanel,#crmFollowTemplatePanel,#crmTemplatePanel,#crmMarketingPanel,#crmCampaignPanel,#crmSourcePanel{left:10px!important;right:10px!important;top:10px!important;bottom:10px!important;width:auto!important;max-width:none!important;max-height:none!important;border-radius:20px!important}#crmUnifiedPanel button,#crmLineOpsPanel button,#crmBulkSafePanel button,#crmListWorkbenchPanel button,#crmCustomerSmartPanel button,#crmInquiryRowPanel button{width:auto;min-height:44px!important}.crm-home-hero{border-radius:20px!important}.crm-home-card,.crm-home-panel{border-radius:18px!important}.crm-home-grid{gap:9px!important}.crm-home-task{gap:8px!important}.crm-home-open{width:100%!important}.crm-ux-toast{width:calc(100% - 28px);text-align:center;border-radius:16px}}
@media(prefers-reduced-motion:reduce){*{transition:none!important;animation:none!important;scroll-behavior:auto!important}}
</style>`;
  const script = `<script id="crm-ui-polish-script">
(()=>{if(window.__crmUiPolish)return;window.__crmUiPolish=1;const $$=(s,r=document)=>Array.from(r.querySelectorAll(s));const labels=['LINE運用','一括確認','一覧操作','顧客スマート','重要','マーケDB','キャンペーン','成約・流入','問い合わせ一覧アクション','問い合わせ→次アクション','フォロー→LINE','LINEテンプレ管理','CRM実務強化','納品進捗','統合ログ'];function toast(msg){let t=document.getElementById('crmUxToast');if(!t){t=document.createElement('div');t.id='crmUxToast';t.className='crm-ux-toast';document.body.appendChild(t)}t.textContent=msg;t.classList.add('show');clearTimeout(t._timer);t._timer=setTimeout(()=>t.classList.remove('show'),2200)}function normalizeButtons(){const unified=$$('button,a,[role=button]').some(el=>/CRMメニュー/.test((el.textContent||'').trim()));if(!unified)return;$$('button,a,[role=button]').forEach(el=>{const txt=(el.textContent||'').trim();if(labels.includes(txt)){el.classList.add('crm-ui-hidden-legacy');el.setAttribute('aria-hidden','true')}})}function polishPanels(){['crmUnifiedPanel','crmLineOpsPanel','crmBulkSafePanel','crmListWorkbenchPanel','crmCustomerSmartPanel','crmInquiryRowPanel','crmInquiryActionsPanel','crmFollowTemplatePanel','crmTemplatePanel','crmMarketingPanel','crmCampaignPanel','crmSourcePanel'].forEach(id=>{const p=document.getElementById(id);if(!p)return;p.setAttribute('role','dialog');p.setAttribute('aria-modal','true');$$('li, .row, .item, .card',p).slice(0,120).forEach(x=>x.classList.add('crm-ui-polish-card'));$$('button',p).forEach(b=>{if(!b.dataset.crmPolished){b.dataset.crmPolished='1';b.addEventListener('click',()=>toast('操作しました'))}})})}function addKeyboard(){document.addEventListener('keydown',e=>{if(e.key==='Escape'){const open=$$('button').find(b=>/閉じる|×|閉/.test((b.textContent||'').trim())&&b.offsetParent);if(open)open.click()}if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==='k'){e.preventDefault();const menu=$$('button,a,[role=button]').find(x=>/CRMメニュー|メニュー/.test((x.textContent||'').trim()));if(menu)menu.click()}})}function markCurrent(){const path=location.pathname;$$('a,button').forEach(x=>{const txt=(x.textContent||'').trim();if(path.includes('admin')&&/CRMメニュー/.test(txt))x.classList.add('crm-ux-focus-ring')})}function run(){normalizeButtons();polishPanels();markCurrent()}addKeyboard();run();new MutationObserver(run).observe(document.body,{childList:true,subtree:true});})();
</script>`;
  return html.includes("</body>") ? html.replace("</body>", style + script + "</body>") : html + style + script;
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);
    try{
      if(url.pathname === "/health"){
        const res = await app.fetch(request, env, ctx);
        const data = await res.json().catch(()=>({}));
        return json({ ...data, uiPolishBuild: BUILD });
      }
      const res = await app.fetch(request, env, ctx);
      const ct = res.headers.get("content-type") || "";
      if(request.method === "GET" && ct.includes("text/html")){
        return new Response(injectUiPolish(await res.text()), { status: res.status, headers: res.headers });
      }
      return res;
    }catch(e){
      return json({ ok:false, build:BUILD, message:String(e && e.message || e) }, 500);
    }
  }
};
