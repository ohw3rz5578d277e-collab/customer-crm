// ======================================================
// CUSTOMER CRM / FINAL LAYOUT CLEANUP WRAPPER
// build: customer-crm-api-final-layout-cleanup-20260614-01
// - レイアウト崩れ対策
// - 文言統一
// - 空表示の補足
// - 右下UI / スマホ / モーダル重なり補正
// ======================================================

import app from "./production-index-crm-ui-polish.js";

const BUILD = "customer-crm-api-final-layout-cleanup-20260614-01";

function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" }
  });
}

function injectFinalCleanup(html){
  if(!html || html.includes("crm-final-layout-cleanup-script")) return html;
  const style = `<style id="crm-final-layout-cleanup-style">
:root{--crm-final-green:#028760;--crm-final-ink:#111827;--crm-final-muted:#6b7280;--crm-final-line:#e5e7eb;--crm-final-bg:#f7f8fa;--crm-final-card:#ffffff;--crm-final-shadow:0 18px 44px rgba(15,23,42,.12)}
body{background:var(--crm-final-bg)!important;color:var(--crm-final-ink)!important;overflow-x:hidden!important}
*{box-sizing:border-box!important}
.crm-final-shell{max-width:1180px;margin:0 auto!important;padding-left:16px!important;padding-right:16px!important}
.crm-home-wrap,.crm-admin-wrap,.admin-wrap,main,.main{max-width:1180px;margin-left:auto!important;margin-right:auto!important}
.crm-home-hero,.crm-home-card,.crm-home-panel,#crmUnifiedPanel,#crmLineOpsPanel,#crmBulkSafePanel,#crmListWorkbenchPanel,#crmCustomerSmartPanel,#crmInquiryRowPanel,#crmInquiryActionsPanel,#crmFollowTemplatePanel,#crmTemplatePanel,#crmMarketingPanel,#crmCampaignPanel,#crmSourcePanel{word-break:break-word!important;overflow-wrap:anywhere!important}
.crm-home-grid{align-items:stretch!important}.crm-home-card{min-height:118px!important}.crm-home-card strong,.crm-home-card b{line-height:1.25!important}.crm-home-card small,.crm-home-sub{color:var(--crm-final-muted)!important}
button,a[role="button"],.crm-home-btn,.crm-home-open{touch-action:manipulation!important;white-space:normal!important;text-align:center!important}
button:disabled{opacity:.55!important;cursor:not-allowed!important;transform:none!important;box-shadow:none!important}
table{width:100%!important;border-collapse:separate!important;border-spacing:0!important}th,td{vertical-align:top!important;line-height:1.55!important}th{font-weight:900!important;color:#374151!important;background:#f9fafb!important}
.crm-final-empty{border:1px dashed var(--crm-final-line)!important;border-radius:18px!important;background:#fff!important;padding:18px!important;color:var(--crm-final-muted)!important;text-align:center!important;font-weight:800!important}
.crm-final-help{margin-top:10px;border:1px solid #bbf7d0;background:#ecfdf5;color:#065f46;border-radius:16px;padding:12px 14px;font-size:13px;font-weight:800;line-height:1.7}
#crmUxToast{z-index:2147483647!important}
#crmUnifiedPanel,#crmLineOpsPanel,#crmBulkSafePanel,#crmListWorkbenchPanel,#crmCustomerSmartPanel,#crmInquiryRowPanel,#crmInquiryActionsPanel,#crmFollowTemplatePanel,#crmTemplatePanel,#crmMarketingPanel,#crmCampaignPanel,#crmSourcePanel{z-index:2147483000!important}
#crmUnifiedPanel{z-index:2147483200!important}.crm-final-menu-button{z-index:2147483300!important}
@media(max-width:760px){.crm-home-wrap,.crm-admin-wrap,.admin-wrap,main,.main{padding-left:10px!important;padding-right:10px!important}.crm-home-card{min-height:auto!important}.crm-home-grid{grid-template-columns:1fr!important}.crm-home-task{grid-template-columns:1fr!important}table,thead,tbody,tr,th,td{display:block!important}thead{display:none!important}tr{border:1px solid var(--crm-final-line)!important;border-radius:16px!important;background:#fff!important;margin:10px 0!important;padding:10px!important}td{border:none!important;padding:6px 0!important}button,a[role="button"]{min-height:44px!important}#crmUnifiedPanel,#crmLineOpsPanel,#crmBulkSafePanel,#crmListWorkbenchPanel,#crmCustomerSmartPanel,#crmInquiryRowPanel,#crmInquiryActionsPanel,#crmFollowTemplatePanel,#crmTemplatePanel,#crmMarketingPanel,#crmCampaignPanel,#crmSourcePanel{padding-bottom:72px!important}}
</style>`;
  const script = `<script id="crm-final-layout-cleanup-script">
(()=>{if(window.__crmFinalLayoutCleanup)return;window.__crmFinalLayoutCleanup=1;const $$=(s,r=document)=>Array.from(r.querySelectorAll(s));const textMap=new Map([['CRM実務強化パネル','実務管理'],['CRM総合強化ダッシュボード','総合ダッシュボード'],['今日のCRMダッシュボード','今日のダッシュボード'],['問い合わせ一覧アクション','問い合わせ一覧'],['問い合わせ→次アクション','問い合わせ対応'],['フォロー→LINE','フォローLINE'],['LINEテンプレ管理','LINEテンプレ'],['マーケDB','マーケティング'],['成約・流入','成約/流入'],['予約連携 要確認','予約要確認']]);function cleanText(){const nodes=$$('button,a,h1,h2,h3,h4,span,div,strong,b').filter(el=>el.childNodes.length===1&&el.childNodes[0].nodeType===3);nodes.forEach(el=>{const t=(el.textContent||'').trim();if(textMap.has(t))el.textContent=textMap.get(t)})}function fixLayout(){document.body.classList.add('crm-final-ready');$$('.crm-home-wrap,.crm-admin-wrap,.admin-wrap,main,.main').forEach(el=>el.classList.add('crm-final-shell'));$$('button,a[role="button"]').forEach(el=>{if(/CRMメニュー|メニュー/.test((el.textContent||'').trim()))el.classList.add('crm-final-menu-button')});$$('ul,ol').forEach(list=>{if(list.children.length===0&&!list.dataset.finalEmpty){list.dataset.finalEmpty='1';const li=document.createElement('li');li.className='crm-final-empty';li.textContent='表示できる項目はまだありません';list.appendChild(li)}});$$('#crmUnifiedPanel,#crmLineOpsPanel,#crmBulkSafePanel,#crmListWorkbenchPanel,#crmCustomerSmartPanel,#crmInquiryRowPanel,#crmInquiryActionsPanel,#crmFollowTemplatePanel,#crmTemplatePanel,#crmMarketingPanel,#crmCampaignPanel,#crmSourcePanel').forEach(panel=>{if(!panel.dataset.finalHelp){panel.dataset.finalHelp='1';const help=document.createElement('div');help.className='crm-final-help';help.textContent='迷ったら「開く」または「作成」から進めてください。操作後は必要に応じてLINE運用・一覧操作・顧客スマートで確認できます。';panel.appendChild(help)}})}function preventDoubleClick(){document.addEventListener('click',e=>{const b=e.target.closest('button');if(!b||b.dataset.finalNoGuard)return;if(/作成|実行|送信済み|予約化|保存/.test((b.textContent||''))){b.dataset.finalNoGuard='1';setTimeout(()=>delete b.dataset.finalNoGuard,900)}})}function run(){cleanText();fixLayout()}preventDoubleClick();run();new MutationObserver(run).observe(document.body,{childList:true,subtree:true});})();
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
        return json({ ...data, finalLayoutCleanupBuild: BUILD });
      }
      const res = await app.fetch(request, env, ctx);
      const ct = res.headers.get("content-type") || "";
      if(request.method === "GET" && ct.includes("text/html")){
        return new Response(injectFinalCleanup(await res.text()), { status: res.status, headers: res.headers });
      }
      return res;
    }catch(e){
      return json({ ok:false, build:BUILD, message:String(e && e.message || e) }, 500);
    }
  }
};
