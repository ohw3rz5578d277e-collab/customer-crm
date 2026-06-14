// ======================================================
// CUSTOMER CRM / MOBILE FIRST UX WRAPPER
// build: customer-crm-api-mobile-first-ux-20260614-01
// - スマホ片手操作最適化
// - 下部固定ナビ / モーダル全画面化
// - 横スクロール防止 / 表のカード化 / 入力欄拡大
// ======================================================

import app from "./production-index-crm-final-layout-cleanup.js";

const BUILD = "customer-crm-api-mobile-first-ux-20260614-01";

function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" }
  });
}

function injectMobileFirstUx(html){
  if(!html || html.includes("crm-mobile-first-ux-script")) return html;
  const style = `<style id="crm-mobile-first-ux-style">
:root{--mf-green:#028760;--mf-dark:#111827;--mf-muted:#6b7280;--mf-line:#e5e7eb;--mf-bg:#f7f8fa;--mf-safe:env(safe-area-inset-bottom,0px)}
html,body{max-width:100%!important;overflow-x:hidden!important;-webkit-text-size-adjust:100%!important;background:var(--mf-bg)!important}
body{padding-bottom:calc(86px + var(--mf-safe))!important}
input,select,textarea{font-size:16px!important;min-height:46px!important;border-radius:14px!important;max-width:100%!important}
button,a[role="button"],.crm-home-btn,.crm-home-open{min-height:46px!important;border-radius:999px!important;font-weight:900!important;letter-spacing:.01em!important}
.crm-mf-bottom{position:fixed;left:10px;right:10px;bottom:calc(10px + var(--mf-safe));z-index:2147483400;display:none;background:rgba(255,255,255,.96);border:1px solid rgba(2,135,96,.18);box-shadow:0 18px 44px rgba(15,23,42,.18);border-radius:24px;padding:8px;backdrop-filter:blur(14px);grid-template-columns:repeat(5,1fr);gap:6px}
.crm-mf-bottom button{appearance:none;border:0;background:#f3f4f6;color:#111827;border-radius:18px;min-height:52px;padding:6px 4px;font-size:11px;line-height:1.25;display:flex;align-items:center;justify-content:center;flex-direction:column;gap:2px}
.crm-mf-bottom button strong{font-size:17px;line-height:1}.crm-mf-bottom button[data-primary="1"]{background:var(--mf-green);color:#fff}
.crm-mf-fab{position:fixed;right:14px;bottom:calc(98px + var(--mf-safe));z-index:2147483390;width:58px;height:58px;border-radius:999px;border:0;background:var(--mf-green);color:#fff;font-weight:900;box-shadow:0 18px 38px rgba(2,135,96,.32);display:none}
.crm-mf-scrolltop{position:fixed;left:14px;bottom:calc(98px + var(--mf-safe));z-index:2147483390;width:52px;height:52px;border-radius:999px;border:1px solid var(--mf-line);background:#fff;color:#111827;font-weight:900;box-shadow:0 14px 34px rgba(15,23,42,.16);display:none}
@media(max-width:760px){
  .crm-mf-bottom,.crm-mf-fab,.crm-mf-scrolltop{display:grid!important}.crm-mf-fab,.crm-mf-scrolltop{display:block!important}
  .crm-home-wrap,.crm-admin-wrap,.admin-wrap,main,.main{width:100%!important;max-width:100%!important;padding-left:10px!important;padding-right:10px!important}
  .crm-home-hero,.crm-home-card,.crm-home-panel,.crm-final-shell{border-radius:20px!important;margin-left:0!important;margin-right:0!important}
  .crm-home-grid,.crm-home-actions,.crm-home-task{display:grid!important;grid-template-columns:1fr!important;gap:10px!important}
  #crmUnifiedPanel,#crmLineOpsPanel,#crmBulkSafePanel,#crmListWorkbenchPanel,#crmCustomerSmartPanel,#crmInquiryRowPanel,#crmInquiryActionsPanel,#crmFollowTemplatePanel,#crmTemplatePanel,#crmMarketingPanel,#crmCampaignPanel,#crmSourcePanel{position:fixed!important;inset:8px 8px calc(84px + var(--mf-safe)) 8px!important;width:auto!important;max-width:none!important;max-height:none!important;border-radius:24px!important;overflow:auto!important;padding:18px!important}
  table{display:block!important;width:100%!important;overflow:visible!important}thead{display:none!important}tbody,tr,td{display:block!important;width:100%!important}tr{box-shadow:0 10px 26px rgba(15,23,42,.08)!important}td:before{content:attr(data-label);display:block;font-size:11px;font-weight:900;color:var(--mf-muted);margin-bottom:2px}
  .crm-mf-hidden-mobile{display:none!important}
}
</style>`;
  const script = `<script id="crm-mobile-first-ux-script">
(()=>{if(window.__crmMobileFirstUx)return;window.__crmMobileFirstUx=1;const $=(s,r=document)=>r.querySelector(s);const $$=(s,r=document)=>Array.from(r.querySelectorAll(s));const actions=[['今日','◎',()=>clickText('今日やること')],['LINE','✉',()=>clickText('LINE運用')],['顧客','人',()=>clickText('顧客スマート')],['一覧','表',()=>clickText('一覧操作')],['メニュー','≡',()=>clickText('CRMメニュー')]];function clickText(t){const el=$$('button,a').find(x=>(x.textContent||'').trim().includes(t));if(el)el.click();else toast('まだ画面に表示されていません')}function toast(msg){let t=$('#crmMfToast');if(!t){t=document.createElement('div');t.id='crmMfToast';t.style.cssText='position:fixed;left:18px;right:18px;bottom:calc(168px + env(safe-area-inset-bottom,0px));z-index:2147483647;background:#111827;color:#fff;border-radius:16px;padding:12px 14px;text-align:center;font-weight:900;box-shadow:0 18px 44px rgba(0,0,0,.24);opacity:0;transition:.2s';document.body.appendChild(t)}t.textContent=msg;t.style.opacity='1';setTimeout(()=>t.style.opacity='0',1400)}function addBottom(){if($('#crmMfBottom'))return;const nav=document.createElement('div');nav.id='crmMfBottom';nav.className='crm-mf-bottom';actions.forEach((a,i)=>{const b=document.createElement('button');b.type='button';b.dataset.primary=i===4?'1':'0';b.innerHTML='<strong>'+a[1]+'</strong><span>'+a[0]+'</span>';b.onclick=a[2];nav.appendChild(b)});document.body.appendChild(nav);const fab=document.createElement('button');fab.type='button';fab.className='crm-mf-fab';fab.textContent='＋';fab.onclick=()=>clickText('CRMメニュー');document.body.appendChild(fab);const top=document.createElement('button');top.type='button';top.className='crm-mf-scrolltop';top.textContent='↑';top.onclick=()=>scrollTo({top:0,behavior:'smooth'});document.body.appendChild(top)}function labelTables(){ $$('table').forEach(table=>{const heads=$$('th',table).map(th=>(th.textContent||'').trim());$$('tbody tr',table).forEach(tr=>{$$('td',tr).forEach((td,i)=>{if(heads[i]&&!td.dataset.label)td.dataset.label=heads[i]})})})}function simplify(){if(innerWidth>760)return;$$('button,a').forEach(el=>{const t=(el.textContent||'').trim();if(t.length>12)el.title=t});}function run(){addBottom();labelTables();simplify()}run();new MutationObserver(run).observe(document.body,{childList:true,subtree:true});})();
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
        return json({ ...data, mobileFirstUxBuild: BUILD });
      }
      const res = await app.fetch(request, env, ctx);
      const ct = res.headers.get("content-type") || "";
      if(request.method === "GET" && ct.includes("text/html")){
        return new Response(injectMobileFirstUx(await res.text()), { status: res.status, headers: res.headers });
      }
      return res;
    }catch(e){
      return json({ ok:false, build:BUILD, message:String(e && e.message || e) }, 500);
    }
  }
};