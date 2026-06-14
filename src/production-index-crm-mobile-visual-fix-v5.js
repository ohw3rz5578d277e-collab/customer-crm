// CUSTOMER CRM / MOBILE VISUAL FIX V5
// Forces Instagram-like icon nav and fixes mobile metric cards.
import app from "./production-index-crm-instagram-nav-v4.js";

function inject(html){
  if(!html || html.includes('crm-mobile-visual-fix-v5')) return html;
  const style = `<style id="crm-mobile-visual-fix-v5">
@media(max-width:767px){
  html,body{max-width:100%!important;overflow-x:hidden!important;}
  body{padding-bottom:calc(112px + env(safe-area-inset-bottom))!important;}

  /* Hide old text-based bottom/floating controls. Keep the new icon nav only. */
  .crm-legacy-hidden-by-v5,
  .crm-legacy-hidden-by-insta,
  .crm-legacy-hidden-by-v3,
  .crm-stable-audit-btn,
  .crm-stable-customer-btn,
  #crmStableAuditBtn,
  #crmStableCustomerBtn,
  #crmSettingsMenuBtn,
  #crmLogoutMenuBtn,
  #crmSettingsUnifiedBtn{
    display:none!important;
  }

  #crmInstaNav{
    display:flex!important;
    position:fixed!important;
    left:10px!important;
    right:10px!important;
    bottom:calc(10px + env(safe-area-inset-bottom))!important;
    height:74px!important;
    z-index:2147483800!important;
  }

  /* Make dashboard KPI cards fit mobile instead of horizontal overflow. */
  main,
  section,
  article,
  .crm-home-dashboard,
  .crm-dashboard,
  .crm-panel,
  .crm-card,
  .crm-section{
    max-width:100%!important;
    box-sizing:border-box!important;
  }

  /* Any horizontal metric strip becomes a two-column grid on mobile. */
  .crm-metric-row,
  .crm-summary-row,
  .crm-kpi-row,
  .crm-dashboard-grid,
  .crm-home-grid,
  .crm-card-grid,
  [class*="metric"],
  [class*="summary"],
  [class*="kpi"]{
    max-width:100%!important;
    overflow:visible!important;
  }

  /* Cards that contain big numbers */
  .crm-metric-card,
  .crm-summary-card,
  .crm-kpi-card,
  .crm-stat-card,
  [class*="metric-card"],
  [class*="summary-card"],
  [class*="kpi-card"],
  [class*="stat-card"]{
    min-width:0!important;
    width:auto!important;
    max-width:100%!important;
    box-sizing:border-box!important;
    overflow:hidden!important;
  }

  /* Fallback: common direct child card layout in dashboard sections */
  section > div:has(> div),
  article > div:has(> div){
    max-width:100%!important;
  }

  /* Big numbers should not overflow. */
  .crm-metric-card b,
  .crm-summary-card b,
  .crm-kpi-card b,
  .crm-stat-card b,
  [class*="metric-card"] b,
  [class*="summary-card"] b,
  [class*="kpi-card"] b,
  [class*="stat-card"] b{
    font-size:clamp(26px,8vw,42px)!important;
    line-height:1.05!important;
    white-space:nowrap!important;
    overflow:hidden!important;
    text-overflow:ellipsis!important;
  }
}
</style>`;
  const script = `<script id="crm-mobile-visual-fix-v5-script">
(()=>{
  if(window.__crmMobileVisualFixV5) return;
  window.__crmMobileVisualFixV5 = 1;
  const qsa=(s,r=document)=>Array.from(r.querySelectorAll(s));
  function hideOldFloating(){
    const old=['顧客リストへ','状態確認','設定','ログアウト','ユーザー管理','ユーザー追加'];
    qsa('button,a,div').forEach(el=>{
      if(el.closest('#crmInstaNav')||el.closest('#crmSettingsPanel')||el.closest('#crmStableCustomerPanel')||el.closest('#crmV2DetailPanel')) return;
      const t=(el.textContent||'').trim();
      const st=getComputedStyle(el);
      if((st.position==='fixed'||st.position==='sticky'||st.position==='absolute') && old.some(x=>t===x||t.includes(x))){
        el.classList.add('crm-legacy-hidden-by-v5');
      }
    });
  }
  function fixMetricRows(){
    if(!matchMedia('(max-width: 767px)').matches) return;
    qsa('section,article,main div').forEach(el=>{
      const text=el.textContent||'';
      if(!/(平均顧客単価|平均注文単価|リピート率|顧客数|売上合計|LINE連携率)/.test(text)) return;
      const cards=Array.from(el.children).filter(c=>/¥|%|人|顧客数|単価|率|売上/.test(c.textContent||''));
      if(cards.length>=3 && cards.length<=12){
        el.style.display='grid';
        el.style.gridTemplateColumns='1fr 1fr';
        el.style.gap='10px';
        el.style.overflow='visible';
        el.style.maxWidth='100%';
        cards.forEach(c=>{c.style.minWidth='0';c.style.width='auto';c.style.maxWidth='100%';c.style.overflow='hidden';});
      }
    });
  }
  function boot(){hideOldFloating();fixMetricRows();}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',boot);else boot();
  new MutationObserver(()=>setTimeout(boot,120)).observe(document.documentElement,{childList:true,subtree:true});
})();
</script>`;
  return html.includes('</head>') ? html.replace('</head>', style+'</head>').replace('</body>', script+'</body>') : html+style+script;
}
export default{async fetch(request,env,ctx){const res=await app.fetch(request,env,ctx);const ct=res.headers.get('content-type')||'';if(request.method==='GET'&&ct.includes('text/html')){const html=await res.text();return new Response(inject(html),{status:res.status,headers:res.headers})}return res}};
