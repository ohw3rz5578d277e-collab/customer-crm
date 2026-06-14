// CUSTOMER CRM / CLEAN MOBILE UX V3
// Stable entrypoint + direct cleanup for old floating controls.
import app from "./production-index-crm-customer-list-detail-v2.js";

function injectCleanup(html){
  if(!html || html.includes('crm-direct-floating-cleanup')) return html;
  const style = `<style id="crm-direct-floating-cleanup-style">
.crm-force-hide-floating{display:none!important;visibility:hidden!important;pointer-events:none!important}.crm-stable-customer-btn,#crmStableCustomerBtn,#crmStableCustomerMenuBtn{display:none!important}
@media(max-width:767px){body{padding-bottom:92px!important}.crm-stable-customer-btn,#crmStableCustomerBtn,#crmStableCustomerMenuBtn{display:none!important}}
</style>`;
  const script = `<script id="crm-direct-floating-cleanup">
(()=>{
  if(window.__crmDirectFloatingCleanup) return;
  window.__crmDirectFloatingCleanup = 1;
  const hideWords = ['顧客リストへ','顧客リスト','状態確認','ユーザー管理','ユーザー追加'];
  function clean(){
    document.querySelectorAll('#crmStableCustomerBtn,#crmStableCustomerMenuBtn,.crm-stable-customer-btn').forEach(el=>el.remove());
    document.querySelectorAll('button,a,div').forEach(el=>{
      if(el.closest('#crmStableCustomerPanel') || el.closest('#crmV2DetailPanel') || el.closest('#crmSettingsPanel')) return;
      const text=(el.textContent||'').trim();
      if(!hideWords.some(w=>text===w || text.includes(w))) return;
      const st=getComputedStyle(el);
      if(st.position==='fixed' || st.position==='sticky' || st.position==='absolute'){
        el.classList.add('crm-force-hide-floating');
        el.style.setProperty('display','none','important');
      }
    });
  }
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',clean); else clean();
  new MutationObserver(()=>setTimeout(clean,80)).observe(document.documentElement,{childList:true,subtree:true});
})();
</script>`;
  return html.includes('</head>') ? html.replace('</head>',style+'</head>').replace('</body>',script+'</body>') : html+style+script;
}

export default {
  async fetch(request, env, ctx){
    const res = await app.fetch(request, env, ctx);
    const ct = res.headers.get('content-type') || '';
    if(request.method === 'GET' && ct.includes('text/html')){
      const html = await res.text();
      return new Response(injectCleanup(html), {status:res.status, headers:res.headers});
    }
    return res;
  }
};
