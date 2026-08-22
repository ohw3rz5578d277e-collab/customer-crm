const BUILD = 'crm-marketing-browser-polish-20260823-01';

function copyHeaders(response) {
  const headers = new Headers(response.headers);
  headers.delete('content-length');
  return headers;
}

export async function patchMarketingBrowserPolish(response) {
  const ct = response.headers.get('content-type') || '';
  if (response.status !== 200 || !ct.includes('text/html')) return response;
  let html = await response.text();
  if (html.includes(BUILD)) {
    return new Response(html, { status: response.status, statusText: response.statusText, headers: copyHeaders(response) });
  }
  const addon = `
<style id="${BUILD}">
.crmMarketingShell .crmMNav button,.crmMarketingShell .crmMBottom button{min-height:44px}.crmMarketingShell .crmMCard h3{overflow-wrap:anywhere}.crmMarketingShell .crmMValue,.crmMarketingShell .crmMMuted,.crmMarketingShell td{overflow-wrap:anywhere}.crmMarketingShell .crmMSearch input{min-width:0}.crmMarketingShell .crmMTable td[data-crm-mobile-label]::before{display:none}@media(max-width:760px){.crmMarketingShell .crmMTable td[data-crm-mobile-label]{display:grid!important;grid-template-columns:6.5em minmax(0,1fr);gap:8px;align-items:start}.crmMarketingShell .crmMTable td[data-crm-mobile-label]::before{content:attr(data-crm-mobile-label);display:block;color:#64748b;font-size:11px;font-weight:900}.crmMarketingShell .crmMHeader h1{font-size:24px}.crmMarketingShell .crmMSearch button{min-width:64px}.crmMarketingShell .crmMBottom{padding-bottom:max(6px,env(safe-area-inset-bottom))}}@media(min-width:761px){.crmMarketingShell .crmMNav button{min-height:44px}}
</style>
<script id="${BUILD}-script">
(()=>{if(window.__crmMarketingBrowserPolish20260823)return;window.__crmMarketingBrowserPolish20260823=true;const labels=['お客様','顧客状態','最終撮影','撮影回数','LTV','次の提案','LINE状態'];function relabelKpi(){document.querySelectorAll('.crmMarketingShell .crmMCard').forEach(card=>{const t=card.textContent||'';if(t.includes('今月売上')&&t.includes('リピート ¥')&&!t.includes('リピート売上')){card.querySelectorAll('.crmMMuted').forEach(m=>{m.textContent=m.textContent.replace('リピート ¥','リピート売上 ¥').replace('新規 ¥','新規売上 ¥')})}})}function labelMobileRows(){document.querySelectorAll('.crmMarketingShell .crmMTable tbody tr').forEach(row=>{[...row.children].forEach((td,i)=>{if(labels[i])td.setAttribute('data-crm-mobile-label',labels[i])})})}function run(){relabelKpi();labelMobileRows()}run();new MutationObserver(run).observe(document.documentElement,{childList:true,subtree:true});})();
</script>`;
  html = html.includes('</body>') ? html.replace('</body>', addon + '</body>') : html + addon;
  const headers = copyHeaders(response);
  headers.set('content-type', 'text/html; charset=utf-8');
  return new Response(html, { status: response.status, statusText: response.statusText, headers });
}

export function marketingBrowserPolishHealth() {
  return {
    marketing_browser_polish_enabled: true,
    marketing_browser_polish_build: BUILD,
    marketing_mobile_customer_labels: true,
    marketing_line_send_enabled: false
  };
}
