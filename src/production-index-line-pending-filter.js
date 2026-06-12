// ======================================================
// CUSTOMER CRM API / LINE PENDING FILTER WRAPPER
// build: customer-crm-api-line-pending-filter-20260613-01
// ======================================================

import app from "./production-index-line-badges.js";

const BUILD = "customer-crm-api-line-pending-filter-20260613-01";

function securityHeaders(headers = {}) {
  const h = new Headers(headers);
  h.set("cache-control", "no-store, no-cache, must-revalidate, max-age=0");
  h.set("pragma", "no-cache");
  h.set("expires", "0");
  h.set("x-robots-tag", "noindex, nofollow, noarchive");
  h.set("x-content-type-options", "nosniff");
  h.set("referrer-policy", "no-referrer");
  h.set("x-frame-options", "DENY");
  return h;
}

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: securityHeaders({ "content-type": "application/json; charset=utf-8" })
  });
}

function injectPendingFilterUi(html) {
  if (!html || !html.includes("</head>") || !html.includes("</body>")) return html;

  const style = `<style id="crm-line-pending-filter-style">
.crm-line-pending-filter-btn{display:inline-flex;align-items:center;gap:6px;margin-left:6px;border:1px solid #fed7aa;background:#fff7ed;color:#9a3412;border-radius:999px;padding:8px 11px;font-size:.78rem;font-weight:950;cursor:pointer;white-space:nowrap}.crm-line-pending-filter-btn.active{background:#ea580c;color:#fff;border-color:#ea580c;box-shadow:0 8px 18px rgba(234,88,12,.22)}.crm-line-pending-filter-count{display:inline-flex;align-items:center;justify-content:center;min-width:22px;height:22px;padding:0 7px;border-radius:999px;background:rgba(255,255,255,.86);color:#9a3412;font-size:.72rem;font-weight:950}.crm-line-pending-filter-btn.active .crm-line-pending-filter-count{background:rgba(255,255,255,.22);color:#fff}.crm-line-pending-filter-empty{display:none;margin:12px 0;border:1px dashed #fed7aa;background:#fff7ed;color:#9a3412;border-radius:18px;padding:14px;font-size:.9rem;font-weight:900;line-height:1.7}.crm-line-pending-filter-empty.show{display:block}.crm-line-pending-filter-hidden{display:none!important}@media(max-width:820px){.crm-line-pending-filter-btn{margin:6px 0 0;padding:9px 12px;width:max-content}.crm-line-pending-filter-empty{font-size:.84rem}}
</style>`;

  const script = `<script id="crm-line-pending-filter-script">
(function(){
  if(window.__crmLinePendingFilterInstalled)return;
  window.__crmLinePendingFilterInstalled=true;
  var filterOn=false;
  var pendingMap={};
  var pendingLoaded=false;
  function qs(sel,root){return (root||document).querySelector(sel)}
  function qsa(sel,root){return Array.prototype.slice.call((root||document).querySelectorAll(sel))}
  function isAdminPage(){return /\/admin\/?$/.test(location.pathname)}
  function toolbar(){return qs('.card .toolbar')||qs('.toolbar')}
  function rows(){return qsa('[data-detail]').map(function(btn){return {button:btn,id:btn.getAttribute('data-detail'),row:btn.closest('tr')}}).filter(function(x){return x.id&&x.row})}
  function ensureUi(){
    if(!isAdminPage())return;
    var tb=toolbar();
    if(!tb)return;
    if(!qs('#crmLinePendingFilterBtn')){
      var b=document.createElement('button');
      b.type='button';
      b.id='crmLinePendingFilterBtn';
      b.className='crm-line-pending-filter-btn';
      b.innerHTML='LINE未送信のみ <span class="crm-line-pending-filter-count" id="crmLinePendingFilterCount">0</span>';
      b.onclick=function(){filterOn=!filterOn;applyFilter(true)};
      tb.appendChild(b);
    }
    if(!qs('#crmLinePendingFilterEmpty')){
      var empty=document.createElement('div');
      empty.id='crmLinePendingFilterEmpty';
      empty.className='crm-line-pending-filter-empty';
      empty.textContent='LINE未送信の顧客はありません。保存済みの文面を作ると、ここに表示されます。';
      var wrap=qs('.tablewrap')||qs('table');
      if(wrap&&wrap.parentNode)wrap.parentNode.insertBefore(empty,wrap);
    }
  }
  async function loadPending(){
    try{
      var r=await fetch('/api/line-message-logs/pending-by-customer?_='+Date.now());
      var j=await r.json();
      if(!r.ok||j.ok===false)throw new Error(j.message||'pending fetch failed');
      pendingMap=j.map||{};
      pendingLoaded=true;
      applyFilter(false);
    }catch(e){console.warn(e)}
  }
  function updateCount(){
    var count=Object.keys(pendingMap||{}).filter(function(id){return pendingMap[id]&&Number(pendingMap[id].pending_count||0)>0}).length;
    var el=qs('#crmLinePendingFilterCount');
    if(el)el.textContent=String(count);
  }
  function applyFilter(showToast){
    ensureUi();
    updateCount();
    var btn=qs('#crmLinePendingFilterBtn');
    if(btn)btn.classList.toggle('active',!!filterOn);
    var visible=0;
    rows().forEach(function(x){
      var has=!!(pendingMap[x.id]&&Number(pendingMap[x.id].pending_count||0)>0);
      var hide=filterOn&&!has;
      x.row.classList.toggle('crm-line-pending-filter-hidden',hide);
      if(!hide)visible++;
    });
    var empty=qs('#crmLinePendingFilterEmpty');
    if(empty)empty.classList.toggle('show',filterOn&&visible===0&&pendingLoaded);
    if(showToast&&window.crmReloadLinePendingBadges)window.crmReloadLinePendingBadges();
  }
  function refresh(){if(!isAdminPage())return;ensureUi();loadPending();setTimeout(function(){applyFilter(false)},300)}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',refresh);else setTimeout(refresh,80);
  var mo=new MutationObserver(function(){if(!isAdminPage())return;clearTimeout(window.__crmLinePendingFilterTimer);window.__crmLinePendingFilterTimer=setTimeout(function(){ensureUi();applyFilter(false)},120)});
  mo.observe(document.documentElement,{childList:true,subtree:true});
  window.crmLinePendingFilterOff=function(){filterOn=false;applyFilter(false)};
  window.crmLinePendingFilterOn=function(){filterOn=true;applyFilter(false)};
  window.crmReloadLinePendingFilter=function(){loadPending()};
})();
</script>`;

  return html.replace("</head>", style + "</head>").replace("</body>", script + "</body>");
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/" || url.pathname === "/health" || url.pathname === "/api/health") {
      return json({ ok: true, service: "customer-crm-api", build: BUILD, hasDb: !!env.DB, secure: true, auth: "cloudflare-access-google" });
    }

    const res = await app.fetch(request, env, ctx);
    const contentType = res.headers.get("content-type") || "";

    if (contentType.includes("text/html")) {
      const body = injectPendingFilterUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }

    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
