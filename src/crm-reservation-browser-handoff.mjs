export const RESERVATION_INTERNAL_PRINCIPAL = "reservation-app";
export const RESERVATION_HANDOFF_BASE_PATH = "/customer-management";
export const RESERVATION_INTERNAL_USER = Object.freeze({
  email: "reservation-app@internal.invalid",
  role: "admin",
  principal: "reservation_service"
});

const HANDOFF_MARKER = "crm-reservation-handoff-20260822";

function raw(v){ return v === undefined || v === null ? "" : String(v); }
function constantTimeEqual(a, b){
  const aa = new TextEncoder().encode(raw(a));
  const bb = new TextEncoder().encode(raw(b));
  const max = Math.max(aa.length, bb.length);
  let diff = aa.length ^ bb.length;
  for(let i = 0; i < max; i++) diff |= (aa[i] || 0) ^ (bb[i] || 0);
  return max > 0 && diff === 0;
}

export function reservationInternalUser(request, env){
  const expected = raw(env && env.CRM_INTERNAL_TOKEN);
  const supplied = raw(request && request.headers && request.headers.get("x-internal-token"));
  const principal = raw(request && request.headers && request.headers.get("x-crm-internal-principal"));
  if(!expected || !supplied) return null;
  if(principal !== RESERVATION_INTERNAL_PRINCIPAL) return null;
  if(!constantTimeEqual(expected, supplied)) return null;
  return { ...RESERVATION_INTERNAL_USER };
}

export function reservationHandoffBasePath(request, env){
  if(!reservationInternalUser(request, env)) return "";
  const value = raw(request && request.headers && request.headers.get("x-crm-handoff-base-path"));
  return value === RESERVATION_HANDOFF_BASE_PATH ? value : "";
}

export function reservationBrowserHandoffHealth(){
  return {
    reservation_browser_handoff_contract: true,
    reservation_browser_handoff_internal_principal: true,
    reservation_browser_handoff_principal: RESERVATION_INTERNAL_PRINCIPAL,
    reservation_browser_handoff_base_path: RESERVATION_HANDOFF_BASE_PATH,
    direct_cloudflare_access_protection: true
  };
}

export function patchReservationHandoffHtml(source, basePath){
  const html = raw(source);
  if(basePath !== RESERVATION_HANDOFF_BASE_PATH || !html || html.includes(HANDOFF_MARKER)) return html;
  const baseJson = JSON.stringify(RESERVATION_HANDOFF_BASE_PATH);
  const bootstrap = `<script id="${HANDOFF_MARKER}">
(function(){
  if(window.__crmReservationHandoff20260822)return;
  window.__crmReservationHandoff20260822=true;
  var BASE=${baseJson};
  window.__CRM_BASE_PATH__=BASE;
  function sameOriginApi(value){
    if(typeof value==='string'){
      if(value==='/api'||value.indexOf('/api/')===0)return BASE+value;
      return value;
    }
    if(typeof URL!=='undefined'&&value instanceof URL){
      if(value.origin===location.origin&&(value.pathname==='/api'||value.pathname.indexOf('/api/')===0)){
        var u=new URL(value.toString());u.pathname=BASE+u.pathname;return u;
      }
    }
    return value;
  }
  var originalFetch=window.fetch.bind(window);
  window.fetch=function(input,init){return originalFetch(sameOriginApi(input),init)};
  if(window.XMLHttpRequest&&XMLHttpRequest.prototype&&XMLHttpRequest.prototype.open){
    var originalOpen=XMLHttpRequest.prototype.open;
    XMLHttpRequest.prototype.open=function(method,url){arguments[1]=sameOriginApi(url);return originalOpen.apply(this,arguments)};
  }
  function fixNavigation(root){
    var scope=root&&root.querySelectorAll?root:document;
    scope.querySelectorAll('a[href="/admin"]').forEach(function(a){a.setAttribute('href',BASE)});
    scope.querySelectorAll('form[action="/admin"]').forEach(function(f){f.setAttribute('action',BASE)});
  }
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',function(){fixNavigation(document)},{once:true});else fixNavigation(document);
  new MutationObserver(function(records){records.forEach(function(r){r.addedNodes.forEach(function(n){if(n&&n.nodeType===1)fixNavigation(n)})})}).observe(document.documentElement,{childList:true,subtree:true});
})();
</script>`;
  if(/<head(?:\s[^>]*)?>/i.test(html)) return html.replace(/<head(\s[^>]*)?>/i, (m) => m + bootstrap);
  return bootstrap + html;
}
