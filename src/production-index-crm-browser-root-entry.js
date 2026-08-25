import app from "./production-index-crm-delivery-deadline-alerts-entry.js";
import { handleLineContextEvents, lineContextHealth } from "./crm-line-context-events.mjs";
import { handleInternalCustomerDetail, internalCustomerDetailHealth } from "./crm-internal-customer-detail.mjs";
import { handleReconciliationReview, patchReconciliationHealth } from "./crm-reconciliation-review.mjs";
import { handleCustomerIdentityResolver, customerIdentityHealth } from "./customer-identity-resolver.mjs";
import { handleCanonicalLineFollow, handleGuardedCustomerUpsert, canonicalCustomerGuardHealth } from "./crm-canonical-customer-guards.mjs";
import { handleIdentityDamageDiagnostic, identityDamageDiagnosticHealth } from "./crm-identity-damage-diagnostic.mjs";
import { reservationInternalUser, reservationHandoffBasePath, reservationBrowserHandoffHealth, patchReservationHandoffHtml } from "./crm-reservation-browser-handoff.mjs";

const BUILD = "customer-crm-api-browser-root-20260825-owner-desktop-hotfix-02";
const RESPONSIVE_MARKER = "crm-production-desktop-owner-hotfix-20260825";

function copyHeaders(response){
  const headers = new Headers(response.headers);
  headers.delete("content-length");
  headers.set("cache-control", "no-store, no-cache, must-revalidate, max-age=0");
  headers.set("x-crm-build", BUILD);
  return headers;
}

function isBrowserNavigation(request){
  const accept = request.headers.get("accept") || "";
  const mode = request.headers.get("sec-fetch-mode") || "";
  const dest = request.headers.get("sec-fetch-dest") || "";
  return accept.includes("text/html") || mode === "navigate" || dest === "document";
}

async function patchHealth(response, env){
  const raw = await response.text();
  let data = {};
  try { data = raw ? JSON.parse(raw) : {}; } catch (_) {}
  const lineContext = await lineContextHealth(env);
  const customerDetail = internalCustomerDetailHealth(env);
  const customerIdentity = customerIdentityHealth(env);
  const canonicalGuard = canonicalCustomerGuardHealth(env);
  const identityDiagnostic = identityDamageDiagnosticHealth(env);
  const reservationHandoff = reservationBrowserHandoffHealth();
  const headers = copyHeaders(response);
  headers.set("content-type", "application/json; charset=utf-8");
  return new Response(JSON.stringify({
    ...data,
    base_build: data.build || data.base_build || "",
    build: BUILD,
    browser_root_redirect: "/admin",
    responsive_admin_hotfix: true,
    responsive_admin_hotfix_marker: RESPONSIVE_MARKER,
    responsive_desktop_bottom_nav: false,
    ...lineContext,
    ...customerDetail,
    ...customerIdentity,
    ...canonicalGuard,
    ...identityDiagnostic,
    ...reservationHandoff
  }, null, 2), { status: response.status, headers });
}

async function patchAdminUi(response, url){
  const ct = response.headers.get("content-type") || "";
  if(response.status !== 200 || !ct.includes("text/html")) return response;
  let body = await response.text();
  const token = url.searchParams.get("token") || "";
  const href = "/admin/customer-id-reconciliation" + (token ? "?token=" + encodeURIComponent(token) : "");
  const link = `<a id="crmReconciliationLink" href="${href}" style="position:fixed;left:12px;bottom:12px;z-index:9998;background:#17202a;color:#fff;text-decoration:none;border-radius:999px;padding:10px 14px;font:800 13px -apple-system,BlinkMacSystemFont,'Noto Sans JP',sans-serif;box-shadow:0 8px 24px rgba(0,0,0,.18)">顧客ID照合</a>`;
  const style = `<style id="${RESPONSIVE_MARKER}">
html,body{width:100%!important;max-width:none!important;min-width:0!important}
@media (min-width:1024px){
  body{padding-bottom:24px!important;overflow-x:hidden!important}
  .app{width:min(calc(100vw - 48px),1600px)!important;max-width:none!important;margin-left:auto!important;margin-right:auto!important;padding-left:20px!important;padding-right:20px!important;box-sizing:border-box!important}
  .crm-final-shell,.crm-home-wrap,.crm-admin-wrap,.admin-wrap{width:100%!important;max-width:none!important;margin-left:auto!important;margin-right:auto!important;box-sizing:border-box!important}
  .crm-today-dash,.crm-delivery-panel{width:100%!important;max-width:none!important;margin-left:0!important;margin-right:0!important;box-sizing:border-box!important}
  #crmUxFab,#crmCustomerReturnFab,.crm-customer-return-fab,#crmCustomerReturnTopCard,.crm-customer-return-top-card,#crmReconciliationLink,#crmInstaNav,.crm-bottom-nav,.crm-mf-bottom,.crm-mf-fab,.crm-mf-scrolltop,.crm-top-menu-btn,.crm-side-menu{display:none!important;visibility:hidden!important;pointer-events:none!important}
  .crm-stable-customer-panel.open{position:relative!important;inset:auto!important;right:auto!important;top:auto!important;bottom:auto!important;width:min(calc(100vw - 48px),1600px)!important;max-width:none!important;max-height:none!important;margin:16px auto 24px!important;z-index:auto!important;box-shadow:0 12px 34px rgba(15,23,42,.10)!important}
  .crm-stable-customer-list{max-height:none!important}
  .grid,#summary.grid{grid-template-columns:repeat(4,minmax(190px,1fr))!important}
  .marketing{grid-template-columns:minmax(0,1.35fr) minmax(0,1fr)!important}
  .crm-today-kpis{grid-template-columns:repeat(4,minmax(180px,1fr))!important}
  .crm-today-body{grid-template-columns:minmax(0,1.15fr) minmax(0,.85fr)!important}
  .toolbar,.crm-stable-customer-search{display:flex!important;align-items:center!important;gap:10px!important;flex-wrap:nowrap!important}
  .toolbar input,.crm-stable-customer-search input{flex:1 1 auto!important;min-width:280px!important}
  .table-wrap,.crm-stable-customer-list{width:100%!important;max-width:100%!important}
  table{width:100%!important}
}
@media (min-width:768px) and (max-width:1023px){
  body{padding-bottom:20px!important;overflow-x:hidden!important}
  .app{width:calc(100vw - 24px)!important;max-width:none!important;margin-left:auto!important;margin-right:auto!important;padding-left:12px!important;padding-right:12px!important;box-sizing:border-box!important}
  .crm-final-shell,.crm-home-wrap,.crm-admin-wrap,.admin-wrap{width:100%!important;max-width:none!important;margin-left:auto!important;margin-right:auto!important;box-sizing:border-box!important}
  .crm-today-dash,.crm-delivery-panel{width:100%!important;max-width:none!important;margin-left:0!important;margin-right:0!important;box-sizing:border-box!important}
  #crmUxFab,#crmCustomerReturnFab,.crm-customer-return-fab,#crmCustomerReturnTopCard,.crm-customer-return-top-card,#crmReconciliationLink,#crmInstaNav,.crm-bottom-nav,.crm-mf-bottom,.crm-mf-fab,.crm-mf-scrolltop,.crm-top-menu-btn,.crm-side-menu{display:none!important;visibility:hidden!important;pointer-events:none!important}
  .crm-stable-customer-panel.open{position:relative!important;inset:auto!important;right:auto!important;top:auto!important;bottom:auto!important;width:calc(100vw - 32px)!important;max-width:none!important;max-height:none!important;margin:12px auto 20px!important;z-index:auto!important}
  .grid,#summary.grid{grid-template-columns:repeat(2,minmax(0,1fr))!important}
  .marketing,.crm-today-body,.crm-today-mini-grid{grid-template-columns:repeat(2,minmax(0,1fr))!important}
}
@media (max-width:767px){
  body{max-width:100%!important;overflow-x:hidden!important}
  .app,.crm-home-wrap,.crm-admin-wrap,.admin-wrap,.crm-final-shell{width:100%!important;max-width:100%!important;margin-left:0!important;margin-right:0!important;box-sizing:border-box!important}
}
</style>`;
  const script = `<script id="crm-owner-desktop-layout-hotfix-script">
(()=>{
  if(window.__crmOwnerDesktopLayoutHotfix20260825)return;
  window.__crmOwnerDesktopLayoutHotfix20260825=1;
  function desktopHost(){
    return document.querySelector('.app') || document.querySelector('.crm-final-shell') || document.querySelector('.crm-admin-wrap') || document.querySelector('main');
  }
  function reconcileDesktopLayout(){
    if(window.innerWidth < 768) return;
    const host=desktopHost();
    const list=document.getElementById('crmStableCustomerPanel');
    if(host && list && list.parentElement!==host) host.appendChild(list);
  }
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',reconcileDesktopLayout,{once:true});else reconcileDesktopLayout();
  window.addEventListener('resize',reconcileDesktopLayout,{passive:true});
  new MutationObserver(()=>reconcileDesktopLayout()).observe(document.documentElement,{childList:true,subtree:true});
})();
</script>`;
  if(!body.includes(RESPONSIVE_MARKER)){
    body = body.includes("</head>") ? body.replace("</head>", style + "</head>") : style + body;
  }
  if(!body.includes('id="crm-owner-desktop-layout-hotfix-script"')){
    body = body.includes("</body>") ? body.replace("</body>", script + "</body>") : body + script;
  }
  if(!body.includes('id="crmReconciliationLink"')){
    body = body.includes("</body>") ? body.replace("</body>", link + "</body>") : body + link;
  }
  const headers = copyHeaders(response);
  headers.set("content-type", "text/html; charset=utf-8");
  return new Response(body,{status:response.status,headers});
}

async function injectReviewLink(response, url){
  return patchAdminUi(response, url);
}

async function patchAdminForReservationHandoff(response, request, url, env){
  const internal = reservationInternalUser(request, env);
  if(!internal) return injectReviewLink(response, url);
  const basePath = reservationHandoffBasePath(request, env);
  if(!basePath){
    const headers = copyHeaders(response);
    headers.set("content-type", "application/json; charset=utf-8");
    return new Response(JSON.stringify({ok:false,error:"invalid_reservation_handoff_base_path"}), {status:400,headers});
  }
  const adminResponse = await patchAdminUi(response, url);
  const ct = adminResponse.headers.get("content-type") || "";
  if(adminResponse.status !== 200 || !ct.includes("text/html")) return adminResponse;
  const body = patchReservationHandoffHtml(await adminResponse.text(), basePath);
  const headers = copyHeaders(adminResponse);
  headers.set("content-type", "text/html; charset=utf-8");
  headers.set("x-crm-reservation-handoff", "reservation-app");
  return new Response(body,{status:adminResponse.status,statusText:adminResponse.statusText,headers});
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);

    const diagnosticResponse = await handleIdentityDamageDiagnostic(request, env);
    if(diagnosticResponse) return diagnosticResponse;

    const canonicalFollowResponse = await handleCanonicalLineFollow(request, env);
    if(canonicalFollowResponse) return canonicalFollowResponse;

    const guardedUpsertResponse = await handleGuardedCustomerUpsert(request, env, app, ctx);
    if(guardedUpsertResponse) return guardedUpsertResponse;

    const identityResponse = await handleCustomerIdentityResolver(request, env);
    if(identityResponse) return identityResponse;

    const reviewResponse = await handleReconciliationReview(request, env);
    if(reviewResponse) return reviewResponse;

    const customerDetailResponse = await handleInternalCustomerDetail(request, env);
    if(customerDetailResponse) return customerDetailResponse;

    const lineContextResponse = await handleLineContextEvents(request, env);
    if(lineContextResponse) return lineContextResponse;

    if(request.method === "GET" && (url.pathname === "/" || url.pathname === "/index.html") && isBrowserNavigation(request)){
      const target = new URL("/admin", url.origin);
      return Response.redirect(target.toString(), 302);
    }

    let response = await app.fetch(request, env, ctx);
    if(request.method === "GET" && (url.pathname === "/health" || url.pathname === "/api/crm-health-check")){
      response = await patchHealth(response, env);
      return patchReconciliationHealth(response);
    }
    if(request.method === "GET" && url.pathname === "/admin") return patchAdminForReservationHandoff(response,request,url,env);
    return response;
  }
};
