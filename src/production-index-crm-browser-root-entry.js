import app from "./production-index-crm-delivery-deadline-alerts-entry.js";
import { handleLineContextEvents, lineContextHealth } from "./crm-line-context-events.mjs";
import { handleInternalCustomerDetail, internalCustomerDetailHealth } from "./crm-internal-customer-detail.mjs";
import { handleReconciliationReview, patchReconciliationHealth } from "./crm-reconciliation-review.mjs";
import { handleCustomerIdentityResolver, customerIdentityHealth } from "./customer-identity-resolver.mjs";

const BUILD = "customer-crm-api-browser-root-20260823-responsive-hotfix-01";
const RESPONSIVE_MARKER = "crm-responsive-production-hotfix-20260823";

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
    ...customerIdentity
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
  .app,body>main,main,.main,.crm-home-wrap,.crm-admin-wrap,.admin-wrap,.crm-final-shell{width:calc(100vw - 48px)!important;max-width:none!important;margin-left:auto!important;margin-right:auto!important;padding-left:0!important;padding-right:0!important;box-sizing:border-box!important}
  .crm-today-dash,.crm-delivery-panel{width:100%!important;max-width:none!important;margin-left:0!important;margin-right:0!important;box-sizing:border-box!important}
  .crm-bottom-nav,.crm-mf-bottom,.crm-mf-fab,.crm-mf-scrolltop{display:none!important;visibility:hidden!important;pointer-events:none!important}
  .crm-top-menu-btn,.crm-side-menu{display:none!important;visibility:hidden!important;pointer-events:none!important}
  .grid{grid-template-columns:repeat(4,minmax(0,1fr))!important}
  #summary.grid{grid-template-columns:repeat(4,minmax(0,1fr))!important}
  .marketing{grid-template-columns:minmax(0,1.35fr) minmax(0,1fr)!important}
  .crm-today-kpis{grid-template-columns:repeat(6,minmax(0,1fr))!important}
  .crm-today-body{grid-template-columns:minmax(0,1.15fr) minmax(0,.85fr)!important}
  .crm-stable-customer-panel{width:min(1180px,calc(100vw - 64px))!important}
  .crm-v2-detail-panel{width:min(1180px,calc(100vw - 64px))!important}
  table{width:100%!important}
  #crmReconciliationLink{bottom:20px!important}
}
@media (min-width:768px) and (max-width:1023px){
  body{padding-bottom:20px!important;overflow-x:hidden!important}
  .app,body>main,main,.main,.crm-home-wrap,.crm-admin-wrap,.admin-wrap,.crm-final-shell{width:calc(100vw - 24px)!important;max-width:none!important;margin-left:auto!important;margin-right:auto!important;box-sizing:border-box!important}
  .crm-today-dash,.crm-delivery-panel{width:100%!important;max-width:none!important;margin-left:0!important;margin-right:0!important;box-sizing:border-box!important}
  .crm-bottom-nav,.crm-mf-bottom,.crm-mf-fab,.crm-mf-scrolltop{display:none!important;visibility:hidden!important;pointer-events:none!important}
  .grid,#summary.grid{grid-template-columns:repeat(2,minmax(0,1fr))!important}
  .marketing,.crm-today-body,.crm-today-mini-grid{grid-template-columns:repeat(2,minmax(0,1fr))!important}
  .crm-stable-customer-panel,.crm-v2-detail-panel{width:calc(100vw - 32px)!important;right:16px!important}
}
@media (max-width:767px){
  body{max-width:100%!important;overflow-x:hidden!important}
  .app,body>main,main,.main,.crm-home-wrap,.crm-admin-wrap,.admin-wrap,.crm-final-shell{width:100%!important;max-width:100%!important;margin-left:0!important;margin-right:0!important;box-sizing:border-box!important}
  .crm-bottom-nav{display:flex!important}
  .crm-top-menu-btn{display:block!important}
}
</style>`;
  if(!body.includes(RESPONSIVE_MARKER)){
    body = body.includes("</head>") ? body.replace("</head>", style + "</head>") : style + body;
  }
  if(!body.includes('id="crmReconciliationLink"')){
    body = body.includes("</body>") ? body.replace("</body>", link + "</body>") : body + link;
  }
  const headers = copyHeaders(response);
  headers.set("content-type", "text/html; charset=utf-8");
  return new Response(body,{status:response.status,headers});
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);

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
    if(request.method === "GET" && url.pathname === "/admin") return patchAdminUi(response,url);
    return response;
  }
};
