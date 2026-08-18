import app from "./production-index-crm-delivery-deadline-alerts-entry.js";
import { handleLineContextEvents, lineContextHealth } from "./crm-line-context-events.mjs";
import { handleInternalCustomerDetail, internalCustomerDetailHealth } from "./crm-internal-customer-detail.mjs";
import { handleReconciliationReview, patchReconciliationHealth } from "./crm-reconciliation-review.mjs";
import { handleCustomerIdentityResolver, customerIdentityHealth } from "./customer-identity-resolver.mjs";

const BUILD = "customer-crm-api-browser-root-20260818-identity-01";

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
    ...lineContext,
    ...customerDetail,
    ...customerIdentity
  }, null, 2), { status: response.status, headers });
}

async function injectReviewLink(response, url){
  const ct = response.headers.get("content-type") || "";
  if(response.status !== 200 || !ct.includes("text/html")) return response;
  let body = await response.text();
  const token = url.searchParams.get("token") || "";
  const href = "/admin/customer-id-reconciliation" + (token ? "?token=" + encodeURIComponent(token) : "");
  const link = `<a href="${href}" style="position:fixed;left:12px;bottom:12px;z-index:9998;background:#17202a;color:#fff;text-decoration:none;border-radius:999px;padding:10px 14px;font:800 13px -apple-system,BlinkMacSystemFont,'Noto Sans JP',sans-serif;box-shadow:0 8px 24px rgba(0,0,0,.18)">顧客ID照合</a>`;
  body = body.includes("</body>") ? body.replace("</body>", link + "</body>") : body + link;
  const headers = copyHeaders(response); headers.set("content-type","text/html; charset=utf-8");
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
    if(request.method === "GET" && url.pathname === "/admin") return injectReviewLink(response,url);
    return response;
  }
};
