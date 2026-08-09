import app from "./production-index-crm-delivery-deadline-alerts-entry.js";

const BUILD = "customer-crm-api-browser-root-20260809-01";

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

async function patchHealth(response){
  const raw = await response.text();
  let data = {};
  try { data = raw ? JSON.parse(raw) : {}; } catch (_) {}
  const headers = copyHeaders(response);
  headers.set("content-type", "application/json; charset=utf-8");
  return new Response(JSON.stringify({
    ...data,
    base_build: data.build || data.base_build || "",
    build: BUILD,
    browser_root_redirect: "/admin"
  }, null, 2), { status: response.status, headers });
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);

    if(request.method === "GET" && (url.pathname === "/" || url.pathname === "/index.html") && isBrowserNavigation(request)){
      const target = new URL("/admin", url.origin);
      return Response.redirect(target.toString(), 302);
    }

    const response = await app.fetch(request, env, ctx);
    if(request.method === "GET" && (url.pathname === "/health" || url.pathname === "/api/crm-health-check")){
      return patchHealth(response);
    }
    return response;
  }
};
