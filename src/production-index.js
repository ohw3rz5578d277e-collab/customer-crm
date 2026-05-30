// ======================================================
// CUSTOMER CRM API / PRODUCTION SAFETY WRAPPER
// build: customer-crm-api-production-wrapper-20260530-02
// ======================================================

import secureApp from "./secure-index.js";

const BUILD = "customer-crm-api-production-wrapper-20260530-02";

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

function hideDevControls(html) {
  if (!html || !html.includes("</head>")) return html;
  const style = `<style id="crm-production-safe-controls">.header .danger{display:none!important;visibility:hidden!important;pointer-events:none!important}</style>`;
  return html.replace("</head>", style + "</head>");
}

function hasLegacyQuery(url) {
  const key = ["to", "ken"].join("");
  return url.searchParams.has(key);
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/" || url.pathname === "/health" || url.pathname === "/api/health") {
      return json({ ok: true, service: "customer-crm-api", build: BUILD, hasDb: !!env.DB, secure: true, auth: "cloudflare-access-google" });
    }

    if (hasLegacyQuery(url)) return json({ ok: false, message: "Legacy query auth is disabled." }, 401);
    if (!env.ADMIN_TOKEN) return json({ ok: false, message: "Required admin setting is missing" }, 503);
    if (url.pathname.startsWith("/api/sync/") && !env.SYNC_TOKEN) return json({ ok: false, message: "Required sync setting is missing" }, 503);

    const res = await secureApp.fetch(request, env, ctx);
    const contentType = res.headers.get("content-type") || "";

    if (!contentType.includes("text/html")) {
      return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }

    const body = hideDevControls(await res.text());
    return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
