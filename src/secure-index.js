// ======================================================
// CUSTOMER CRM API / GOOGLE ACCESS SECURE ENTRYPOINT
// build: customer-crm-api-google-access-20260530-01
// ======================================================

import app from "./index.js";

const BUILD = "customer-crm-api-google-access-20260530-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";

function text(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

function secureHeaders(headers = {}) {
  const h = new Headers(headers);
  h.set("cache-control", "no-store, no-cache, must-revalidate, max-age=0");
  h.set("pragma", "no-cache");
  h.set("expires", "0");
  h.set("x-robots-tag", "noindex, nofollow, noarchive");
  h.set("x-content-type-options", "nosniff");
  h.set("referrer-policy", "no-referrer");
  h.set("x-frame-options", "DENY");
  h.set("permissions-policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=()");
  return h;
}

function response(body, status = 200, headers = {}) {
  return new Response(body, { status, headers: secureHeaders(headers) });
}

function json(data, status = 200) {
  return response(JSON.stringify(data, null, 2), status, { "content-type": "application/json; charset=utf-8" });
}

function html(body, status = 200) {
  return response(body, status, { "content-type": "text/html; charset=utf-8" });
}

async function readJson(request) {
  try { return await request.json(); } catch (_) { return {}; }
}

function hasUrlToken(url) {
  return url.searchParams.has("token") || /token=/i.test(url.search || "");
}

function accessEmail(request) {
  return (text(request.headers.get("cf-access-authenticated-user-email")) || text(request.headers.get("cf-access-user-email"))).toLowerCase();
}

function loginRequiredPage() {
  return `<!doctype html><html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow,noarchive"><title>Googleログインが必要です</title><style>body{margin:0;min-height:100vh;display:grid;place-items:center;background:#f6f7fb;color:#111827;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans JP',sans-serif}.box{width:min(92vw,460px);background:#fff;border:1px solid #e5e7eb;border-radius:22px;padding:24px;box-shadow:0 18px 60px rgba(15,23,42,.12)}h1{font-size:24px;margin:0 0 8px}.muted{color:#64748b;font-size:14px;line-height:1.8}.warn{background:#fff7ed;border:1px solid #fed7aa;color:#9a3412;border-radius:14px;padding:12px;margin-top:14px;font-size:13px;line-height:1.7}</style></head><body><main class="box"><h1>Googleログインが必要です</h1><div class="muted">この管理画面はCloudflare Zero Trust AccessのGoogleログインで保護します。</div><div class="warn">Cloudflare Access設定が未完了、または許可メールではありません。</div></main></body></html>`;
}

async function ensureAdminUsers(env) {
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users (email TEXT PRIMARY KEY, role TEXT, status TEXT, created_by TEXT, created_at TEXT, updated_at TEXT)`).run();
  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email, role, status, created_by, created_at, updated_at) VALUES(?, 'admin', 'active', 'system', datetime('now'), datetime('now'))`).bind(ROOT_ADMIN_EMAIL).run();
}

async function getCurrentUser(request, env) {
  const email = accessEmail(request);
  if (!email) return null;
  await ensureAdminUsers(env);
  const row = await env.DB.prepare(`SELECT email, role, status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`).bind(email).first();
  return row ? { email: row.email, role: row.role || "viewer" } : null;
}

async function listAdminUsers(env) {
  await ensureAdminUsers(env);
  const rows = await env.DB.prepare(`SELECT email, role, status, created_by, created_at, updated_at FROM crm_admin_users ORDER BY created_at ASC`).all();
  return { ok: true, items: rows.results || [] };
}

async function addAdminUser(request, env, current) {
  if (!current || current.role !== "admin") return { ok: false, message: "Only admin can add users" };
  const body = await readJson(request);
  const email = text(body.email).toLowerCase();
  const role = text(body.role || "viewer") === "admin" ? "admin" : "viewer";
  if (!/^.+@.+\..+$/.test(email)) return { ok: false, message: "email is required" };
  await ensureAdminUsers(env);
  await env.DB.prepare(`INSERT INTO crm_admin_users(email, role, status, created_by, created_at, updated_at) VALUES(?, ?, 'active', ?, datetime('now'), datetime('now')) ON CONFLICT(email) DO UPDATE SET role=excluded.role, status='active', updated_at=datetime('now')`).bind(email, role, current.email).run();
  return { ok: true, email, role };
}

async function removeAdminUser(request, env, current) {
  if (!current || current.role !== "admin") return { ok: false, message: "Only admin can remove users" };
  const body = await readJson(request);
  const email = text(body.email).toLowerCase();
  if (!email) return { ok: false, message: "email is required" };
  if (email === ROOT_ADMIN_EMAIL) return { ok: false, message: "Root admin cannot be removed" };
  await ensureAdminUsers(env);
  await env.DB.prepare(`UPDATE crm_admin_users SET status='disabled', updated_at=datetime('now') WHERE lower(email)=lower(?)`).bind(email).run();
  return { ok: true, email };
}

function injectUserAdminUi(body, current) {
  if (!body || !body.includes("</body>")) return body;
  const adminButtons = current.role === "admin" ? `<button id="crmAddAdminUser" style="margin-left:6px">ユーザー追加</button>` : "";
  const panel = `<div style="position:fixed;right:12px;bottom:12px;z-index:99999;background:#fff;border:1px solid #e5e7eb;border-radius:16px;padding:10px;box-shadow:0 14px 36px rgba(15,23,42,.18);font-family:sans-serif;font-size:12px;line-height:1.5"><b>Googleログイン</b><br>${current.email}<br><button id="crmShowAdminUsers">ユーザー確認</button>${adminButtons}</div><script>(function(){function j(r){return r.json()}var s=document.getElementById('crmShowAdminUsers');if(s)s.onclick=function(){fetch('/api/admin-users').then(j).then(function(d){alert(JSON.stringify(d,null,2))})};var a=document.getElementById('crmAddAdminUser');if(a)a.onclick=function(){var e=prompt('追加するGoogleメール');if(!e)return;var role=confirm('管理者として追加しますか？ OK=admin / キャンセル=viewer')?'admin':'viewer';fetch('/api/admin-users/add',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({email:e,role:role})}).then(j).then(function(d){alert(JSON.stringify(d,null,2))})}})();</script>`;
  return body.replace("</body>", panel + "</body>");
}

async function callOriginal(request, env, ctx, current) {
  const token = text(env.ADMIN_TOKEN);
  if (!token) return json({ ok: false, message: "ADMIN_TOKEN is not configured" }, 503);
  const url = new URL(request.url);
  url.searchParams.delete("token");
  url.searchParams.set("token", token);
  const headers = new Headers(request.headers);
  headers.set("x-admin-token", token);
  headers.set("authorization", `Bearer ${token}`);
  const init = { method: request.method, headers, redirect: "manual" };
  if (request.method !== "GET" && request.method !== "HEAD") init.body = request.body;
  const res = await app.fetch(new Request(url.toString(), init), env, ctx);
  const contentType = res.headers.get("content-type") || "";
  if (contentType.includes("text/html")) {
    let body = await res.text();
    body = body.replaceAll(token, "").replace(/\/admin\?token=[^'\"\s<)]+/g, "/admin");
    return html(injectUserAdminUi(body, current), res.status);
  }
  return new Response(res.body, { status: res.status, statusText: res.statusText, headers: secureHeaders(res.headers) });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    if (request.method === "OPTIONS") return response(null, 204);
    if (path === "/robots.txt") return response("User-agent: *\nDisallow: /\n", 200, { "content-type": "text/plain; charset=utf-8" });
    if (path === "/" || path === "/health" || path === "/api/health") return json({ ok: true, service: "customer-crm-api", build: BUILD, hasDb: !!env.DB, secure: true, auth: "cloudflare-access-google" });
    if (hasUrlToken(url)) {
      if (path === "/admin") { url.search = ""; return Response.redirect(url.toString(), 302); }
      return json({ ok: false, message: "URL token authentication is disabled." }, 401);
    }
    if (path.startsWith("/api/sync/")) return await app.fetch(request, env, ctx);
    const current = await getCurrentUser(request, env);
    if (!current) {
      if (path === "/admin") return html(loginRequiredPage(), 401);
      return json({ ok: false, message: "Google login through Cloudflare Access is required" }, 401);
    }
    if (path === "/api/admin-users" && request.method === "GET") return json(await listAdminUsers(env));
    if (path === "/api/admin-users/add" && request.method === "POST") return json(await addAdminUser(request, env, current));
    if (path === "/api/admin-users/remove" && request.method === "POST") return json(await removeAdminUser(request, env, current));
    if (path === "/admin" || path.startsWith("/api/")) return await callOriginal(request, env, ctx, current);
    return json({ ok: false, message: "Not Found" }, 404);
  }
};
