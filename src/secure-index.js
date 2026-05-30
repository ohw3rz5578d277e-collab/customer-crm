// ======================================================
// CUSTOMER CRM API / GOOGLE ACCESS SECURE ENTRYPOINT
// build: customer-crm-api-google-access-20260530-02
// ======================================================

import app from "./index.js";

const BUILD = "customer-crm-api-google-access-20260530-02";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";

function text(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

function escHtml(v) {
  return String(v ?? "").replace(/[&<>\"]/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[m]));
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
  try {
    return await request.json();
  } catch (_) {
    return {};
  }
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
  const rows = await env.DB.prepare(`SELECT email, role, status, created_by, created_at, updated_at FROM crm_admin_users ORDER BY CASE WHEN lower(email)=lower(?) THEN 0 ELSE 1 END, status ASC, role ASC, email ASC`).bind(ROOT_ADMIN_EMAIL).all();
  return { ok: true, root_admin_email: ROOT_ADMIN_EMAIL, items: rows.results || [] };
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

  const currentEmail = escHtml(current.email);
  const currentRole = escHtml(current.role);
  const currentEmailJson = JSON.stringify(current.email || "");
  const currentRoleJson = JSON.stringify(current.role || "viewer");
  const rootEmailJson = JSON.stringify(ROOT_ADMIN_EMAIL);

  const panel = `
<style id="crm-admin-users-style">
  .crm-user-fab{position:fixed;right:14px;bottom:14px;z-index:99999;background:#fff;border:1px solid #e5e7eb;border-radius:18px;padding:12px 13px;box-shadow:0 16px 44px rgba(15,23,42,.20);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans JP',sans-serif;font-size:12px;line-height:1.5;color:#111827;min-width:210px}.crm-user-fab b{font-size:13px}.crm-user-fab .sub{color:#64748b;font-size:11px;margin:2px 0 8px;word-break:break-all}.crm-user-fab .row{display:flex;gap:6px;flex-wrap:wrap}.crm-user-fab button,.crm-user-modal button{border:1px solid #d1d5db;background:#fff;border-radius:10px;padding:7px 9px;font-weight:800;font-size:12px;cursor:pointer}.crm-user-fab button.primary,.crm-user-modal button.primary{background:#111827;color:#fff;border-color:#111827}.crm-user-fab button.danger,.crm-user-modal button.danger{background:#fee2e2;color:#991b1b;border-color:#fecaca}.crm-user-backdrop{position:fixed;inset:0;z-index:100000;background:rgba(15,23,42,.38);display:none}.crm-user-backdrop.show{display:block}.crm-user-modal{position:fixed;right:18px;bottom:18px;z-index:100001;width:min(94vw,620px);max-height:min(86vh,760px);overflow:auto;background:#fff;border:1px solid #e5e7eb;border-radius:24px;box-shadow:0 24px 80px rgba(15,23,42,.32);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans JP',sans-serif;color:#111827;display:none}.crm-user-modal.show{display:block}.crm-user-head{position:sticky;top:0;background:#fff;border-bottom:1px solid #e5e7eb;padding:16px 18px;display:flex;align-items:flex-start;justify-content:space-between;gap:12px;border-radius:24px 24px 0 0}.crm-user-head h2{margin:0;font-size:20px}.crm-user-head p{margin:4px 0 0;color:#64748b;font-size:12px;line-height:1.6}.crm-user-body{padding:16px 18px 20px}.crm-user-add{display:grid;grid-template-columns:1.4fr .6fr auto;gap:8px;margin:0 0 14px;padding:12px;background:#f8fafc;border:1px solid #e5e7eb;border-radius:16px}.crm-user-add input,.crm-user-add select{width:100%;box-sizing:border-box;border:1px solid #d1d5db;border-radius:12px;padding:10px;font-size:14px;background:#fff}.crm-user-list{display:grid;gap:8px}.crm-user-card{display:grid;grid-template-columns:1fr auto;gap:10px;align-items:center;border:1px solid #e5e7eb;border-radius:16px;padding:12px;background:#fff}.crm-user-card.disabled{opacity:.55;background:#f8fafc}.crm-user-email{font-weight:900;word-break:break-all}.crm-user-meta{display:flex;gap:6px;flex-wrap:wrap;margin-top:6px}.crm-badge{display:inline-flex;align-items:center;border-radius:999px;padding:4px 8px;background:#f1f5f9;color:#334155;font-size:11px;font-weight:800}.crm-badge.admin{background:#ecfdf5;color:#166534}.crm-badge.disabled{background:#f3f4f6;color:#6b7280}.crm-user-msg{margin:10px 0 0;color:#64748b;font-size:12px;line-height:1.6}.crm-user-error{color:#b91c1c}.crm-user-empty{padding:16px;border:1px dashed #cbd5e1;border-radius:16px;color:#64748b;background:#f8fafc}@media(max-width:760px){.crm-user-fab{left:12px;right:12px;bottom:12px}.crm-user-modal{left:10px;right:10px;bottom:10px;width:auto;max-height:88vh}.crm-user-add{grid-template-columns:1fr}.crm-user-card{grid-template-columns:1fr}.crm-user-card .actions{display:flex;justify-content:flex-end}}
</style>
<div class="crm-user-fab" id="crmUserFab">
  <b>Googleログイン</b>
  <div class="sub">${currentEmail}<br>role: ${currentRole}</div>
  <div class="row"><button class="primary" id="crmOpenUsers">ユーザー管理</button></div>
</div>
<div class="crm-user-backdrop" id="crmUserBackdrop"></div>
<div class="crm-user-modal" id="crmUserModal" aria-hidden="true">
  <div class="crm-user-head">
    <div><h2>管理ユーザー</h2><p>Googleログイン後、ここに登録されているメールだけCRMを利用できます。初期管理者は削除できません。</p></div>
    <button id="crmCloseUsers">閉じる</button>
  </div>
  <div class="crm-user-body">
    <div id="crmUserAddBox"></div>
    <div id="crmUserList" class="crm-user-list"><div class="crm-user-empty">読み込み中...</div></div>
    <div id="crmUserMsg" class="crm-user-msg"></div>
  </div>
</div>
<script id="crm-admin-users-script">
(function(){
  var CURRENT_EMAIL = ${currentEmailJson};
  var CURRENT_ROLE = ${currentRoleJson};
  var ROOT_EMAIL = ${rootEmailJson};
  function el(id){return document.getElementById(id)}
  function esc(v){return String(v==null?'':v).replace(/[&<>\"]/g,function(m){return {'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[m]})}
  function setMsg(v,isErr){var m=el('crmUserMsg'); if(!m)return; m.className='crm-user-msg'+(isErr?' crm-user-error':''); m.textContent=v||''}
  async function api(path,opt){opt=opt||{}; opt.headers=opt.headers||{}; if(opt.body&&!opt.headers['content-type'])opt.headers['content-type']='application/json'; var r=await fetch(path,opt); var j=await r.json().catch(function(){return {ok:false,message:'JSON parse error'}}); if(!r.ok||j.ok===false)throw new Error(j.message||('HTTP '+r.status)); return j}
  function openModal(){el('crmUserBackdrop').classList.add('show');el('crmUserModal').classList.add('show');loadUsers()}
  function closeModal(){el('crmUserBackdrop').classList.remove('show');el('crmUserModal').classList.remove('show')}
  function renderAddBox(){var box=el('crmUserAddBox'); if(!box)return; if(CURRENT_ROLE!=='admin'){box.innerHTML='<div class="crm-user-empty">ユーザー追加・無効化は管理者のみ操作できます。</div>';return;} box.innerHTML='<div class="crm-user-add"><input id="crmNewUserEmail" type="email" placeholder="追加するGoogleメール"><select id="crmNewUserRole"><option value="viewer">viewer</option><option value="admin">admin</option></select><button class="primary" id="crmCreateUser">追加</button></div>'; var b=el('crmCreateUser'); if(b)b.onclick=addUser}
  function renderUsers(items){var list=el('crmUserList'); if(!list)return; if(!items||!items.length){list.innerHTML='<div class="crm-user-empty">管理ユーザーはまだありません。</div>';return;} list.innerHTML=items.map(function(u){var email=String(u.email||''); var status=String(u.status||''); var role=String(u.role||'viewer'); var root=email.toLowerCase()===ROOT_EMAIL.toLowerCase(); var disabled=status!=='active'; var canRemove=CURRENT_ROLE==='admin'&&!root&&!disabled; return '<div class="crm-user-card '+(disabled?'disabled':'')+'"><div><div class="crm-user-email">'+esc(email)+'</div><div class="crm-user-meta"><span class="crm-badge '+(role==='admin'?'admin':'')+'">'+esc(role)+'</span><span class="crm-badge '+(disabled?'disabled':'')+'">'+esc(status||'unknown')+'</span>'+(root?'<span class="crm-badge admin">root admin</span>':'')+'<span class="crm-badge">created: '+esc(u.created_at||'-')+'</span></div></div><div class="actions">'+(canRemove?'<button class="danger" data-remove="'+esc(email)+'">無効化</button>':'')+'</div></div>'}).join(''); list.querySelectorAll('[data-remove]').forEach(function(btn){btn.onclick=function(){removeUser(btn.getAttribute('data-remove'))}})}
  async function loadUsers(){try{setMsg('読み込み中...');renderAddBox();var d=await api('/api/admin-users');renderUsers(d.items||[]);setMsg('読み込み完了: '+((d.items||[]).length)+'件')}catch(e){setMsg(e.message,true);renderUsers([])}}
  async function addUser(){var email=(el('crmNewUserEmail')&&el('crmNewUserEmail').value||'').trim(); var role=(el('crmNewUserRole')&&el('crmNewUserRole').value||'viewer'); if(!email){setMsg('メールアドレスを入力してください',true);return;} if(role==='admin'&&!confirm(email+' を管理者として追加しますか？'))return; try{setMsg('追加中...');await api('/api/admin-users/add',{method:'POST',body:JSON.stringify({email:email,role:role})}); if(el('crmNewUserEmail'))el('crmNewUserEmail').value=''; await loadUsers(); setMsg('追加しました: '+email)}catch(e){setMsg(e.message,true)}}
  async function removeUser(email){if(!email)return; if(!confirm(email+' を無効化しますか？'))return; try{setMsg('無効化中...');await api('/api/admin-users/remove',{method:'POST',body:JSON.stringify({email:email})}); await loadUsers(); setMsg('無効化しました: '+email)}catch(e){setMsg(e.message,true)}}
  var open=el('crmOpenUsers'), close=el('crmCloseUsers'), bg=el('crmUserBackdrop'); if(open)open.onclick=openModal; if(close)close.onclick=closeModal; if(bg)bg.onclick=closeModal;
})();
</script>`;

  return body.replace("</body>", panel + "</body>");
}

async function callOriginal(request, env, ctx, current) {
  const token = text(env.ADMIN_TOKEN);
  if (!token) return json({ ok: false, message: "ADMIN_TOKEN is not configured" }, 503);
  const url = new URL(request.url);
  url.searchParams.delete("token");
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
