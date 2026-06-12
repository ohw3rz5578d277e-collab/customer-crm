// ======================================================
// CUSTOMER CRM API / LINE FOLLOW OVERVIEW WRAPPER
// build: customer-crm-api-line-follow-overview-20260613-01
// ======================================================

import app from "./production-index-line-log.js";

const BUILD = "customer-crm-api-line-follow-overview-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";

function text(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

function normalizeEmail(v) {
  return text(v).toLowerCase();
}

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

function getAccessEmail(request) {
  return normalizeEmail(
    request.headers.get("cf-access-authenticated-user-email") ||
    request.headers.get("Cf-Access-Authenticated-User-Email") ||
    request.headers.get("cf-access-user-email") ||
    ""
  );
}

async function addColumn(db, table, definition) {
  try {
    await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run();
  } catch (_) {}
}

async function ensureAdminUsers(env) {
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users (
    email TEXT PRIMARY KEY,
    role TEXT,
    status TEXT,
    created_by TEXT,
    created_at TEXT,
    updated_at TEXT
  )`).run();

  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email, role, status, created_by, created_at, updated_at)
    VALUES(?, 'admin', 'active', 'system', datetime('now'), datetime('now'))`)
    .bind(ROOT_ADMIN_EMAIL)
    .run();
}

async function getCurrentUser(request, env) {
  const email = getAccessEmail(request);
  if (!email) return null;
  await ensureAdminUsers(env);
  const row = await env.DB.prepare(
    `SELECT email, role, status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`
  ).bind(email).first();
  return row ? { email: row.email, role: row.role || "viewer" } : null;
}

async function ensureLineDraftLogSchema(env) {
  if (!env.DB) throw new Error("D1 DB binding(DB) is missing");

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS customer_line_draft_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT NOT NULL,
    customer_name TEXT,
    action_type TEXT,
    action_label TEXT,
    priority TEXT,
    message_text TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'copied',
    channel TEXT NOT NULL DEFAULT 'line',
    created_by TEXT,
    copied_at TEXT,
    sent_at TEXT,
    memo TEXT,
    raw_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();

  for (const col of [
    "customer_id TEXT",
    "customer_name TEXT",
    "action_type TEXT",
    "action_label TEXT",
    "priority TEXT",
    "message_text TEXT",
    "status TEXT DEFAULT 'copied'",
    "channel TEXT DEFAULT 'line'",
    "created_by TEXT",
    "copied_at TEXT",
    "sent_at TEXT",
    "memo TEXT",
    "raw_json TEXT",
    "created_at TEXT",
    "updated_at TEXT"
  ]) {
    await addColumn(env.DB, "customer_line_draft_logs", col);
  }

  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_customer_line_draft_logs_customer ON customer_line_draft_logs(customer_id, created_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_customer_line_draft_logs_status ON customer_line_draft_logs(status, created_at)`).run();
}

function isRecentLineLogPath(path) {
  return path === "/api/line-message-logs/recent" || path === "/api/line-message-logs/overview";
}

async function listRecentLineLogs(request, env) {
  const current = await getCurrentUser(request, env);
  if (!current) return json({ ok: false, message: "Google login through Cloudflare Access is required" }, 401);

  await ensureLineDraftLogSchema(env);

  const url = new URL(request.url);
  const status = text(url.searchParams.get("status"));
  const keyword = text(url.searchParams.get("keyword"));
  const limitRaw = parseInt(url.searchParams.get("limit") || "30", 10);
  const limit = Math.max(1, Math.min(Number.isFinite(limitRaw) ? limitRaw : 30, 100));

  const where = [];
  const params = [];

  if (status === "pending") {
    where.push("COALESCE(status,'copied') <> 'sent'");
  } else if (["copied", "saved", "sent"].includes(status)) {
    where.push("status = ?");
    params.push(status);
  }

  if (keyword) {
    const like = `%${keyword}%`;
    where.push("(customer_name LIKE ? OR customer_id LIKE ? OR action_label LIKE ? OR message_text LIKE ?)");
    params.push(like, like, like, like);
  }

  const whereSql = where.length ? "WHERE " + where.join(" AND ") : "";

  const rows = await env.DB.prepare(`
    SELECT id, customer_id, customer_name, action_type, action_label, priority, message_text, status, channel, created_by, copied_at, sent_at, memo, created_at, updated_at
    FROM customer_line_draft_logs
    ${whereSql}
    ORDER BY COALESCE(sent_at, copied_at, created_at) DESC, id DESC
    LIMIT ?
  `).bind(...params, limit).all();

  const summary = await env.DB.prepare(`
    SELECT
      COUNT(*) AS total,
      SUM(CASE WHEN COALESCE(status,'copied') <> 'sent' THEN 1 ELSE 0 END) AS pending,
      SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) AS sent,
      SUM(CASE WHEN date(COALESCE(sent_at,copied_at,created_at)) >= date('now','-7 day') THEN 1 ELSE 0 END) AS last_7d,
      SUM(CASE WHEN date(COALESCE(sent_at,copied_at,created_at)) >= date('now','-30 day') THEN 1 ELSE 0 END) AS last_30d
    FROM customer_line_draft_logs
  `).first();

  return json({
    ok: true,
    count: (rows.results || []).length,
    status: status || "all",
    summary: summary || {},
    items: rows.results || []
  });
}

function injectLineOverviewUi(html) {
  if (!html || !html.includes("</head>") || !html.includes("</body>")) return html;

  const style = `<style id="crm-line-overview-style">
.crm-line-overview{background:#fff;border:1px solid #e5e7eb;border-radius:20px;padding:12px;margin:12px 0;box-shadow:0 8px 24px rgba(15,23,42,.06)}.crm-line-overview-head{display:flex;justify-content:space-between;align-items:flex-start;gap:10px}.crm-line-overview-title{font-weight:950;font-size:1rem}.crm-line-overview-sub{color:#64748b;font-size:.78rem;line-height:1.55;margin-top:3px}.crm-line-overview-actions{display:flex;gap:6px;flex-wrap:wrap}.crm-line-overview button{border:1px solid #d1d5db;background:#fff;border-radius:999px;padding:7px 10px;font-size:.75rem;font-weight:900;cursor:pointer}.crm-line-overview button.active{background:#028760;border-color:#028760;color:#fff}.crm-line-overview-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin:10px 0}.crm-line-overview-stat{background:#f8fafc;border:1px solid #e5e7eb;border-radius:14px;padding:9px}.crm-line-overview-stat b{display:block;font-size:1.18rem}.crm-line-overview-stat span{display:block;color:#64748b;font-size:.7rem;font-weight:900;margin-top:2px}.crm-line-overview-list{display:grid;gap:8px;max-height:360px;overflow:auto}.crm-line-overview-item{border:1px solid #edf2f7;background:#fff;border-radius:14px;padding:10px}.crm-line-overview-meta{display:flex;gap:6px;flex-wrap:wrap;align-items:center;color:#64748b;font-size:.72rem;font-weight:900}.crm-line-overview-name{font-weight:950;color:#111827}.crm-line-overview-text{margin-top:6px;white-space:pre-wrap;line-height:1.55;color:#334155;font-size:.78rem;max-height:72px;overflow:auto}.crm-line-overview-open,.crm-line-overview-sent{border-radius:999px!important;padding:5px 8px!important;font-size:.7rem!important}.crm-line-overview-sent{background:#028760!important;color:#fff!important;border-color:#028760!important}.crm-line-overview-empty{color:#64748b;background:#f8fafc;border:1px dashed #cbd5e1;border-radius:14px;padding:12px;font-size:.82rem}.crm-line-overview-toast{position:fixed;left:50%;bottom:86px;transform:translateX(-50%);z-index:999999;background:#111827;color:#fff;border-radius:999px;padding:10px 14px;font-size:13px;font-weight:900;box-shadow:0 14px 36px rgba(15,23,42,.25)}@media(max-width:820px){.crm-line-overview{margin:10px 0;border-radius:18px}.crm-line-overview-head{display:block}.crm-line-overview-actions{margin-top:8px}.crm-line-overview-grid{display:flex;overflow-x:auto;padding-bottom:4px}.crm-line-overview-stat{min-width:116px}.crm-line-overview-list{max-height:320px}}
</style>`;

  const script = `<script id="crm-line-overview-script">
(function(){
  if(window.__crmLineOverviewInstalled)return;
  window.__crmLineOverviewInstalled=true;
  var state={status:'pending',keyword:''};
  function esc(v){return String(v==null?'':v).replace(/[&<>\"]/g,function(m){return {'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[m]})}
  function yen(v){return Number(v||0).toLocaleString('ja-JP')}
  function toast(msg){var old=document.querySelector('.crm-line-overview-toast');if(old)old.remove();var t=document.createElement('div');t.className='crm-line-overview-toast';t.textContent=msg||'更新しました';document.body.appendChild(t);setTimeout(function(){t.remove()},1600)}
  function findInsertTarget(){return document.querySelector('.toolbar')||document.getElementById('summary')||document.querySelector('.app')||document.body}
  function ensurePanel(){if(document.getElementById('crmLineOverview'))return document.getElementById('crmLineOverview');var target=findInsertTarget();var box=document.createElement('section');box.id='crmLineOverview';box.className='crm-line-overview';box.innerHTML='<div class="crm-line-overview-head"><div><div class="crm-line-overview-title">LINEフォロー履歴</div><div class="crm-line-overview-sub">保存したLINE文面・未送信・送信済みを顧客一覧から確認できます。</div></div><div class="crm-line-overview-actions"><button data-line-filter="pending" class="active">未送信</button><button data-line-filter="all">すべて</button><button data-line-filter="sent">送信済み</button><button data-line-refresh>更新</button></div></div><div class="crm-line-overview-grid" id="crmLineOverviewStats"></div><div class="crm-line-overview-list" id="crmLineOverviewList"><div class="crm-line-overview-empty">読み込み中...</div></div>';if(target.id==='summary'&&target.parentNode){target.parentNode.insertBefore(box,target.nextSibling)}else if(target.classList&&target.classList.contains('toolbar')&&target.parentNode){target.parentNode.insertBefore(box,target)}else if(target.firstChild){target.insertBefore(box,target.firstChild)}else{target.appendChild(box)}box.querySelectorAll('[data-line-filter]').forEach(function(b){b.onclick=function(){state.status=b.getAttribute('data-line-filter');box.querySelectorAll('[data-line-filter]').forEach(function(x){x.classList.toggle('active',x===b)});loadOverview()}});var r=box.querySelector('[data-line-refresh]');if(r)r.onclick=loadOverview;return box}
  function renderStats(s){var el=document.getElementById('crmLineOverviewStats');if(!el)return;s=s||{};el.innerHTML='<div class="crm-line-overview-stat"><b>'+yen(s.pending||0)+'</b><span>未送信</span></div><div class="crm-line-overview-stat"><b>'+yen(s.sent||0)+'</b><span>送信済み</span></div><div class="crm-line-overview-stat"><b>'+yen(s.total||0)+'</b><span>保存総数</span></div><div class="crm-line-overview-stat"><b>'+yen(s.last_7d||0)+'</b><span>7日以内</span></div><div class="crm-line-overview-stat"><b>'+yen(s.last_30d||0)+'</b><span>30日以内</span></div>'}
  function renderItems(items){var el=document.getElementById('crmLineOverviewList');if(!el)return;items=items||[];if(!items.length){el.innerHTML='<div class="crm-line-overview-empty">該当するLINE文面履歴はありません。</div>';return}el.innerHTML=items.slice(0,30).map(function(x){var sent=x.status==='sent';return '<article class="crm-line-overview-item"><div class="crm-line-overview-meta"><span class="crm-line-overview-name">'+esc(x.customer_name||x.customer_id||'顧客')+'</span><span>'+esc(x.action_label||'文面')+'</span><span>'+esc(x.status||'copied')+'</span><span>'+esc(x.sent_at||x.copied_at||x.created_at||'')+'</span><button class="crm-line-overview-open" data-open-customer="'+esc(x.customer_id)+'">詳細</button>'+(sent?'':'<button class="crm-line-overview-sent" data-overview-sent="'+esc(x.id)+'">送信済みにする</button>')+'</div><div class="crm-line-overview-text">'+esc(x.message_text||'')+'</div></article>'}).join('');el.querySelectorAll('[data-open-customer]').forEach(function(b){b.onclick=function(){var id=b.getAttribute('data-open-customer');if(window.openDetail){window.openDetail(id)}else{fetch('/api/customers/'+encodeURIComponent(id)).then(function(){toast('顧客詳細APIを確認しました')})}}});el.querySelectorAll('[data-overview-sent]').forEach(function(b){b.onclick=function(){markSent(b.getAttribute('data-overview-sent'))}})}
  async function markSent(id){try{var r=await fetch('/api/line-message-logs/'+encodeURIComponent(id)+'/mark-sent',{method:'POST'});var j=await r.json();if(!r.ok||j.ok===false)throw new Error(j.message||'更新に失敗しました');toast('送信済みにしました');loadOverview()}catch(e){alert(e.message||String(e))}}
  async function loadOverview(){ensurePanel();try{var qs=new URLSearchParams();qs.set('limit','30');if(state.status&&state.status!=='all')qs.set('status',state.status);var r=await fetch('/api/line-message-logs/recent?'+qs.toString());var j=await r.json();if(!r.ok||j.ok===false)throw new Error(j.message||'読み込みに失敗しました');renderStats(j.summary||{});renderItems(j.items||[])}catch(e){var el=document.getElementById('crmLineOverviewList');if(el)el.innerHTML='<div class="crm-line-overview-empty">LINE履歴の読み込みに失敗しました: '+esc(e.message||e)+'</div>'}}
  function init(){if(!/\/admin\/?$/.test(location.pathname))return;ensurePanel();loadOverview()}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else setTimeout(init,60);
  var mo=new MutationObserver(function(){if(/\/admin\/?$/.test(location.pathname)&&!document.getElementById('crmLineOverview'))setTimeout(init,80)});mo.observe(document.documentElement,{childList:true,subtree:true});
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

    if (isRecentLineLogPath(url.pathname) && request.method === "GET") {
      return await listRecentLineLogs(request, env);
    }

    const res = await app.fetch(request, env, ctx);
    const contentType = res.headers.get("content-type") || "";

    if (contentType.includes("text/html")) {
      const body = injectLineOverviewUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }

    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
