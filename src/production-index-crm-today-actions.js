// ======================================================
// CUSTOMER CRM / TODAY DASHBOARD QUICK ACTIONS WRAPPER
// build: customer-crm-api-today-actions-20260613-01
// Adds one-click actions to today's dashboard: mark LINE sent, complete follow task, resync reservation link.
// ======================================================

import app from "./production-index-crm-today-dashboard.js";

const BUILD = "customer-crm-api-today-actions-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const READ_ROLES = ["root_admin", "admin", "staff", "viewer"];
const WRITE_ROLES = ["root_admin", "admin", "staff"];

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
  h.set("x-content-type-options", "nosniff");
  h.set("referrer-policy", "no-referrer");
  h.set("x-frame-options", "DENY");
  return h;
}

function json(data, status = 200, headers = {}) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: securityHeaders({ "content-type": "application/json; charset=utf-8", ...headers })
  });
}

function getAccessEmail(request) {
  return normalizeEmail(
    request.headers.get("cf-access-authenticated-user-email") ||
    request.headers.get("Cf-Access-Authenticated-User-Email") ||
    request.headers.get("cf-access-user-email") ||
    request.headers.get("x-user-email") ||
    ""
  );
}

async function addColumn(db, table, definition) {
  try { await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run(); } catch (_) {}
}

async function ensureSchema(env) {
  if (!env.DB) throw new Error("D1 DB binding(DB) is missing");

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

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_today_action_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action_type TEXT NOT NULL,
    target_type TEXT,
    target_id TEXT,
    customer_id TEXT,
    customer_name TEXT,
    status_before TEXT,
    status_after TEXT,
    actor_email TEXT,
    result TEXT,
    message TEXT,
    raw_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();

  await addColumn(env.DB, "customer_line_draft_logs", "sent_by TEXT");
  await addColumn(env.DB, "crm_follow_tasks", "completed_by TEXT");
  await addColumn(env.DB, "crm_follow_tasks", "completed_at TEXT");

  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_today_action_logs_created ON crm_today_action_logs(created_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_today_action_logs_target ON crm_today_action_logs(target_type, target_id, created_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_today_action_logs_customer ON crm_today_action_logs(customer_id, created_at)`).run();
}

async function requireRole(request, env, allowedRoles) {
  await ensureSchema(env);
  const email = getAccessEmail(request);
  if (!email) return { ok: false, response: json({ ok: false, message: "Login required" }, 401) };
  const user = await env.DB.prepare(
    `SELECT email, role, status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`
  ).bind(email).first();
  if (!user) return { ok: false, response: json({ ok: false, message: "User is not allowed" }, 403) };
  if (!allowedRoles.includes(user.role || "")) return { ok: false, response: json({ ok: false, message: "Permission denied" }, 403) };
  return { ok: true, email, user };
}

async function writeActionLog(env, row) {
  try {
    await env.DB.prepare(`INSERT INTO crm_today_action_logs(
      action_type, target_type, target_id, customer_id, customer_name, status_before, status_after,
      actor_email, result, message, raw_json, created_at
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,datetime('now'))`).bind(
      text(row.action_type), text(row.target_type), text(row.target_id), text(row.customer_id), text(row.customer_name),
      text(row.status_before), text(row.status_after), text(row.actor_email), text(row.result || "ok"), text(row.message),
      JSON.stringify(row.raw_json || {})
    ).run();
  } catch (_) {}
}

async function markLineSent(request, env, lineLogId) {
  const auth = await requireRole(request, env, WRITE_ROLES);
  if (!auth.ok) return auth.response;

  const id = Number(lineLogId);
  if (!Number.isFinite(id) || id <= 0) return json({ ok: false, message: "invalid line log id" }, 400);

  const row = await env.DB.prepare(`SELECT * FROM customer_line_draft_logs WHERE id=? LIMIT 1`).bind(id).first();
  if (!row) return json({ ok: false, message: "LINE draft log not found" }, 404);

  const before = text(row.status);
  await env.DB.prepare(`UPDATE customer_line_draft_logs
    SET status='sent', sent_at=COALESCE(sent_at, datetime('now')), sent_by=?, updated_at=datetime('now')
    WHERE id=?`).bind(auth.email, id).run();

  await writeActionLog(env, {
    action_type: "today_line_mark_sent",
    target_type: "customer_line_draft_logs",
    target_id: String(id),
    customer_id: row.customer_id,
    customer_name: row.customer_name,
    status_before: before,
    status_after: "sent",
    actor_email: auth.email,
    message: "今日やることダッシュボードからLINE送信済みに変更",
    raw_json: { action_label: row.action_label, action_type: row.action_type }
  });

  return json({ ok: true, action: "line_mark_sent", id, customer_id: row.customer_id, status_before: before, status_after: "sent" });
}

async function completeFollowTask(request, env, taskId) {
  const auth = await requireRole(request, env, WRITE_ROLES);
  if (!auth.ok) return auth.response;

  const id = Number(taskId);
  if (!Number.isFinite(id) || id <= 0) return json({ ok: false, message: "invalid follow task id" }, 400);

  const row = await env.DB.prepare(`SELECT * FROM crm_follow_tasks WHERE id=? LIMIT 1`).bind(id).first();
  if (!row) return json({ ok: false, message: "follow task not found" }, 404);

  const before = text(row.status || "open");
  await env.DB.prepare(`UPDATE crm_follow_tasks
    SET status='completed', completed_by=?, completed_at=COALESCE(completed_at, datetime('now')), updated_at=datetime('now')
    WHERE id=?`).bind(auth.email, id).run();

  await writeActionLog(env, {
    action_type: "today_follow_complete",
    target_type: "crm_follow_tasks",
    target_id: String(id),
    customer_id: row.customer_id,
    customer_name: row.customer_name,
    status_before: before,
    status_after: "completed",
    actor_email: auth.email,
    message: "今日やることダッシュボードからフォロー完了",
    raw_json: { title: row.title, due_date: row.due_date, priority: row.priority }
  });

  return json({ ok: true, action: "follow_complete", id, customer_id: row.customer_id, status_before: before, status_after: "completed" });
}

async function resyncReservation(request, env, ctx, draftId) {
  const auth = await requireRole(request, env, WRITE_ROLES);
  if (!auth.ok) return auth.response;

  const id = Number(draftId);
  if (!Number.isFinite(id) || id <= 0) return json({ ok: false, message: "invalid reservation draft id" }, 400);

  const before = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts WHERE id=? LIMIT 1`).bind(id).first();
  if (!before) return json({ ok: false, message: "reservation draft not found" }, 404);

  const url = new URL(request.url);
  const headers = new Headers(request.headers);
  headers.set("content-type", "application/json");
  headers.set("x-user-email", auth.email);
  const res = await app.fetch(new Request(`${url.origin}/api/reservation-link-monitor/${id}/resync`, {
    method: "POST",
    headers,
    body: JSON.stringify({ source: "today_dashboard" })
  }), env, ctx);
  const data = await res.json().catch(() => ({ ok: false, status: res.status }));

  const after = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts WHERE id=? LIMIT 1`).bind(id).first();
  await writeActionLog(env, {
    action_type: "today_reservation_resync",
    target_type: "crm_reservation_drafts",
    target_id: String(id),
    customer_id: before.customer_id,
    customer_name: before.customer_name,
    status_before: before.status,
    status_after: after?.status || before.status,
    actor_email: auth.email,
    result: data.ok ? "ok" : "failed",
    message: "今日やることダッシュボードから予約連携を再同期",
    raw_json: data
  });

  return json({ ok: !!data.ok, action: "reservation_resync", id, resync: data, status: res.status }, res.ok ? 200 : res.status || 500);
}

async function actionLogsApi(request, env) {
  const auth = await requireRole(request, env, READ_ROLES);
  if (!auth.ok) return auth.response;
  const url = new URL(request.url);
  const limit = Math.min(Math.max(Number(url.searchParams.get("limit") || 30), 1), 100);
  const rows = await env.DB.prepare(`SELECT * FROM crm_today_action_logs ORDER BY datetime(created_at) DESC, id DESC LIMIT ?`).bind(limit).all();
  return json({ ok: true, build: BUILD, logs: rows.results || [] });
}

function injectTodayActionUi(html) {
  if (!html || html.includes("crmTodayActionScript")) return html;

  const style = `<style id="crmTodayActionStyle">
.crm-today-action-panel{margin:10px auto 18px;max-width:1180px;border:1px solid #bbf7d0;background:linear-gradient(135deg,#f0fdf4,#ffffff);border-radius:18px;padding:12px;box-shadow:0 10px 28px rgba(22,163,74,.08);font-family:inherit;color:#0f172a}.crm-today-action-head{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;flex-wrap:wrap}.crm-today-action-title{font-size:17px;font-weight:950;margin:0}.crm-today-action-sub{font-size:12px;color:#64748b;margin:4px 0 0}.crm-today-action-buttons{display:flex;gap:8px;flex-wrap:wrap}.crm-today-action-buttons button{border:1px solid #86efac;background:#fff;border-radius:11px;padding:8px 10px;font-size:12px;font-weight:900;color:#166534;cursor:pointer}.crm-today-action-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:10px}.crm-today-action-box{border:1px solid #dcfce7;background:#fff;border-radius:14px;padding:10px;min-width:0}.crm-today-action-box h3{font-size:14px;margin:0 0 8px;font-weight:950}.crm-today-action-row{border-top:1px solid #f1f5f9;padding:8px 0;font-size:13px}.crm-today-action-row:first-of-type{border-top:0}.crm-today-action-row b{font-weight:950}.crm-today-action-meta{font-size:11px;color:#64748b;margin-top:3px;line-height:1.45}.crm-today-action-row button{border:0;background:#166534;color:#fff;border-radius:10px;padding:7px 9px;font-size:12px;font-weight:950;cursor:pointer;margin-top:6px;margin-right:6px}.crm-today-action-row button.secondary{background:#1d4ed8}.crm-today-action-row button.warn{background:#92400e}.crm-today-action-empty{font-size:13px;color:#64748b;padding:8px 0}@media(max-width:860px){.crm-today-action-panel{margin:10px}.crm-today-action-grid{grid-template-columns:1fr}}
</style>`;

  const script = `<script id="crmTodayActionScript">
(function(){
  if(window.__crmTodayActionInstalled)return; window.__crmTodayActionInstalled=true;
  function esc(v){return String(v == null ? '' : v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
  function short(v){return v ? String(v).replace('T',' ').slice(0,16) : '-';}
  function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
  function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:1000004;background:#14532d;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.18)';document.body.appendChild(d);setTimeout(function(){d.remove()},2600)}
  function panel(){return '<section id="crmTodayActionPanel" class="crm-today-action-panel"><div class="crm-today-action-head"><div><h2 class="crm-today-action-title">今日のクイック操作</h2><p class="crm-today-action-sub">未送信LINE・フォロー予定・予約連携の詰まりをここから直接処理できます。</p></div><div class="crm-today-action-buttons"><button id="crmTodayActionReload">操作リスト更新</button><button id="crmTodayActionLogs">操作ログ</button></div></div><div class="crm-today-action-grid"><div class="crm-today-action-box"><h3>LINE送信済みにする</h3><div id="crmTodayActionLine"><div class="crm-today-action-empty">読み込み中...</div></div></div><div class="crm-today-action-box"><h3>フォロー完了にする</h3><div id="crmTodayActionFollow"><div class="crm-today-action-empty">読み込み中...</div></div></div><div class="crm-today-action-box"><h3>予約連携を再同期</h3><div id="crmTodayActionReservation"><div class="crm-today-action-empty">読み込み中...</div></div></div></div></section>';}
  function install(){if(document.getElementById('crmTodayActionPanel'))return; var base=document.getElementById('crmTodayDashboard'); if(base){base.insertAdjacentHTML('afterend',panel());}else{var target=document.querySelector('main')||document.querySelector('#app')||document.querySelector('.container')||document.body; target.insertAdjacentHTML('afterbegin',panel());} loadActions();}
  function lineRow(x){return '<div class="crm-today-action-row"><b>'+esc(x.customer_name||'-')+'</b><div>'+esc(x.action_label||x.action_type||'LINE文面')+'</div><div class="crm-today-action-meta">ID '+esc(x.id)+' / 優先度 '+esc(x.priority||'-')+' / 保存 '+short(x.created_at)+'</div><button data-today-line-sent="'+esc(x.id)+'">LINE送信済み</button></div>';}
  function followRow(x){return '<div class="crm-today-action-row"><b>'+esc(x.customer_name||'-')+'</b><div>'+esc(x.title||'フォロー予定')+'</div><div class="crm-today-action-meta">ID '+esc(x.id)+' / 期限 '+esc(x.due_date||'-')+' / 優先度 '+esc(x.priority||'-')+'</div><button data-today-follow-complete="'+esc(x.id)+'">フォロー完了</button></div>';}
  function reservationRow(x){return '<div class="crm-today-action-row"><b>'+esc(x.customer_name||'-')+'</b><div>'+esc(x.stage||x.label||'予約連携')+'</div><div class="crm-today-action-meta">下書きID '+esc(x.id||x.draft_id)+' / 予約ID '+esc(x.reservation_app_reservation_id||'-')+' / '+esc(x.reason||'')+'</div><button class="secondary" data-today-resync="'+esc(x.id||x.draft_id)+'">自動再同期</button></div>';}
  async function loadActions(){var line=document.getElementById('crmTodayActionLine'),follow=document.getElementById('crmTodayActionFollow'),reservation=document.getElementById('crmTodayActionReservation');try{var data=await api('/api/today-dashboard');if(!data.ok)throw new Error(data.message||'load failed');var lineItems=(data.line_pending||[]).slice(0,8);var followItems=(data.follow_tasks||[]).slice(0,8);var resItems=(data.reservation_alerts||[]).slice(0,8);if(line)line.innerHTML=lineItems.length?lineItems.map(lineRow).join(''):'<div class="crm-today-action-empty">未送信LINEはありません。</div>';if(follow)follow.innerHTML=followItems.length?followItems.map(followRow).join(''):'<div class="crm-today-action-empty">今日対応のフォロー予定はありません。</div>';if(reservation)reservation.innerHTML=resItems.length?resItems.map(reservationRow).join(''):'<div class="crm-today-action-empty">予約連携の要確認はありません。</div>';}catch(e){if(line)line.innerHTML='<div class="crm-today-action-empty">読み込み失敗：'+esc(e.message||e)+'</div>';}}
  async function runAction(kind,id){var url='',label=''; if(kind==='line'){url='/api/today-dashboard/actions/line/'+id+'/mark-sent';label='LINE送信済みにしました';} if(kind==='follow'){url='/api/today-dashboard/actions/follow/'+id+'/complete';label='フォロー完了にしました';} if(kind==='reservation'){url='/api/today-dashboard/actions/reservation/'+id+'/resync';label='予約連携を再同期しました';} if(!url)return; if(!confirm('この操作を実行しますか？'))return; var data=await api(url,{method:'POST',body:'{}'}); if(!data.ok){toast('失敗：'+(data.message||data.status||'unknown'));return;} toast(label); loadActions(); var reload=document.getElementById('crmTodayReload'); if(reload)reload.click(); var alertReload=document.getElementById('crmLinkAlertReloadBtn'); if(alertReload)alertReload.click();}
  async function showLogs(){var data=await api('/api/today-dashboard/action-logs?limit=20'); if(!data.ok){toast('ログを取得できませんでした');return;} var rows=(data.logs||[]).map(function(x){return short(x.created_at)+' / '+esc(x.action_type)+' / '+esc(x.customer_name||x.customer_id||'-')+' / '+esc(x.result||'')}).join('\n'); alert(rows||'操作ログはまだありません。');}
  document.addEventListener('click',function(e){var t=e.target;if(!t)return; if(t.id==='crmTodayActionReload'){loadActions();toast('操作リストを更新しました')} if(t.id==='crmTodayActionLogs'){showLogs()} var line=t.getAttribute&&t.getAttribute('data-today-line-sent'); if(line)runAction('line',line); var follow=t.getAttribute&&t.getAttribute('data-today-follow-complete'); if(follow)runAction('follow',follow); var res=t.getAttribute&&t.getAttribute('data-today-resync'); if(res)runAction('reservation',res);});
  document.addEventListener('DOMContentLoaded',install); setTimeout(install,1000); setInterval(loadActions,120000);
})();
</script>`;

  return html.replace("</head>", `${style}</head>`).replace("</body>", `${script}</body>`);
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    if ((url.pathname === "/" || url.pathname === "/health" || url.pathname === "/api/health") && request.method === "GET") {
      return json({ ok: true, service: "customer-crm-api", build: BUILD, time: new Date().toISOString() });
    }

    const lineMatch = url.pathname.match(/^\/api\/today-dashboard\/actions\/line\/(\d+)\/mark-sent$/);
    if (lineMatch && request.method === "POST") return markLineSent(request, env, lineMatch[1]);

    const followMatch = url.pathname.match(/^\/api\/today-dashboard\/actions\/follow\/(\d+)\/complete$/);
    if (followMatch && request.method === "POST") return completeFollowTask(request, env, followMatch[1]);

    const reservationMatch = url.pathname.match(/^\/api\/today-dashboard\/actions\/reservation\/(\d+)\/resync$/);
    if (reservationMatch && request.method === "POST") return resyncReservation(request, env, ctx, reservationMatch[1]);

    if (url.pathname === "/api/today-dashboard/action-logs" && request.method === "GET") return actionLogsApi(request, env);

    const res = await app.fetch(request, env, ctx);
    const type = res.headers.get("content-type") || "";
    if (type.includes("text/html")) {
      const body = injectTodayActionUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }
    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};