// ======================================================
// CUSTOMER CRM / TODAY ACTION FILTERS WRAPPER
// build: customer-crm-api-today-action-filters-20260613-01
// Adds filtered quick-action queue: hide completed, filter by type, priority, and assignee.
// ======================================================

import app from "./production-index-crm-today-actions.js";

const BUILD = "customer-crm-api-today-action-filters-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const READ_ROLES = ["root_admin", "admin", "staff", "viewer"];

function text(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

function normalizeEmail(v) {
  return text(v).toLowerCase();
}

function num(v) {
  const n = Number(v || 0);
  return Number.isFinite(n) ? n : 0;
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

  await addColumn(env.DB, "customer_line_draft_logs", "assigned_to TEXT");
  await addColumn(env.DB, "crm_follow_tasks", "assigned_to TEXT");
  await addColumn(env.DB, "crm_reservation_drafts", "assigned_to TEXT");
  await addColumn(env.DB, "crm_reservation_drafts", "priority TEXT");

  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_today_line_filters ON customer_line_draft_logs(status, priority, assigned_to, created_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_today_follow_filters ON crm_follow_tasks(status, due_date, priority, assigned_to)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_today_reservation_filters ON crm_reservation_drafts(status, priority, assigned_to, updated_at)`).run();
}

async function requireReader(request, env) {
  await ensureSchema(env);
  const email = getAccessEmail(request);
  if (!email) return { ok: false, response: json({ ok: false, message: "Login required" }, 401) };
  const user = await env.DB.prepare(
    `SELECT email, role, status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`
  ).bind(email).first();
  if (!user) return { ok: false, response: json({ ok: false, message: "User is not allowed" }, 403) };
  if (!READ_ROLES.includes(user.role || "")) return { ok: false, response: json({ ok: false, message: "Permission denied" }, 403) };
  return { ok: true, email, user };
}

async function safeAll(env, sql, bindings = []) {
  try {
    const stmt = env.DB.prepare(sql);
    const res = bindings.length ? await stmt.bind(...bindings).all() : await stmt.all();
    return res.results || [];
  } catch (_) {
    return [];
  }
}

function priorityKey(v, fallback = "medium") {
  const s = text(v).toLowerCase();
  if (["urgent", "high", "danger", "高", "至急"].includes(s)) return "high";
  if (["low", "低"].includes(s)) return "low";
  if (["warn", "medium", "normal", "中", "通常"].includes(s)) return "medium";
  if (["ok", "done", "completed"].includes(s)) return "low";
  return fallback;
}

function priorityRank(v) {
  const p = priorityKey(v);
  if (p === "high") return 0;
  if (p === "medium") return 1;
  return 2;
}

function assigneeValue(row) {
  return text(row.assigned_to || row.owner_email || row.staff_email || row.created_by || "");
}

function assigneeMatches(row, assignee, actorEmail) {
  const filter = normalizeEmail(assignee || "all");
  if (!filter || filter === "all") return true;
  const assigned = normalizeEmail(assigneeValue(row));
  if (filter === "me") return assigned === normalizeEmail(actorEmail);
  if (filter === "unassigned") return !assigned;
  return assigned === filter;
}

function typeMatches(type, expected) {
  return !type || type === "all" || type === expected;
}

function priorityMatches(rowPriority, filter) {
  const p = text(filter || "all").toLowerCase();
  if (!p || p === "all") return true;
  return priorityKey(rowPriority) === priorityKey(p);
}

function classifyReservation(row) {
  const cancelled = !!(row.reservation_app_cancelled_at || row.status === "cancelled");
  const sent = !!row.sent_to_reservation_at;
  const created = !!row.reservation_app_created_at || !!row.reservation_app_reservation_id || row.status === "created";
  const historySynced = !!row.history_synced_at;
  const cancelSynced = !!row.cancellation_synced_at;
  const updatedSynced = !!row.reservation_app_updated_at;

  if (cancelled && !cancelSynced) {
    return { open: true, stage: "キャンセル未同期", severity: "danger", priority: "high", reason: "キャンセルがCRM履歴へ未反映です。" };
  }
  if (!cancelled && created && !historySynced) {
    return { open: true, stage: "本予約作成済み・CRM履歴未反映", severity: "danger", priority: "high", reason: "本予約IDはありますが、CRM予約履歴への反映が未完了です。" };
  }
  if (!cancelled && sent && !created) {
    return { open: true, stage: "送信済み・本予約未作成", severity: "warn", priority: "medium", reason: "予約管理へ送信済みですが、本予約作成がまだです。" };
  }
  if (!sent && !created && !cancelled) {
    return { open: true, stage: "未送信", severity: "warn", priority: priorityKey(row.priority || "medium"), reason: "CRM下書きが予約管理へ未送信です。" };
  }
  if (cancelled && cancelSynced) {
    return { open: false, stage: "キャンセル同期済み", severity: "ok", priority: "low", reason: "キャンセル同期済みです。" };
  }
  if (created && historySynced && updatedSynced) {
    return { open: false, stage: "変更同期済み", severity: "ok", priority: "low", reason: "本予約作成・変更同期済みです。" };
  }
  if (created && historySynced) {
    return { open: false, stage: "CRM履歴反映済み", severity: "ok", priority: "low", reason: "CRM予約履歴へ反映済みです。" };
  }
  return { open: false, stage: "確認不要", severity: "ok", priority: "low", reason: "連携済みです。" };
}

function makeLineItem(row) {
  const completed = ["sent", "送信済み"].includes(text(row.status).toLowerCase()) || !!row.sent_at;
  return {
    kind: "line",
    id: row.id,
    customer_id: row.customer_id,
    customer_name: row.customer_name,
    title: row.action_label || row.action_type || "LINE文面",
    meta: `保存 ${row.created_at || "-"} / 優先度 ${row.priority || "-"}`,
    priority: priorityKey(row.priority || "medium"),
    assignee: assigneeValue(row),
    status: completed ? "送信済み" : "未送信",
    completed,
    action_label: completed ? "完了済み" : "LINE送信済み",
    action_url: completed ? "" : `/api/today-dashboard/actions/line/${row.id}/mark-sent`,
    updated_at: row.updated_at || row.created_at || ""
  };
}

function makeFollowItem(row) {
  const completed = ["completed", "done", "closed", "完了"].includes(text(row.status).toLowerCase()) || !!row.completed_at;
  const overdue = text(row.due_date) && text(row.due_date).slice(0, 10) < new Date(Date.now() + 9 * 60 * 60 * 1000).toISOString().slice(0, 10);
  return {
    kind: "follow",
    id: row.id,
    customer_id: row.customer_id,
    customer_name: row.customer_name,
    title: row.title || "フォロー予定",
    meta: `期限 ${row.due_date || "-"} / 優先度 ${row.priority || "-"}`,
    priority: priorityKey(row.priority || (overdue ? "high" : "medium")),
    assignee: assigneeValue(row),
    status: completed ? "完了" : (overdue ? "期限超過" : "未完了"),
    completed,
    action_label: completed ? "完了済み" : "フォロー完了",
    action_url: completed ? "" : `/api/today-dashboard/actions/follow/${row.id}/complete`,
    updated_at: row.updated_at || row.created_at || ""
  };
}

function makeReservationItem(row) {
  const state = classifyReservation(row);
  return {
    kind: "reservation",
    id: row.id,
    customer_id: row.customer_id,
    customer_name: row.customer_name,
    title: state.stage,
    meta: `下書きID ${row.id} / 予約ID ${row.reservation_app_reservation_id || "-"} / ${state.reason}`,
    priority: state.priority,
    assignee: assigneeValue(row),
    status: state.stage,
    completed: !state.open,
    action_label: state.open ? "自動再同期" : "完了済み",
    action_url: state.open ? `/api/today-dashboard/actions/reservation/${row.id}/resync` : "",
    updated_at: row.updated_at || row.created_at || "",
    severity: state.severity
  };
}

async function actionQueueApi(request, env) {
  const auth = await requireReader(request, env);
  if (!auth.ok) return auth.response;

  const url = new URL(request.url);
  const type = text(url.searchParams.get("type") || "all").toLowerCase();
  const priority = text(url.searchParams.get("priority") || "all").toLowerCase();
  const assignee = text(url.searchParams.get("assignee") || "all");
  const includeCompleted = ["1", "true", "yes"].includes(text(url.searchParams.get("include_completed")).toLowerCase()) || text(url.searchParams.get("hide_completed")) === "0";
  const limit = Math.min(Math.max(num(url.searchParams.get("limit") || 80), 1), 200);

  const items = [];

  if (typeMatches(type, "line")) {
    const rows = await safeAll(env, `SELECT id, customer_id, customer_name, action_type, action_label, priority, status, created_by, assigned_to, sent_by, sent_at, created_at, updated_at
      FROM customer_line_draft_logs
      ORDER BY datetime(COALESCE(updated_at, created_at, '1970-01-01')) DESC
      LIMIT 500`);
    for (const row of rows) items.push(makeLineItem(row));
  }

  if (typeMatches(type, "follow")) {
    const rows = await safeAll(env, `SELECT id, customer_id, customer_name, task_type, title, due_date, priority, status, created_by, assigned_to, completed_by, completed_at, created_at, updated_at
      FROM crm_follow_tasks
      ORDER BY date(COALESCE(due_date, '2999-12-31')) ASC, datetime(COALESCE(updated_at, created_at, '1970-01-01')) DESC
      LIMIT 500`);
    for (const row of rows) items.push(makeFollowItem(row));
  }

  if (typeMatches(type, "reservation")) {
    const rows = await safeAll(env, `SELECT * FROM crm_reservation_drafts
      ORDER BY datetime(COALESCE(updated_at, created_at, '1970-01-01')) DESC
      LIMIT 500`);
    for (const row of rows) items.push(makeReservationItem(row));
  }

  let filtered = items.filter((item) => {
    if (!includeCompleted && item.completed) return false;
    if (!priorityMatches(item.priority, priority)) return false;
    if (!assigneeMatches(item, assignee, auth.email)) return false;
    return true;
  });

  filtered.sort((a, b) => {
    const ca = a.completed ? 1 : 0;
    const cb = b.completed ? 1 : 0;
    if (ca !== cb) return ca - cb;
    const pa = priorityRank(a.priority);
    const pb = priorityRank(b.priority);
    if (pa !== pb) return pa - pb;
    return String(b.updated_at || "").localeCompare(String(a.updated_at || ""));
  });

  const counts = {
    total: filtered.length,
    open: filtered.filter((x) => !x.completed).length,
    completed: filtered.filter((x) => x.completed).length,
    line: filtered.filter((x) => x.kind === "line").length,
    follow: filtered.filter((x) => x.kind === "follow").length,
    reservation: filtered.filter((x) => x.kind === "reservation").length,
    high: filtered.filter((x) => x.priority === "high").length,
    medium: filtered.filter((x) => x.priority === "medium").length,
    low: filtered.filter((x) => x.priority === "low").length
  };

  return json({
    ok: true,
    build: BUILD,
    filters: { type, priority, assignee, include_completed: includeCompleted, actor_email: auth.email },
    counts,
    items: filtered.slice(0, limit),
    checked_at: new Date().toISOString()
  });
}

function injectFilterUi(html) {
  if (!html || html.includes("crmTodayFilterScript")) return html;

  const style = `<style id="crmTodayFilterStyle">
.crm-today-filter-panel{margin:10px auto 18px;max-width:1180px;border:1px solid #fed7aa;background:linear-gradient(135deg,#fff7ed,#ffffff);border-radius:18px;padding:12px;box-shadow:0 10px 28px rgba(249,115,22,.08);font-family:inherit;color:#0f172a}.crm-today-filter-head{display:flex;justify-content:space-between;gap:10px;align-items:flex-start;flex-wrap:wrap}.crm-today-filter-title{font-size:17px;font-weight:950;margin:0}.crm-today-filter-sub{font-size:12px;color:#64748b;margin:4px 0 0}.crm-today-filter-controls{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px;align-items:center}.crm-today-filter-controls select,.crm-today-filter-controls input{border:1px solid #fdba74;border-radius:10px;padding:8px 9px;font-size:12px;background:#fff}.crm-today-filter-controls label{font-size:12px;font-weight:900;color:#7c2d12}.crm-today-filter-controls button{border:0;background:#ea580c;color:#fff;border-radius:10px;padding:8px 10px;font-size:12px;font-weight:950;cursor:pointer}.crm-today-filter-kpis{display:grid;grid-template-columns:repeat(6,minmax(90px,1fr));gap:8px;margin-top:10px}.crm-today-filter-kpi{background:#fff;border:1px solid #ffedd5;border-radius:13px;padding:9px}.crm-today-filter-kpi b{display:block;font-size:20px}.crm-today-filter-kpi span{font-size:11px;color:#64748b;font-weight:900}.crm-today-filter-list{margin-top:10px;display:grid;grid-template-columns:repeat(2,1fr);gap:8px}.crm-today-filter-row{border:1px solid #ffedd5;background:#fff;border-radius:14px;padding:10px;font-size:13px}.crm-today-filter-row.done{opacity:.64}.crm-today-filter-row b{font-weight:950}.crm-today-filter-meta{font-size:11px;color:#64748b;margin-top:4px;line-height:1.45}.crm-today-filter-badge{display:inline-block;border-radius:999px;background:#fef3c7;color:#92400e;font-size:11px;font-weight:950;padding:3px 7px;margin-right:4px}.crm-today-filter-badge.high{background:#fee2e2;color:#991b1b}.crm-today-filter-badge.low{background:#dcfce7;color:#166534}.crm-today-filter-row button{border:0;background:#ea580c;color:#fff;border-radius:10px;padding:7px 9px;font-size:12px;font-weight:950;cursor:pointer;margin-top:7px}.crm-today-filter-empty{font-size:13px;color:#64748b;background:#fff;border:1px dashed #fdba74;border-radius:14px;padding:12px;margin-top:10px}@media(max-width:860px){.crm-today-filter-panel{margin:10px}.crm-today-filter-kpis{grid-template-columns:repeat(2,1fr)}.crm-today-filter-list{grid-template-columns:1fr}}
</style>`;

  const script = `<script id="crmTodayFilterScript">
(function(){
  if(window.__crmTodayFilterInstalled)return; window.__crmTodayFilterInstalled=true;
  function esc(v){return String(v == null ? '' : v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
  function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
  function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:1000005;background:#7c2d12;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.18)';document.body.appendChild(d);setTimeout(function(){d.remove()},2600)}
  function panel(){return '<section id="crmTodayFilterPanel" class="crm-today-filter-panel"><div class="crm-today-filter-head"><div><h2 class="crm-today-filter-title">今日のクイック操作フィルター</h2><p class="crm-today-filter-sub">完了済みを隠して、担当者・優先度・種別で今日の操作を絞り込めます。</p></div></div><div class="crm-today-filter-controls"><select id="crmTodayFilterType"><option value="all">すべて</option><option value="line">LINE</option><option value="follow">フォロー</option><option value="reservation">予約連携</option></select><select id="crmTodayFilterPriority"><option value="all">優先度すべて</option><option value="high">高</option><option value="medium">中</option><option value="low">低</option></select><select id="crmTodayFilterAssignee"><option value="all">担当すべて</option><option value="me">自分担当</option><option value="unassigned">未担当</option></select><label><input type="checkbox" id="crmTodayFilterCompleted"> 完了済みも表示</label><button id="crmTodayFilterApply">絞り込み</button></div><div id="crmTodayFilterKpis" class="crm-today-filter-kpis"></div><div id="crmTodayFilterList" class="crm-today-filter-list"><div class="crm-today-filter-empty">読み込み中...</div></div></section>'}
  function kpis(c){var a=[['open','未完了'],['completed','完了済み'],['line','LINE'],['follow','フォロー'],['reservation','予約'],['high','高優先']];return a.map(function(x){return '<div class="crm-today-filter-kpi"><b>'+esc(c&&c[x[0]]||0)+'</b><span>'+x[1]+'</span></div>'}).join('')}
  function row(x){var pri=x.priority||'medium';var cls=pri==='high'?'high':(pri==='low'?'low':'');var done=x.completed?' done':'';var btn=x.action_url?'<button data-crm-today-filter-action="'+esc(x.action_url)+'">'+esc(x.action_label||'実行')+'</button>':'<button disabled>完了済み</button>';return '<div class="crm-today-filter-row'+done+'"><div><span class="crm-today-filter-badge '+cls+'">'+esc(x.kind)+'</span><span class="crm-today-filter-badge '+cls+'">優先度 '+esc(pri)+'</span><span class="crm-today-filter-badge">'+esc(x.status||'-')+'</span></div><b>'+esc(x.customer_name||'-')+'</b><div>'+esc(x.title||'-')+'</div><div class="crm-today-filter-meta">'+esc(x.meta||'')+'<br>担当 '+esc(x.assignee||'未担当')+' / ID '+esc(x.id)+'</div>'+btn+'</div>'}
  function params(){var t=document.getElementById('crmTodayFilterType'),p=document.getElementById('crmTodayFilterPriority'),a=document.getElementById('crmTodayFilterAssignee'),c=document.getElementById('crmTodayFilterCompleted');return '?type='+(t?t.value:'all')+'&priority='+(p?p.value:'all')+'&assignee='+(a?a.value:'all')+'&include_completed='+(c&&c.checked?'1':'0')+'&limit=100'}
  async function load(){var list=document.getElementById('crmTodayFilterList'),kp=document.getElementById('crmTodayFilterKpis');try{var data=await api('/api/today-dashboard/action-queue'+params());if(!data.ok)throw new Error(data.message||'load failed');if(kp)kp.innerHTML=kpis(data.counts||{});if(list)list.innerHTML=(data.items||[]).length?(data.items||[]).map(row).join(''):'<div class="crm-today-filter-empty">該当する操作はありません。</div>';}catch(e){if(list)list.innerHTML='<div class="crm-today-filter-empty">読み込み失敗：'+esc(e.message||e)+'</div>';}}
  async function run(url){if(!url)return;if(!confirm('この操作を実行しますか？'))return;var data=await api(url,{method:'POST',body:'{}'});if(!data.ok){toast('失敗：'+(data.message||data.status||'unknown'));return;}toast('処理しました');load();var r=document.getElementById('crmTodayActionReload');if(r)r.click();var d=document.getElementById('crmTodayReload');if(d)d.click();}
  function install(){if(document.getElementById('crmTodayFilterPanel'))return;var base=document.getElementById('crmTodayActionPanel')||document.getElementById('crmTodayDashboard');if(base)base.insertAdjacentHTML('afterend',panel());else (document.querySelector('main')||document.body).insertAdjacentHTML('afterbegin',panel());load();}
  document.addEventListener('click',function(e){var t=e.target;if(!t)return;if(t.id==='crmTodayFilterApply'){load();toast('絞り込みました')}var u=t.getAttribute&&t.getAttribute('data-crm-today-filter-action');if(u)run(u);});
  document.addEventListener('DOMContentLoaded',install);setTimeout(install,1200);setInterval(load,120000);
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

    if (url.pathname === "/api/today-dashboard/action-queue" && request.method === "GET") return actionQueueApi(request, env);

    const res = await app.fetch(request, env, ctx);
    const type = res.headers.get("content-type") || "";
    if (type.includes("text/html")) {
      const body = injectFilterUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }
    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
