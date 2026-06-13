// ======================================================
// CUSTOMER CRM / TODAY ACTION DASHBOARD WRAPPER
// build: customer-crm-api-today-dashboard-20260613-01
// Adds a top dashboard for today's CRM actions: reservation alerts, LINE drafts, follow tasks, and sales focus.
// ======================================================

import app from "./production-index-crm-daily-alert-summary.js";

const BUILD = "customer-crm-api-today-dashboard-20260613-01";
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

function csv(textBody, filename) {
  return new Response("\ufeff" + textBody, {
    status: 200,
    headers: securityHeaders({
      "content-type": "text/csv; charset=utf-8",
      "content-disposition": `attachment; filename="${filename}"`
    })
  });
}

function csvCell(v) {
  let s = text(v).replace(/\r?\n/g, " ");
  if (/^[=+\-@]/.test(s)) s = "'" + s;
  return `"${s.replace(/"/g, '""')}"`;
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

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_follow_tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT NOT NULL,
    customer_name TEXT,
    task_type TEXT,
    title TEXT NOT NULL,
    message_text TEXT,
    due_date TEXT,
    priority TEXT DEFAULT 'medium',
    status TEXT DEFAULT 'open',
    created_by TEXT,
    completed_by TEXT,
    completed_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_reservation_link_alert_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    draft_id INTEGER NOT NULL,
    customer_id TEXT,
    stage_key TEXT NOT NULL,
    acknowledged_at TEXT DEFAULT CURRENT_TIMESTAMP,
    acknowledged_by TEXT,
    note TEXT
  )`).run();

  for (const col of [
    "reservation_intake_id TEXT",
    "sent_to_reservation_at TEXT",
    "reservation_app_reservation_id TEXT",
    "reservation_app_created_at TEXT",
    "history_synced_at TEXT",
    "reservation_app_updated_at TEXT",
    "reservation_app_cancelled_at TEXT",
    "cancellation_synced_at TEXT"
  ]) await addColumn(env.DB, "crm_reservation_drafts", col);

  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_today_line_status ON customer_line_draft_logs(status, priority, created_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_today_follow_due ON crm_follow_tasks(status, due_date, priority)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_today_reservation_drafts ON crm_reservation_drafts(status, sent_to_reservation_at, reservation_app_created_at, history_synced_at, reservation_app_cancelled_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_today_alert_checks ON crm_reservation_link_alert_checks(draft_id, stage_key, acknowledged_at)`).run();
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

function jstDate(value) {
  if (!value) return "";
  const s = String(value);
  const d = new Date(s.includes("T") ? s : s.replace(" ", "T") + "Z");
  if (Number.isNaN(d.getTime())) return s.slice(0, 10);
  const jst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10);
}

function todayJst() {
  const jst = new Date(Date.now() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10);
}

function classifyReservationAlert(row) {
  const cancelled = !!(row.reservation_app_cancelled_at || row.status === "cancelled");
  const sent = !!row.sent_to_reservation_at;
  const created = !!row.reservation_app_created_at || !!row.reservation_app_reservation_id || row.status === "created";
  const historySynced = !!row.history_synced_at;
  const cancelSynced = !!row.cancellation_synced_at;

  if (cancelled && !cancelSynced) {
    return { stage_key: "cancel_unsynced", stage: "キャンセル未同期", severity: "danger", reason: "キャンセルがCRM履歴へ未反映です。", alert_time: text(row.reservation_app_cancelled_at || row.updated_at || row.created_at) };
  }
  if (!cancelled && created && !historySynced) {
    return { stage_key: "created_unreflected", stage: "本予約作成済み・CRM履歴未反映", severity: "danger", reason: "本予約IDはありますが、CRM予約履歴への反映が未完了です。", alert_time: text(row.reservation_app_created_at || row.updated_at || row.created_at) };
  }
  if (!cancelled && sent && !created) {
    return { stage_key: "sent_uncreated", stage: "送信済み・本予約未作成", severity: "warn", reason: "予約管理へ送信済みですが、本予約作成がまだです。", alert_time: text(row.sent_to_reservation_at || row.updated_at || row.created_at) };
  }
  return null;
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

async function safeFirst(env, sql, bindings = []) {
  try {
    const stmt = env.DB.prepare(sql);
    return bindings.length ? await stmt.bind(...bindings).first() : await stmt.first();
  } catch (_) {
    return null;
  }
}

async function getReservationAlerts(env, today) {
  const rows = await safeAll(env, `SELECT * FROM crm_reservation_drafts
    ORDER BY COALESCE(reservation_app_cancelled_at, reservation_app_updated_at, reservation_app_created_at, sent_to_reservation_at, updated_at, created_at, '') DESC
    LIMIT 1000`);
  const draftIds = rows.map((r) => Number(r.id)).filter((id) => Number.isFinite(id) && id > 0);
  const ackMap = new Map();
  if (draftIds.length) {
    const placeholders = draftIds.map(() => "?").join(",");
    const acks = await safeAll(env, `SELECT draft_id, stage_key, MAX(acknowledged_at) AS acknowledged_at, acknowledged_by
      FROM crm_reservation_link_alert_checks
      WHERE draft_id IN (${placeholders})
      GROUP BY draft_id, stage_key`, draftIds);
    for (const ack of acks) ackMap.set(`${ack.draft_id}:${ack.stage_key}`, ack);
  }

  const alerts = [];
  let sentToday = 0;
  let createdToday = 0;
  let cancelledToday = 0;
  let updatedToday = 0;
  let historySyncedToday = 0;

  for (const row of rows) {
    const alert = classifyReservationAlert(row);
    if (alert) {
      const ack = ackMap.get(`${row.id}:${alert.stage_key}`);
      const acknowledged = !!(ack?.acknowledged_at && (!alert.alert_time || ack.acknowledged_at >= alert.alert_time));
      alerts.push({ ...row, ...alert, acknowledged, acknowledged_at: acknowledged ? ack.acknowledged_at : "", acknowledged_by: acknowledged ? ack.acknowledged_by : "" });
    }
    if (jstDate(row.sent_to_reservation_at) === today) sentToday += 1;
    if (jstDate(row.reservation_app_created_at) === today) createdToday += 1;
    if (jstDate(row.reservation_app_cancelled_at) === today) cancelledToday += 1;
    if (jstDate(row.reservation_app_updated_at) === today) updatedToday += 1;
    if (jstDate(row.history_synced_at) === today) historySyncedToday += 1;
  }
  return {
    rows,
    alerts: alerts.sort((a, b) => String(b.alert_time || "").localeCompare(String(a.alert_time || ""))),
    counts: { sent_today: sentToday, created_today: createdToday, cancelled_today: cancelledToday, updated_today: updatedToday, history_synced_today: historySyncedToday }
  };
}

async function getTodayDashboard(env) {
  await ensureSchema(env);
  const today = todayJst();
  const reservation = await getReservationAlerts(env, today);
  const openReservationAlerts = reservation.alerts.filter((a) => !a.acknowledged);

  const linePending = await safeAll(env, `SELECT id, customer_id, customer_name, action_type, action_label, priority, status, created_at, updated_at, message_text
    FROM customer_line_draft_logs
    WHERE COALESCE(status,'') NOT IN ('sent','送信済み')
    ORDER BY CASE lower(COALESCE(priority,'')) WHEN 'high' THEN 0 WHEN 'urgent' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END, datetime(COALESCE(updated_at, created_at, '1970-01-01')) DESC
    LIMIT 30`);

  const followTasks = await safeAll(env, `SELECT id, customer_id, customer_name, task_type, title, message_text, due_date, priority, status, created_at, updated_at
    FROM crm_follow_tasks
    WHERE COALESCE(status,'open') NOT IN ('completed','done','closed')
      AND (due_date IS NULL OR due_date='' OR date(due_date) <= date(?))
    ORDER BY CASE lower(COALESCE(priority,'')) WHEN 'high' THEN 0 WHEN 'urgent' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END, date(COALESCE(due_date, ?)) ASC, datetime(COALESCE(updated_at, created_at, '1970-01-01')) DESC
    LIMIT 30`, [today, today]);

  const overdueTasks = followTasks.filter((t) => text(t.due_date) && text(t.due_date).slice(0, 10) < today);
  const highLinePending = linePending.filter((l) => ["high", "urgent", "高"].includes(text(l.priority).toLowerCase()));

  const sales = await safeFirst(env, `SELECT
      COUNT(*) AS customer_count,
      COALESCE(SUM(CAST(total_revenue AS INTEGER)), 0) AS total_revenue,
      COALESCE(AVG(NULLIF(CAST(total_revenue AS INTEGER), 0)), 0) AS avg_revenue,
      SUM(CASE WHEN CAST(COALESCE(repeat_count,0) AS INTEGER) >= 2 THEN 1 ELSE 0 END) AS repeat_customers,
      SUM(CASE WHEN CAST(COALESCE(dormant_days,0) AS INTEGER) >= 180 THEN 1 ELSE 0 END) AS dormant_customers
    FROM customers
    WHERE COALESCE(deleted_at,'')=''`) || {};

  const salesFocus = await safeAll(env, `SELECT customer_id, customer_name, total_revenue, repeat_count, dormant_days, last_shoot_date, genre_history, line_user_id
    FROM customers
    WHERE COALESCE(deleted_at,'')=''
      AND (
        CAST(COALESCE(dormant_days,0) AS INTEGER) >= 180
        OR CAST(COALESCE(total_revenue,0) AS INTEGER) >= 80000
        OR CAST(COALESCE(repeat_count,0) AS INTEGER) >= 2
      )
    ORDER BY CAST(COALESCE(total_revenue,0) AS INTEGER) DESC, CAST(COALESCE(dormant_days,0) AS INTEGER) DESC
    LIMIT 12`);

  const priorityItems = [];
  for (const r of openReservationAlerts.slice(0, 8)) priorityItems.push({ type: "reservation_alert", severity: r.severity, label: r.stage, customer_id: r.customer_id, customer_name: r.customer_name, title: r.reason, meta: `下書きID ${r.id} / 予約ID ${r.reservation_app_reservation_id || '-'}`, draft_id: r.id });
  for (const t of overdueTasks.slice(0, 6)) priorityItems.push({ type: "follow_overdue", severity: "danger", label: "期限超過フォロー", customer_id: t.customer_id, customer_name: t.customer_name, title: t.title, meta: `期限 ${t.due_date || '-'} / 優先度 ${t.priority || '-'}`, task_id: t.id });
  for (const t of followTasks.filter((x) => !overdueTasks.some((o) => o.id === x.id)).slice(0, 5)) priorityItems.push({ type: "follow_today", severity: "warn", label: "今日フォロー", customer_id: t.customer_id, customer_name: t.customer_name, title: t.title, meta: `期限 ${t.due_date || '-'} / 優先度 ${t.priority || '-'}`, task_id: t.id });
  for (const l of highLinePending.slice(0, 5)) priorityItems.push({ type: "line_pending", severity: "warn", label: "LINE未送信", customer_id: l.customer_id, customer_name: l.customer_name, title: l.action_label || l.action_type || "LINE文面", meta: `保存 ${l.created_at || '-'} / 優先度 ${l.priority || '-'}`, line_log_id: l.id });

  const counts = {
    reservation_alerts: openReservationAlerts.length,
    reservation_danger: openReservationAlerts.filter((a) => a.severity === "danger").length,
    line_pending: linePending.length,
    line_high: highLinePending.length,
    follow_due: followTasks.length,
    follow_overdue: overdueTasks.length,
    sent_today: reservation.counts.sent_today,
    created_today: reservation.counts.created_today,
    cancelled_today: reservation.counts.cancelled_today,
    sales_total: num(sales.total_revenue),
    customer_count: num(sales.customer_count),
    repeat_customers: num(sales.repeat_customers),
    dormant_customers: num(sales.dormant_customers)
  };

  return {
    ok: true,
    build: BUILD,
    date_jst: today,
    counts,
    priority_items: priorityItems.slice(0, 20),
    reservation_alerts: openReservationAlerts.slice(0, 12),
    line_pending: linePending.slice(0, 15),
    follow_tasks: followTasks.slice(0, 15),
    sales_focus: salesFocus,
    checked_at: new Date().toISOString()
  };
}

async function todayDashboardApi(request, env) {
  const auth = await requireReader(request, env);
  if (!auth.ok) return auth.response;
  return json(await getTodayDashboard(env));
}

async function todayDashboardCsv(request, env) {
  const auth = await requireReader(request, env);
  if (!auth.ok) return auth.response;
  const data = await getTodayDashboard(env);
  const header = ["区分", "ID", "顧客ID", "顧客名", "内容", "状態/優先度", "メモ", "日付", "予約ID"];
  const lines = [header.map(csvCell).join(",")];

  for (const x of data.priority_items || []) {
    lines.push(["優先対応", x.task_id || x.line_log_id || x.draft_id || "", x.customer_id, x.customer_name, x.title, x.label, x.meta, data.date_jst, ""].map(csvCell).join(","));
  }
  for (const r of data.reservation_alerts || []) {
    lines.push(["予約連携アラート", r.id, r.customer_id, r.customer_name, r.reason, r.stage, r.severity, r.alert_time, r.reservation_app_reservation_id].map(csvCell).join(","));
  }
  for (const l of data.line_pending || []) {
    lines.push(["LINE未送信", l.id, l.customer_id, l.customer_name, l.action_label || l.action_type, l.priority, l.status, l.created_at, ""].map(csvCell).join(","));
  }
  for (const t of data.follow_tasks || []) {
    lines.push(["フォロー予定", t.id, t.customer_id, t.customer_name, t.title, t.priority, t.status, t.due_date, ""].map(csvCell).join(","));
  }
  for (const s of data.sales_focus || []) {
    lines.push(["売上/リピート注目", "", s.customer_id, s.customer_name, s.genre_history, `売上${s.total_revenue || 0} / 回数${s.repeat_count || 0}`, `休眠${s.dormant_days || 0}日`, s.last_shoot_date, ""].map(csvCell).join(","));
  }
  return csv(lines.join("\n"), `crm-today-dashboard-${data.date_jst}.csv`);
}

function injectTodayDashboardUi(html) {
  if (!html || html.includes("crmTodayDashboardScript")) return html;

  const style = `<style id="crmTodayDashboardStyle">
.crm-today-dash{margin:14px auto 18px;max-width:1180px;border:1px solid #dbeafe;background:linear-gradient(135deg,#eff6ff,#fff);border-radius:20px;padding:14px;box-shadow:0 12px 34px rgba(37,99,235,.08);font-family:inherit;color:#0f172a}.crm-today-dash-head{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap}.crm-today-dash-title{font-size:20px;font-weight:950;margin:0}.crm-today-dash-sub{font-size:12px;color:#64748b;margin:4px 0 0}.crm-today-dash-actions{display:flex;gap:8px;flex-wrap:wrap}.crm-today-dash-actions button,.crm-today-dash-actions a{border:1px solid #bfdbfe;background:#fff;border-radius:11px;padding:8px 10px;font-size:12px;font-weight:900;color:#1e3a8a;text-decoration:none;cursor:pointer}.crm-today-dash-actions .danger{background:#991b1b;border-color:#991b1b;color:#fff}.crm-today-kpis{display:grid;grid-template-columns:repeat(6,minmax(110px,1fr));gap:8px;margin-top:12px}.crm-today-kpi{border:1px solid #dbeafe;background:#fff;border-radius:15px;padding:10px}.crm-today-kpi b{display:block;font-size:24px;line-height:1}.crm-today-kpi span{display:block;font-size:11px;color:#64748b;font-weight:900;margin-top:5px}.crm-today-kpi.alert{border-color:#fecaca;background:#fff7f7;color:#991b1b}.crm-today-kpi.warn{border-color:#fde68a;background:#fffbeb;color:#92400e}.crm-today-body{display:grid;grid-template-columns:1.1fr .9fr;gap:10px;margin-top:12px}.crm-today-box{border:1px solid #e2e8f0;background:#fff;border-radius:15px;padding:10px;min-width:0}.crm-today-box h3{font-size:14px;margin:0 0 8px;font-weight:950}.crm-today-row{border-top:1px solid #f1f5f9;padding:8px 0;font-size:13px}.crm-today-row:first-of-type{border-top:0}.crm-today-row b{font-weight:950}.crm-today-badge{display:inline-block;border-radius:999px;background:#fee2e2;color:#991b1b;font-size:11px;font-weight:950;padding:3px 7px;margin-right:5px}.crm-today-badge.warn{background:#fef3c7;color:#92400e}.crm-today-badge.ok{background:#dcfce7;color:#166534}.crm-today-meta{font-size:11px;color:#64748b;margin-top:3px;line-height:1.45}.crm-today-mini-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-top:10px}@media(max-width:860px){.crm-today-dash{margin:10px}.crm-today-kpis{grid-template-columns:repeat(2,1fr)}.crm-today-body,.crm-today-mini-grid{grid-template-columns:1fr}}
</style>`;

  const script = `<script id="crmTodayDashboardScript">
(function(){
  if(window.__crmTodayDashboardInstalled)return; window.__crmTodayDashboardInstalled=true;
  function esc(v){return String(v == null ? '' : v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
  function yen(v){var n=Number(v||0);return n.toLocaleString('ja-JP');}
  function short(v){return v ? String(v).replace('T',' ').slice(0,16) : '-';}
  function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
  function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:1000003;background:#0f172a;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.18)';document.body.appendChild(d);setTimeout(function(){d.remove()},2600)}
  function panelHtml(){return '<section id="crmTodayDashboard" class="crm-today-dash"><div class="crm-today-dash-head"><div><h2 class="crm-today-dash-title">今日やることダッシュボード</h2><p class="crm-today-dash-sub">予約連携・LINE未送信・フォロー予定・売上フォローをまとめて確認できます。</p></div><div class="crm-today-dash-actions"><button id="crmTodayReload">更新</button><button id="crmTodayAlerts" class="danger">要確認を見る</button><button id="crmTodayLinePending">LINE未送信</button><button id="crmTodayFollow">今日対応</button><a href="/api/today-dashboard.csv">CSV</a></div></div><div id="crmTodayKpis" class="crm-today-kpis"><div class="crm-today-kpi"><b>...</b><span>読み込み中</span></div></div><div class="crm-today-body"><div class="crm-today-box"><h3>優先対応</h3><div id="crmTodayPriority"><div class="crm-today-row">読み込み中...</div></div></div><div class="crm-today-box"><h3>フォロー予定・LINE</h3><div id="crmTodayFollowLine"><div class="crm-today-row">読み込み中...</div></div></div></div><div class="crm-today-mini-grid"><div class="crm-today-box"><h3>売上・リピート注目</h3><div id="crmTodaySales"><div class="crm-today-row">読み込み中...</div></div></div><div class="crm-today-box"><h3>予約連携の今日の動き</h3><div id="crmTodayReservation"><div class="crm-today-row">読み込み中...</div></div></div></div></section>';}
  function install(){if(document.getElementById('crmTodayDashboard'))return; var target=document.querySelector('main')||document.querySelector('#app')||document.querySelector('.container')||document.body; if(target===document.body){document.body.insertAdjacentHTML('afterbegin',panelHtml());}else{target.insertAdjacentHTML('afterbegin',panelHtml());} loadToday();}
  function itemBadge(x){var cls=x.severity==='danger'?'':'warn';return '<span class="crm-today-badge '+cls+'">'+esc(x.label||x.type||'要対応')+'</span>';}
  async function loadToday(){var k=document.getElementById('crmTodayKpis'),p=document.getElementById('crmTodayPriority'),fl=document.getElementById('crmTodayFollowLine'),s=document.getElementById('crmTodaySales'),r=document.getElementById('crmTodayReservation');try{var data=await api('/api/today-dashboard');if(!data.ok)throw new Error(data.message||'load failed');var c=data.counts||{};if(k)k.innerHTML=[['予約要確認',c.reservation_alerts||0,(c.reservation_alerts||0)?'alert':''],['LINE未送信',c.line_pending||0,(c.line_pending||0)?'warn':''],['今日フォロー',c.follow_due||0,(c.follow_overdue||0)?'alert':''],['期限超過',c.follow_overdue||0,(c.follow_overdue||0)?'alert':''],['今日本予約',c.created_today||0,''],['売上合計',yen(c.sales_total||0)+'円','']].map(function(x){return '<div class="crm-today-kpi '+x[2]+'"><b>'+esc(x[1])+'</b><span>'+esc(x[0])+'</span></div>'}).join('');var pri=(data.priority_items||[]).slice(0,10);if(p)p.innerHTML=pri.length?pri.map(function(x){return '<div class="crm-today-row">'+itemBadge(x)+'<b>'+esc(x.customer_name||'-')+'</b><div>'+esc(x.title||'')+'</div><div class="crm-today-meta">'+esc(x.meta||'')+' / 顧客ID '+esc(x.customer_id||'-')+'</div></div>';}).join(''):'<div class="crm-today-row">優先対応はありません。</div>';var lines=[];(data.follow_tasks||[]).slice(0,6).forEach(function(x){lines.push('<div class="crm-today-row"><span class="crm-today-badge '+(String(x.due_date||'').slice(0,10)<data.date_jst?'':'warn')+'">フォロー</span><b>'+esc(x.customer_name||'-')+'</b><div>'+esc(x.title||'')+'</div><div class="crm-today-meta">期限 '+esc(x.due_date||'-')+' / 優先度 '+esc(x.priority||'-')+'</div></div>')});(data.line_pending||[]).slice(0,5).forEach(function(x){lines.push('<div class="crm-today-row"><span class="crm-today-badge warn">LINE</span><b>'+esc(x.customer_name||'-')+'</b><div>'+esc(x.action_label||x.action_type||'LINE文面')+'</div><div class="crm-today-meta">保存 '+short(x.created_at)+' / 優先度 '+esc(x.priority||'-')+'</div></div>')});if(fl)fl.innerHTML=lines.length?lines.join(''):'<div class="crm-today-row">今日対応のフォロー・LINEはありません。</div>';var sf=(data.sales_focus||[]).slice(0,8);if(s)s.innerHTML=sf.length?sf.map(function(x){return '<div class="crm-today-row"><span class="crm-today-badge ok">注目</span><b>'+esc(x.customer_name||'-')+'</b><div class="crm-today-meta">売上 '+yen(x.total_revenue)+'円 / 撮影 '+esc(x.repeat_count||0)+'回 / 休眠 '+esc(x.dormant_days||0)+'日</div><div class="crm-today-meta">'+esc(x.genre_history||'')+'</div></div>';}).join(''):'<div class="crm-today-row">売上・リピート注目顧客はありません。</div>';var rv=[];rv.push('<div class="crm-today-row"><b>今日送信</b> '+esc(c.sent_today||0)+'件</div>');rv.push('<div class="crm-today-row"><b>今日本予約</b> '+esc(c.created_today||0)+'件</div>');rv.push('<div class="crm-today-row"><b>今日キャンセル</b> '+esc(c.cancelled_today||0)+'件</div>');rv.push('<div class="crm-today-row"><b>CRM履歴反映</b> '+esc((data.counts||{}).history_synced_today||0)+'件</div>');if(r)r.innerHTML=rv.join('');}catch(e){if(k)k.innerHTML='<div class="crm-today-kpi alert"><b>!</b><span>読み込み失敗</span></div>';if(p)p.innerHTML='<div class="crm-today-row">読み込み失敗：'+esc(e.message||e)+'</div>';}}
  document.addEventListener('click',function(e){var x=e.target;if(!x)return;if(x.id==='crmTodayReload'){loadToday();toast('更新しました')}if(x.id==='crmTodayAlerts'){var b=document.getElementById('crmLinkAlertOpenBtn'); if(b)b.click(); else toast('アラート画面を開けませんでした')}if(x.id==='crmTodayLinePending'){var b=document.getElementById('crmLinePendingOnlyBtn')||document.querySelector('[data-crm-line-pending-filter]'); if(b)b.click(); else toast('LINE未送信フィルターを開けませんでした')}if(x.id==='crmTodayFollow'){var b=document.getElementById('crmTodayTasksBtn')||document.querySelector('[data-crm-today-tasks]'); if(b)b.click(); else toast('今日対応画面を開けませんでした')}});
  document.addEventListener('DOMContentLoaded',install); setTimeout(install,800); setInterval(loadToday,120000);
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

    if (url.pathname === "/api/today-dashboard" && request.method === "GET") return todayDashboardApi(request, env);
    if (url.pathname === "/api/today-dashboard.csv" && request.method === "GET") return todayDashboardCsv(request, env);

    const res = await app.fetch(request, env, ctx);
    const type = res.headers.get("content-type") || "";
    if (type.includes("text/html")) {
      const body = injectTodayDashboardUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }
    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
