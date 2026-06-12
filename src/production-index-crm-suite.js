// ======================================================
// CUSTOMER CRM API / CRM SUITE WRAPPER
// build: customer-crm-api-suite-20260613-01
// Adds: bulk LINE sent, memos, tags, follow tasks, segments,
// analytics, reservation summary, mobile/detail UI helpers.
// ======================================================

import app from "./production-index-line-pending-csv.js";

const BUILD = "customer-crm-api-suite-20260613-01";
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

async function readJson(request) {
  try {
    return await request.json();
  } catch (_) {
    return {};
  }
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

function isEditor(current) {
  return !!current && ["root_admin", "admin", "staff"].includes(current.role || "");
}

async function ensureLineDraftLogSchema(env) {
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
    sent_by TEXT,
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
    "sent_by TEXT",
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

async function ensureSuiteSchema(env) {
  if (!env.DB) throw new Error("D1 DB binding(DB) is missing");
  await ensureLineDraftLogSchema(env);

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_customer_memos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT NOT NULL,
    memo_text TEXT NOT NULL,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_customer_tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT NOT NULL,
    tag TEXT NOT NULL,
    color TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id, tag)
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

  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_customer_memos_customer ON crm_customer_memos(customer_id, created_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_customer_tags_customer ON crm_customer_tags(customer_id, tag)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_follow_tasks_due ON crm_follow_tasks(status, due_date, priority)`).run();
}

function cleanCustomerIdFromPath(path, prefix, suffix = "") {
  let id = decodeURIComponent(path.replace(prefix, ""));
  if (suffix && id.endsWith(suffix)) id = id.slice(0, -suffix.length);
  return text(id).replace(/^\/+|\/+$/g, "");
}

async function bulkMarkLineSent(request, env) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "admin or staff permission required" }, 403);
  await ensureSuiteSchema(env);

  const body = await readJson(request);
  const allPending = body.all_pending === true || body.mode === "all_pending";
  const ids = Array.isArray(body.ids) ? body.ids.map((v) => parseInt(v, 10)).filter((v) => Number.isFinite(v) && v > 0) : [];

  if (allPending) {
    const res = await env.DB.prepare(`
      UPDATE customer_line_draft_logs
      SET status='sent', sent_at=datetime('now'), sent_by=?, updated_at=datetime('now')
      WHERE COALESCE(status, 'copied') <> 'sent'
    `).bind(current.email).run();
    return json({ ok: true, mode: "all_pending", updated: res.meta?.changes || 0 });
  }

  if (!ids.length) return json({ ok: false, message: "ids or all_pending required" }, 400);
  if (ids.length > 500) return json({ ok: false, message: "ids limit is 500" }, 400);

  const placeholders = ids.map(() => "?").join(",");
  const res = await env.DB.prepare(`
    UPDATE customer_line_draft_logs
    SET status='sent', sent_at=datetime('now'), sent_by=?, updated_at=datetime('now')
    WHERE id IN (${placeholders}) AND COALESCE(status, 'copied') <> 'sent'
  `).bind(current.email, ...ids).run();

  return json({ ok: true, mode: "ids", requested: ids.length, updated: res.meta?.changes || 0 });
}

async function getLineLogsOverview(request, env) {
  const current = await getCurrentUser(request, env);
  if (!current) return json({ ok: false, message: "Google login through Cloudflare Access is required" }, 401);
  await ensureSuiteSchema(env);

  const summary = await env.DB.prepare(`
    SELECT
      SUM(CASE WHEN COALESCE(status, 'copied') <> 'sent' THEN 1 ELSE 0 END) AS pending,
      SUM(CASE WHEN COALESCE(status, 'copied') = 'sent' THEN 1 ELSE 0 END) AS sent,
      COUNT(*) AS total,
      COUNT(DISTINCT CASE WHEN COALESCE(status, 'copied') <> 'sent' THEN customer_id END) AS pending_customers
    FROM customer_line_draft_logs
  `).first();

  const recent = await env.DB.prepare(`
    SELECT id, customer_id, customer_name, action_label, priority, status,
           COALESCE(copied_at, created_at, '') AS saved_at,
           COALESCE(sent_at, '') AS sent_at,
           message_text
    FROM customer_line_draft_logs
    ORDER BY COALESCE(copied_at, created_at) DESC
    LIMIT 30
  `).all();

  return json({ ok: true, summary: summary || {}, logs: recent.results || [] });
}

async function getCustomerMemos(request, env, customerId) {
  const current = await getCurrentUser(request, env);
  if (!current) return json({ ok: false, message: "login required" }, 401);
  await ensureSuiteSchema(env);
  const rows = await env.DB.prepare(`
    SELECT id, customer_id, memo_text, created_by, created_at, updated_at
    FROM crm_customer_memos
    WHERE customer_id=?
    ORDER BY created_at DESC, id DESC
    LIMIT 100
  `).bind(customerId).all();
  return json({ ok: true, customer_id: customerId, memos: rows.results || [] });
}

async function addCustomerMemo(request, env, customerId) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "edit permission required" }, 403);
  await ensureSuiteSchema(env);
  const body = await readJson(request);
  const memo = text(body.memo || body.memo_text || body.text);
  if (!memo) return json({ ok: false, message: "memo required" }, 400);

  const res = await env.DB.prepare(`
    INSERT INTO crm_customer_memos(customer_id, memo_text, created_by, created_at, updated_at)
    VALUES(?, ?, ?, datetime('now'), datetime('now'))
  `).bind(customerId, memo, current.email).run();

  return json({ ok: true, id: res.meta?.last_row_id || null, customer_id: customerId });
}

async function getCustomerTags(request, env, customerId) {
  const current = await getCurrentUser(request, env);
  if (!current) return json({ ok: false, message: "login required" }, 401);
  await ensureSuiteSchema(env);
  const rows = await env.DB.prepare(`
    SELECT id, customer_id, tag, color, created_by, created_at
    FROM crm_customer_tags
    WHERE customer_id=?
    ORDER BY tag ASC
  `).bind(customerId).all();
  return json({ ok: true, customer_id: customerId, tags: rows.results || [] });
}

async function addCustomerTag(request, env, customerId) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "edit permission required" }, 403);
  await ensureSuiteSchema(env);
  const body = await readJson(request);
  const tag = text(body.tag);
  const color = text(body.color);
  if (!tag) return json({ ok: false, message: "tag required" }, 400);

  await env.DB.prepare(`
    INSERT OR IGNORE INTO crm_customer_tags(customer_id, tag, color, created_by, created_at, updated_at)
    VALUES(?, ?, ?, ?, datetime('now'), datetime('now'))
  `).bind(customerId, tag, color, current.email).run();

  return json({ ok: true, customer_id: customerId, tag });
}

async function removeCustomerTag(request, env, customerId) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "edit permission required" }, 403);
  await ensureSuiteSchema(env);
  const body = await readJson(request);
  const tag = text(body.tag);
  if (!tag) return json({ ok: false, message: "tag required" }, 400);

  const res = await env.DB.prepare(`DELETE FROM crm_customer_tags WHERE customer_id=? AND tag=?`).bind(customerId, tag).run();
  return json({ ok: true, deleted: res.meta?.changes || 0 });
}

async function getFollowTasks(request, env, customerId = "") {
  const current = await getCurrentUser(request, env);
  if (!current) return json({ ok: false, message: "login required" }, 401);
  await ensureSuiteSchema(env);

  const url = new URL(request.url);
  const status = text(url.searchParams.get("status") || "open");
  const todayOnly = url.pathname === "/api/follow-tasks/today";
  const limit = Math.min(Math.max(parseInt(url.searchParams.get("limit") || "100", 10) || 100, 1), 300);

  let where = `WHERE 1=1`;
  const binds = [];
  if (customerId) {
    where += ` AND customer_id=?`;
    binds.push(customerId);
  }
  if (status !== "all") {
    where += ` AND COALESCE(status, 'open')=?`;
    binds.push(status);
  }
  if (todayOnly) {
    where += ` AND (due_date IS NULL OR due_date='' OR date(due_date) <= date('now'))`;
  }

  const rows = await env.DB.prepare(`
    SELECT id, customer_id, customer_name, task_type, title, message_text, due_date, priority,
           status, created_by, completed_by, completed_at, created_at, updated_at
    FROM crm_follow_tasks
    ${where}
    ORDER BY
      CASE COALESCE(priority, 'medium') WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
      COALESCE(due_date, '9999-12-31') ASC,
      id DESC
    LIMIT ?
  `).bind(...binds, limit).all();

  return json({ ok: true, tasks: rows.results || [] });
}

async function addFollowTask(request, env, customerId) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "edit permission required" }, 403);
  await ensureSuiteSchema(env);

  const body = await readJson(request);
  const title = text(body.title);
  if (!title) return json({ ok: false, message: "title required" }, 400);

  let customerName = text(body.customer_name);
  if (!customerName) {
    const c = await env.DB.prepare(`SELECT COALESCE(name, customer_id) AS name FROM customers WHERE customer_id=? LIMIT 1`).bind(customerId).first();
    customerName = c?.name || customerId;
  }

  const res = await env.DB.prepare(`
    INSERT INTO crm_follow_tasks(customer_id, customer_name, task_type, title, message_text, due_date, priority, status, created_by, created_at, updated_at)
    VALUES(?, ?, ?, ?, ?, ?, ?, 'open', ?, datetime('now'), datetime('now'))
  `).bind(
    customerId,
    customerName,
    text(body.task_type || "manual"),
    title,
    text(body.message_text || body.message || ""),
    text(body.due_date || ""),
    text(body.priority || "medium"),
    current.email
  ).run();

  return json({ ok: true, id: res.meta?.last_row_id || null });
}

async function completeFollowTask(request, env, taskId) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "edit permission required" }, 403);
  await ensureSuiteSchema(env);

  const res = await env.DB.prepare(`
    UPDATE crm_follow_tasks
    SET status='done', completed_by=?, completed_at=datetime('now'), updated_at=datetime('now')
    WHERE id=?
  `).bind(current.email, taskId).run();

  return json({ ok: true, updated: res.meta?.changes || 0 });
}

function segmentCondition(segment) {
  const s = text(segment);
  if (s === "dormant") return `CAST(COALESCE(dormant_days, 0) AS INTEGER) >= 180`;
  if (s === "vip") return `CAST(COALESCE(total_revenue, 0) AS INTEGER) >= 100000`;
  if (s === "repeat") return `CAST(COALESCE(repeat_count, 0) AS INTEGER) >= 2`;
  if (s === "line-linked") return `COALESCE(line_user_id, '') <> ''`;
  if (s === "line-unlinked") return `COALESCE(line_user_id, '') = ''`;
  if (s === "photo-ok") return `(photo_public_ok=1 OR photo_public_ok='1' OR lower(COALESCE(photo_public_ok,''))='true' OR photo_public_ok='OK')`;
  if (s === "753") return `COALESCE(genre_history, '') LIKE '%七五三%'`;
  if (s === "omiyamairi") return `COALESCE(genre_history, '') LIKE '%お宮参り%'`;
  if (s === "pending-line") return `customer_id IN (SELECT customer_id FROM customer_line_draft_logs WHERE COALESCE(status, 'copied') <> 'sent')`;
  return `1=1`;
}

async function getSegmentCustomers(request, env, segment) {
  const current = await getCurrentUser(request, env);
  if (!current) return json({ ok: false, message: "login required" }, 401);
  await ensureSuiteSchema(env);

  const url = new URL(request.url);
  const limit = Math.min(Math.max(parseInt(url.searchParams.get("limit") || "100", 10) || 100, 1), 300);
  const condition = segmentCondition(segment);

  const rows = await env.DB.prepare(`
    SELECT customer_id, name, line_display_name, phone, email, genre_history, last_shoot_date,
           dormant_days, total_revenue, repeat_count, line_user_id, photo_public_ok
    FROM customers
    WHERE (${condition})
      AND (deleted_at IS NULL OR deleted_at='')
    ORDER BY
      CAST(COALESCE(dormant_days, 0) AS INTEGER) DESC,
      CAST(COALESCE(total_revenue, 0) AS INTEGER) DESC
    LIMIT ?
  `).bind(limit).all();

  return json({ ok: true, segment, customers: rows.results || [] });
}

function draftMessageForSegment(customer, segment) {
  const name = text(customer.name || customer.line_display_name || "お客様") + "様";
  if (segment === "dormant") {
    return `${name}\nこんにちは。水野写真の水野です。\n前回の撮影から少し期間が空いているため、ご家族写真や季節の記念撮影のご案内でご連絡しました。\nまた良いタイミングがあれば、いつでもご相談ください。`;
  }
  if (segment === "753") {
    return `${name}\nこんにちは。水野写真の水野です。\n七五三やご兄弟撮影のご相談が増える時期になりました。\n日程や神社、服装なども事前にLINEでご相談いただけます。`;
  }
  if (segment === "omiyamairi") {
    return `${name}\nこんにちは。水野写真の水野です。\n以前のお宮参り撮影から、お子さまの成長記録としてバースデーや七五三のご相談も承っています。\n気になる時期があればお気軽にご相談ください。`;
  }
  if (segment === "vip") {
    return `${name}\nいつもありがとうございます。水野写真の水野です。\nこれまで大切な節目を撮影させていただき、ありがとうございます。\nご家族の次の記念日や季節撮影など、優先的にご相談いただけます。`;
  }
  return `${name}\nこんにちは。水野写真の水野です。\n次回の撮影やご家族の記念日について、気になることがあればLINEでお気軽にご相談ください。`;
}

async function getSegmentLineDrafts(request, env, segment) {
  const res = await getSegmentCustomers(request, env, segment);
  const data = await res.json();
  if (!data.ok) return json(data, res.status);
  const drafts = (data.customers || []).map((c) => ({
    customer_id: c.customer_id,
    customer_name: c.name || c.line_display_name || c.customer_id,
    line_display_name: c.line_display_name || "",
    segment,
    message: draftMessageForSegment(c, segment)
  }));
  return json({ ok: true, segment, count: drafts.length, drafts });
}

async function getSalesRepeatAnalytics(request, env) {
  const current = await getCurrentUser(request, env);
  if (!current) return json({ ok: false, message: "login required" }, 401);
  await ensureSuiteSchema(env);

  const totals = await env.DB.prepare(`
    SELECT
      COUNT(*) AS customers,
      SUM(CAST(COALESCE(total_revenue, 0) AS INTEGER)) AS total_revenue,
      AVG(CAST(COALESCE(total_revenue, 0) AS INTEGER)) AS avg_revenue,
      SUM(CASE WHEN CAST(COALESCE(repeat_count, 0) AS INTEGER) >= 2 THEN 1 ELSE 0 END) AS repeat_customers,
      SUM(CASE WHEN CAST(COALESCE(dormant_days, 0) AS INTEGER) >= 180 THEN 1 ELSE 0 END) AS dormant_customers,
      SUM(CASE WHEN COALESCE(line_user_id, '') <> '' THEN 1 ELSE 0 END) AS line_linked_customers
    FROM customers
    WHERE deleted_at IS NULL OR deleted_at=''
  `).first();

  const genreRows = await env.DB.prepare(`
    SELECT COALESCE(primary_genre, '未分類') AS genre,
           COUNT(*) AS customers,
           SUM(CAST(COALESCE(total_revenue, 0) AS INTEGER)) AS total_revenue,
           AVG(CAST(COALESCE(total_revenue, 0) AS INTEGER)) AS avg_revenue
    FROM customers
    WHERE deleted_at IS NULL OR deleted_at=''
    GROUP BY COALESCE(primary_genre, '未分類')
    ORDER BY total_revenue DESC
    LIMIT 20
  `).all();

  const repeatRate = totals?.customers ? Math.round((Number(totals.repeat_customers || 0) / Number(totals.customers || 1)) * 1000) / 10 : 0;
  const expectedDormantRevenueAt10Percent = Math.round(Number(totals?.dormant_customers || 0) * Number(totals?.avg_revenue || 0) * 0.1);

  return json({
    ok: true,
    totals: totals || {},
    repeat_rate_percent: repeatRate,
    expected_dormant_revenue_at_10_percent: expectedDormantRevenueAt10Percent,
    by_genre: genreRows.results || []
  });
}

async function getReservationSummary(request, env, customerId) {
  const current = await getCurrentUser(request, env);
  if (!current) return json({ ok: false, message: "login required" }, 401);
  await ensureSuiteSchema(env);

  const rows = await env.DB.prepare(`
    SELECT *
    FROM customer_reservations
    WHERE customer_id=?
      AND (deleted_at IS NULL OR deleted_at='')
    ORDER BY COALESCE(shoot_date, reservation_date, created_at) DESC
    LIMIT 50
  `).bind(customerId).all();

  return json({
    ok: true,
    customer_id: customerId,
    integration: "local_customer_reservations",
    message: "予約管理アプリとの直接API連携前に、CRM内の予約履歴を表示します。",
    reservations: rows.results || []
  });
}

function injectSuiteUi(html) {
  if (!html || !html.includes("</head>") || !html.includes("</body>")) return html;

  const style = `<style id="crm-suite-style">
.crm-suite-btn{display:inline-flex;align-items:center;gap:6px;margin:4px 4px 4px 0;border:1px solid #cbd5e1;background:#fff;color:#0f172a;border-radius:999px;padding:8px 11px;font-size:.78rem;font-weight:950;cursor:pointer;white-space:nowrap;text-decoration:none}
.crm-suite-btn:hover{background:#f8fafc}.crm-suite-btn.primary{border-color:#93c5fd;background:#eff6ff;color:#1d4ed8}.crm-suite-btn.danger{border-color:#fecaca;background:#fff1f2;color:#be123c}.crm-suite-panel{border:1px solid #dbeafe;background:#f8fbff;border-radius:16px;padding:12px;margin:12px 0;box-shadow:0 8px 24px rgba(15,23,42,.06)}
.crm-suite-title{font-weight:950;color:#0f172a;margin-bottom:8px}.crm-suite-muted{color:#64748b;font-size:.82rem}.crm-suite-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:8px}.crm-suite-card{background:#fff;border:1px solid #e2e8f0;border-radius:14px;padding:10px}.crm-suite-card b{display:block;font-size:1.15rem}.crm-suite-input,.crm-suite-textarea{width:100%;box-sizing:border-box;border:1px solid #cbd5e1;border-radius:12px;padding:9px 10px;background:#fff;font-size:.9rem}.crm-suite-textarea{min-height:72px;resize:vertical}.crm-suite-tag{display:inline-flex;align-items:center;gap:4px;background:#ecfeff;color:#155e75;border:1px solid #a5f3fc;border-radius:999px;padding:5px 8px;font-size:.78rem;font-weight:900;margin:3px}.crm-suite-tabs{display:flex;gap:6px;flex-wrap:wrap;margin:8px 0}.crm-suite-tab{border:1px solid #cbd5e1;background:#fff;border-radius:999px;padding:7px 10px;font-size:.78rem;font-weight:900;cursor:pointer}.crm-suite-tab.active{background:#0f172a;color:#fff;border-color:#0f172a}.crm-suite-tabpane{display:none}.crm-suite-tabpane.active{display:block}.crm-suite-log{border-top:1px solid #e2e8f0;padding:8px 0}.crm-suite-log:first-child{border-top:0}
@media(max-width:820px){
  body.crm-card-mode .tablewrap table,body.crm-card-mode .tablewrap thead,body.crm-card-mode .tablewrap tbody,body.crm-card-mode .tablewrap tr,body.crm-card-mode .tablewrap th,body.crm-card-mode .tablewrap td{display:block!important;width:100%!important}
  body.crm-card-mode .tablewrap thead{display:none!important}
  body.crm-card-mode .tablewrap tr{border:1px solid #e2e8f0;border-radius:16px;margin:10px 0;padding:10px;background:#fff;box-shadow:0 8px 22px rgba(15,23,42,.06)}
  body.crm-card-mode .tablewrap td{border:0!important;padding:6px 4px!important}
  body.crm-card-mode .tablewrap td::before{content:attr(data-label);display:block;color:#64748b;font-size:.72rem;font-weight:900;margin-bottom:2px}
}
</style>`;

  const script = `<script id="crm-suite-script">
(function(){
  if(window.__crmSuiteInstalled)return;
  window.__crmSuiteInstalled=true;

  function qs(sel,root){return (root||document).querySelector(sel)}
  function qsa(sel,root){return Array.prototype.slice.call((root||document).querySelectorAll(sel))}
  function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin',headers:{'content-type':'application/json'}},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
  function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]})}
  function toolbar(){return qs('.card .toolbar')||qs('.toolbar')||qs('main')||document.body}
  function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:99999;background:#0f172a;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.18)';document.body.appendChild(d);setTimeout(function(){d.remove()},2200)}

  function addToolbarButtons(){
    var tb=toolbar(); if(!tb||qs('#crmSuiteBulkSentBtn'))return;
    var bulk=document.createElement('button'); bulk.id='crmSuiteBulkSentBtn'; bulk.className='crm-suite-btn danger'; bulk.textContent='CSV分を一括送信済み';
    bulk.onclick=function(){
      if(!confirm('未送信のLINE文面をすべて送信済みにします。LINEへ送信済みのときだけ実行してください。'))return;
      api('/api/line-message-logs/mark-sent-bulk',{method:'POST',body:JSON.stringify({all_pending:true})}).then(function(d){toast(d.ok?'送信済みにしました：'+(d.updated||0)+'件':'失敗しました')});
    };
    var today=document.createElement('button'); today.id='crmSuiteTodayBtn'; today.className='crm-suite-btn primary'; today.textContent='今日対応';
    today.onclick=function(){openSuitePanel('today')};
    var analytics=document.createElement('button'); analytics.id='crmSuiteAnalyticsBtn'; analytics.className='crm-suite-btn'; analytics.textContent='売上分析';
    analytics.onclick=function(){openSuitePanel('analytics')};
    var mobile=document.createElement('button'); mobile.id='crmSuiteMobileBtn'; mobile.className='crm-suite-btn'; mobile.textContent='スマホカードON';
    mobile.onclick=function(){document.body.classList.toggle('crm-card-mode'); mobile.textContent=document.body.classList.contains('crm-card-mode')?'スマホカードOFF':'スマホカードON'; labelTableCells()};
    tb.appendChild(bulk); tb.appendChild(today); tb.appendChild(analytics); tb.appendChild(mobile);
  }

  function labelTableCells(){
    qsa('.tablewrap table').forEach(function(table){
      var heads=qsa('thead th',table).map(function(th){return th.textContent.trim()});
      qsa('tbody tr',table).forEach(function(tr){
        qsa('td',tr).forEach(function(td,i){if(!td.getAttribute('data-label'))td.setAttribute('data-label',heads[i]||'項目')})
      })
    })
  }

  function ensureTopPanel(){
    if(qs('#crmSuiteTopPanel'))return qs('#crmSuiteTopPanel');
    var host=qs('.card')||qs('main')||document.body;
    var panel=document.createElement('div');
    panel.id='crmSuiteTopPanel';
    panel.className='crm-suite-panel';
    panel.style.display='none';
    host.insertBefore(panel,host.firstChild);
    return panel;
  }

  function openSuitePanel(mode){
    var p=ensureTopPanel(); p.style.display='block';
    p.innerHTML='<div class="crm-suite-title">読み込み中...</div>';
    if(mode==='today'){
      api('/api/follow-tasks/today').then(function(d){
        var tasks=d.tasks||[];
        p.innerHTML='<div class="crm-suite-title">今日対応する顧客</div><div class="crm-suite-muted">期限未設定・今日以前の未完了タスクを表示します。</div>'+
          (tasks.length?tasks.map(function(t){return '<div class="crm-suite-log"><b>'+esc(t.title)+'</b><div class="crm-suite-muted">'+esc(t.customer_name||t.customer_id)+' / '+esc(t.due_date||'期限なし')+' / '+esc(t.priority||'')+'</div><button class="crm-suite-btn primary" data-complete-task="'+esc(t.id)+'">完了</button></div>'}).join(''):'<div class="crm-suite-muted">今日対応のタスクはありません。</div>');
      });
    }
    if(mode==='analytics'){
      api('/api/analytics/sales-repeat').then(function(d){
        var t=d.totals||{};
        p.innerHTML='<div class="crm-suite-title">売上・リピート分析</div><div class="crm-suite-grid">'+
          '<div class="crm-suite-card"><span>顧客数</span><b>'+esc(t.customers||0)+'</b></div>'+
          '<div class="crm-suite-card"><span>累計売上</span><b>¥'+Number(t.total_revenue||0).toLocaleString()+'</b></div>'+
          '<div class="crm-suite-card"><span>リピート率</span><b>'+esc(d.repeat_rate_percent||0)+'%</b></div>'+
          '<div class="crm-suite-card"><span>休眠10%回収見込</span><b>¥'+Number(d.expected_dormant_revenue_at_10_percent||0).toLocaleString()+'</b></div>'+
          '</div><div class="crm-suite-muted">ジャンル別分析もAPIで取得できます。</div>';
      });
    }
  }

  document.addEventListener('click',function(e){
    var t=e.target.closest('[data-detail]');
    if(t){window.__crmSuiteCustomerId=t.getAttribute('data-detail'); setTimeout(enhanceDetail,600); setTimeout(enhanceDetail,1400)}
    var c=e.target.closest('[data-complete-task]');
    if(c){api('/api/follow-tasks/'+c.getAttribute('data-complete-task')+'/complete',{method:'POST',body:'{}'}).then(function(d){toast(d.ok?'完了にしました':'失敗しました');openSuitePanel('today')})}
  });

  function findDetailHost(){
    var candidates=qsa('.modal,.drawer,[role="dialog"],.detail,.detail-modal,.customer-detail');
    for(var i=0;i<candidates.length;i++){
      var el=candidates[i], st=getComputedStyle(el);
      if(st.display!=='none'&&st.visibility!=='hidden'&&el.offsetParent!==null)return el;
    }
    return null;
  }

  function enhanceDetail(){
    var customerId=window.__crmSuiteCustomerId;
    if(!customerId)return;
    var host=findDetailHost(); if(!host||qs('#crmSuiteDetailPanel',host))return;
    var panel=document.createElement('div'); panel.id='crmSuiteDetailPanel'; panel.className='crm-suite-panel';
    panel.innerHTML='<div class="crm-suite-title">顧客フォロー管理</div><div class="crm-suite-tabs">'+
      '<button class="crm-suite-tab active" data-tab="logs">LINE履歴</button><button class="crm-suite-tab" data-tab="memo">メモ</button><button class="crm-suite-tab" data-tab="tags">タグ</button><button class="crm-suite-tab" data-tab="task">予定</button><button class="crm-suite-tab" data-tab="reserve">予約履歴</button>'+
      '</div><div class="crm-suite-tabpane active" data-pane="logs"></div><div class="crm-suite-tabpane" data-pane="memo"></div><div class="crm-suite-tabpane" data-pane="tags"></div><div class="crm-suite-tabpane" data-pane="task"></div><div class="crm-suite-tabpane" data-pane="reserve"></div>';
    host.appendChild(panel);
    qsa('.crm-suite-tab',panel).forEach(function(btn){btn.onclick=function(){qsa('.crm-suite-tab',panel).forEach(function(b){b.classList.remove('active')});qsa('.crm-suite-tabpane',panel).forEach(function(p){p.classList.remove('active')});btn.classList.add('active');qs('[data-pane="'+btn.dataset.tab+'"]',panel).classList.add('active')}});
    loadDetailPanels(customerId,panel);
  }

  function loadDetailPanels(customerId,panel){
    api('/api/customers/'+encodeURIComponent(customerId)+'/line-message-logs').then(function(d){
      var pane=qs('[data-pane="logs"]',panel), logs=d.logs||[];
      pane.innerHTML=logs.length?logs.map(function(l){return '<div class="crm-suite-log"><b>'+esc(l.action_label||'LINE文面')+'</b><div class="crm-suite-muted">'+esc(l.status||'')+' / '+esc(l.saved_at||l.created_at||'')+'</div><div style="white-space:pre-wrap">'+esc(l.message_text||'')+'</div></div>'}).join(''):'<div class="crm-suite-muted">LINE文面履歴はありません。</div>';
    });
    api('/api/customers/'+encodeURIComponent(customerId)+'/memos').then(function(d){
      var pane=qs('[data-pane="memo"]',panel), memos=d.memos||[];
      pane.innerHTML='<textarea class="crm-suite-textarea" id="crmSuiteMemoInput" placeholder="顧客メモを入力"></textarea><button class="crm-suite-btn primary" id="crmSuiteMemoSave">メモ保存</button><div id="crmSuiteMemoList">'+(memos.map(function(m){return '<div class="crm-suite-log">'+esc(m.memo_text)+'<div class="crm-suite-muted">'+esc(m.created_by||'')+' / '+esc(m.created_at||'')+'</div></div>'}).join('')||'<div class="crm-suite-muted">メモはありません。</div>')+'</div>';
      qs('#crmSuiteMemoSave',pane).onclick=function(){var v=qs('#crmSuiteMemoInput',pane).value; api('/api/customers/'+encodeURIComponent(customerId)+'/memos',{method:'POST',body:JSON.stringify({memo:v})}).then(function(){toast('メモを保存しました');loadDetailPanels(customerId,panel)})};
    });
    api('/api/customers/'+encodeURIComponent(customerId)+'/tags').then(function(d){
      var pane=qs('[data-pane="tags"]',panel), tags=d.tags||[];
      pane.innerHTML='<input class="crm-suite-input" id="crmSuiteTagInput" placeholder="タグ 例：七五三候補"><button class="crm-suite-btn primary" id="crmSuiteTagAdd">タグ追加</button><div>'+(tags.map(function(t){return '<span class="crm-suite-tag">'+esc(t.tag)+'</span>'}).join('')||'<div class="crm-suite-muted">タグはありません。</div>')+'</div>';
      qs('#crmSuiteTagAdd',pane).onclick=function(){var v=qs('#crmSuiteTagInput',pane).value; api('/api/customers/'+encodeURIComponent(customerId)+'/tags',{method:'POST',body:JSON.stringify({tag:v})}).then(function(){toast('タグを追加しました');loadDetailPanels(customerId,panel)})};
    });
    api('/api/customers/'+encodeURIComponent(customerId)+'/follow-tasks').then(function(d){
      var pane=qs('[data-pane="task"]',panel), tasks=d.tasks||[];
      pane.innerHTML='<input class="crm-suite-input" id="crmSuiteTaskTitle" placeholder="予定タイトル 例：七五三案内"><input class="crm-suite-input" id="crmSuiteTaskDue" type="date"><button class="crm-suite-btn primary" id="crmSuiteTaskAdd">予定追加</button>'+(tasks.map(function(t){return '<div class="crm-suite-log"><b>'+esc(t.title)+'</b><div class="crm-suite-muted">'+esc(t.due_date||'期限なし')+' / '+esc(t.status||'')+'</div></div>'}).join('')||'<div class="crm-suite-muted">フォロー予定はありません。</div>');
      qs('#crmSuiteTaskAdd',pane).onclick=function(){api('/api/customers/'+encodeURIComponent(customerId)+'/follow-tasks',{method:'POST',body:JSON.stringify({title:qs('#crmSuiteTaskTitle',pane).value,due_date:qs('#crmSuiteTaskDue',pane).value})}).then(function(){toast('予定を追加しました');loadDetailPanels(customerId,panel)})};
    });
    api('/api/customers/'+encodeURIComponent(customerId)+'/reservation-summary').then(function(d){
      var pane=qs('[data-pane="reserve"]',panel), rows=d.reservations||[];
      pane.innerHTML=rows.length?rows.map(function(r){return '<div class="crm-suite-log"><b>'+esc(r.shoot_date||r.reservation_date||r.created_at||'予約')+'</b><pre style="white-space:pre-wrap;font-size:.78rem">'+esc(JSON.stringify(r,null,2))+'</pre></div>'}).join(''):'<div class="crm-suite-muted">CRM内の予約履歴はありません。</div>';
    });
  }

  function init(){addToolbarButtons();labelTableCells();if(innerWidth<=820)document.body.classList.add('crm-card-mode')}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else setTimeout(init,80);
  var mo=new MutationObserver(function(){clearTimeout(window.__crmSuiteTimer);window.__crmSuiteTimer=setTimeout(function(){addToolbarButtons();labelTableCells();enhanceDetail()},200)});
  mo.observe(document.documentElement,{childList:true,subtree:true});
})();
</script>`;

  return html.replace("</head>", style + "</head>").replace("</body>", script + "</body>");
}

function routeCustomerSubpath(path, sub) {
  return path.startsWith("/api/customers/") && path.endsWith(sub);
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === "/" || path === "/health" || path === "/api/health") {
      return json({ ok: true, service: "customer-crm-api", build: BUILD, hasDb: !!env.DB, secure: true, auth: "cloudflare-access-google" });
    }

    try {
      if (path === "/api/line-message-logs/mark-sent-bulk" && request.method === "POST") {
        return await bulkMarkLineSent(request, env);
      }

      if (path === "/api/line-message-logs/overview" && request.method === "GET") {
        return await getLineLogsOverview(request, env);
      }

      if (path === "/api/follow-tasks/today" && request.method === "GET") {
        return await getFollowTasks(request, env);
      }

      if (path.startsWith("/api/follow-tasks/") && path.endsWith("/complete") && request.method === "POST") {
        const taskId = parseInt(path.replace("/api/follow-tasks/", "").replace("/complete", ""), 10);
        return await completeFollowTask(request, env, taskId);
      }

      if (path.startsWith("/api/segments/") && path.endsWith("/customers") && request.method === "GET") {
        const segment = cleanCustomerIdFromPath(path, "/api/segments/", "/customers");
        return await getSegmentCustomers(request, env, segment);
      }

      if (path.startsWith("/api/segments/") && path.endsWith("/line-drafts") && request.method === "GET") {
        const segment = cleanCustomerIdFromPath(path, "/api/segments/", "/line-drafts");
        return await getSegmentLineDrafts(request, env, segment);
      }

      if (path === "/api/analytics/sales-repeat" && request.method === "GET") {
        return await getSalesRepeatAnalytics(request, env);
      }

      if (routeCustomerSubpath(path, "/memos")) {
        const customerId = cleanCustomerIdFromPath(path, "/api/customers/", "/memos");
        if (request.method === "GET") return await getCustomerMemos(request, env, customerId);
        if (request.method === "POST") return await addCustomerMemo(request, env, customerId);
      }

      if (routeCustomerSubpath(path, "/tags")) {
        const customerId = cleanCustomerIdFromPath(path, "/api/customers/", "/tags");
        if (request.method === "GET") return await getCustomerTags(request, env, customerId);
        if (request.method === "POST") return await addCustomerTag(request, env, customerId);
        if (request.method === "DELETE") return await removeCustomerTag(request, env, customerId);
      }

      if (routeCustomerSubpath(path, "/follow-tasks")) {
        const customerId = cleanCustomerIdFromPath(path, "/api/customers/", "/follow-tasks");
        if (request.method === "GET") return await getFollowTasks(request, env, customerId);
        if (request.method === "POST") return await addFollowTask(request, env, customerId);
      }

      if (routeCustomerSubpath(path, "/reservation-summary") && request.method === "GET") {
        const customerId = cleanCustomerIdFromPath(path, "/api/customers/", "/reservation-summary");
        return await getReservationSummary(request, env, customerId);
      }
    } catch (err) {
      return json({ ok: false, message: "crm suite error", error: String(err && err.message ? err.message : err) }, 500);
    }

    const res = await app.fetch(request, env, ctx);
    const contentType = res.headers.get("content-type") || "";

    if (contentType.includes("text/html")) {
      const body = injectSuiteUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }

    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
