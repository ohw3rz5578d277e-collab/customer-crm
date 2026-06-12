// ======================================================
// CUSTOMER CRM API / LINE PENDING CSV EXPORT WRAPPER
// build: customer-crm-api-line-pending-csv-20260613-01
// ======================================================

import app from "./production-index-line-pending-filter.js";

const BUILD = "customer-crm-api-line-pending-csv-20260613-01";
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

function csvSafe(value) {
  let v = value === undefined || value === null ? "" : String(value);
  v = v.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  if (/^[=+\-@]/.test(v)) v = "'" + v;
  return `"${v.replace(/"/g, '""')}"`;
}

function csvResponse(csv, filename) {
  const h = securityHeaders({
    "content-type": "text/csv; charset=utf-8",
    "content-disposition": `attachment; filename="${filename}"`
  });
  return new Response("\ufeff" + csv, { status: 200, headers: h });
}

async function pendingLineCsv(request, env) {
  const current = await getCurrentUser(request, env);
  if (!current) return json({ ok: false, message: "Google login through Cloudflare Access is required" }, 401);

  await ensureLineDraftLogSchema(env);

  const url = new URL(request.url);
  const limit = Math.min(Math.max(parseInt(url.searchParams.get("limit") || "1000", 10) || 1000, 1), 3000);

  const rows = await env.DB.prepare(`
    SELECT
      l.id,
      l.customer_id,
      COALESCE(c.name, l.customer_name, l.customer_id) AS customer_name,
      COALESCE(c.line_display_name, '') AS line_display_name,
      COALESCE(c.phone, '') AS phone,
      COALESCE(c.email, '') AS email,
      COALESCE(c.genre_history, '') AS genre_history,
      COALESCE(c.last_shoot_date, '') AS last_shoot_date,
      COALESCE(c.dormant_days, '') AS dormant_days,
      COALESCE(c.total_revenue, '') AS total_revenue,
      COALESCE(c.repeat_count, '') AS repeat_count,
      COALESCE(l.action_label, 'LINE文面') AS action_label,
      COALESCE(l.priority, '') AS priority,
      COALESCE(l.status, 'copied') AS status,
      COALESCE(l.copied_at, l.created_at, '') AS saved_at,
      COALESCE(l.sent_at, '') AS sent_at,
      COALESCE(l.created_by, '') AS created_by,
      COALESCE(l.message_text, '') AS message_text
    FROM customer_line_draft_logs l
    LEFT JOIN customers c ON c.customer_id = l.customer_id
    WHERE COALESCE(l.status, 'copied') <> 'sent'
      AND l.customer_id IS NOT NULL
      AND l.customer_id <> ''
    ORDER BY COALESCE(l.copied_at, l.created_at) DESC
    LIMIT ?
  `).bind(limit).all();

  const header = [
    "ログID",
    "顧客ID",
    "顧客名",
    "LINE名",
    "電話番号",
    "メール",
    "ジャンル履歴",
    "最終撮影日",
    "休眠日数",
    "累計売上",
    "撮影回数",
    "アクション",
    "優先度",
    "ステータス",
    "保存日時",
    "送信日時",
    "保存者",
    "LINE文面"
  ];

  const lines = [header.map(csvSafe).join(",")];
  for (const row of rows.results || []) {
    lines.push([
      row.id,
      row.customer_id,
      row.customer_name,
      row.line_display_name,
      row.phone,
      row.email,
      row.genre_history,
      row.last_shoot_date,
      row.dormant_days,
      row.total_revenue,
      row.repeat_count,
      row.action_label,
      row.priority,
      row.status,
      row.saved_at,
      row.sent_at,
      row.created_by,
      row.message_text
    ].map(csvSafe).join(","));
  }

  const date = new Date().toISOString().slice(0, 10).replace(/-/g, "");
  return csvResponse(lines.join("\n"), `customer-crm-line-pending-${date}.csv`);
}

function injectPendingCsvUi(html) {
  if (!html || !html.includes("</head>") || !html.includes("</body>")) return html;

  const style = `<style id="crm-line-pending-csv-style">
.crm-line-pending-csv-btn{display:inline-flex;align-items:center;gap:6px;margin-left:6px;border:1px solid #bbf7d0;background:#f0fdf4;color:#166534;border-radius:999px;padding:8px 11px;font-size:.78rem;font-weight:950;cursor:pointer;white-space:nowrap;text-decoration:none}.crm-line-pending-csv-btn:hover{background:#dcfce7;border-color:#86efac}.crm-line-pending-csv-btn small{font-size:.68rem;font-weight:900;opacity:.78}@media(max-width:820px){.crm-line-pending-csv-btn{margin:6px 0 0;width:max-content;padding:9px 12px}}
</style>`;

  const script = `<script id="crm-line-pending-csv-script">
(function(){
  if(window.__crmLinePendingCsvInstalled)return;
  window.__crmLinePendingCsvInstalled=true;
  function qs(sel,root){return (root||document).querySelector(sel)}
  function isAdminPage(){return /\/admin\/?$/.test(location.pathname)}
  function toolbar(){return qs('.card .toolbar')||qs('.toolbar')}
  function ensureCsvButton(){
    if(!isAdminPage())return;
    var tb=toolbar();
    if(!tb||qs('#crmLinePendingCsvBtn'))return;
    var a=document.createElement('a');
    a.id='crmLinePendingCsvBtn';
    a.className='crm-line-pending-csv-btn';
    a.href='/api/line-message-logs/pending.csv?limit=3000&_='+Date.now();
    a.target='_blank';
    a.rel='noopener';
    a.innerHTML='未送信CSV <small>DL</small>';
    a.onclick=function(){a.href='/api/line-message-logs/pending.csv?limit=3000&_='+Date.now()};
    tb.appendChild(a);
  }
  function init(){ensureCsvButton();setTimeout(ensureCsvButton,300);setTimeout(ensureCsvButton,1000)}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else setTimeout(init,80);
  var mo=new MutationObserver(function(){if(!isAdminPage())return;clearTimeout(window.__crmLinePendingCsvTimer);window.__crmLinePendingCsvTimer=setTimeout(ensureCsvButton,120)});
  mo.observe(document.documentElement,{childList:true,subtree:true});
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

    if (url.pathname === "/api/line-message-logs/pending.csv" && request.method === "GET") {
      return await pendingLineCsv(request, env);
    }

    const res = await app.fetch(request, env, ctx);
    const contentType = res.headers.get("content-type") || "";

    if (contentType.includes("text/html")) {
      const body = injectPendingCsvUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }

    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};