// ======================================================
// CUSTOMER CRM API / LINE PENDING BADGES WRAPPER
// build: customer-crm-api-line-pending-badges-20260613-01
// ======================================================

import app from "./production-index-line-overview.js";

const BUILD = "customer-crm-api-line-pending-badges-20260613-01";
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

async function pendingByCustomer(request, env) {
  const current = await getCurrentUser(request, env);
  if (!current) return json({ ok: false, message: "Google login through Cloudflare Access is required" }, 401);

  await ensureLineDraftLogSchema(env);

  const rows = await env.DB.prepare(`
    SELECT
      customer_id,
      COALESCE(MAX(customer_name), customer_id) AS customer_name,
      COUNT(*) AS pending_count,
      MAX(COALESCE(copied_at, created_at)) AS latest_at,
      GROUP_CONCAT(DISTINCT COALESCE(action_label, 'LINE文面')) AS labels
    FROM customer_line_draft_logs
    WHERE COALESCE(status, 'copied') <> 'sent'
      AND customer_id IS NOT NULL
      AND customer_id <> ''
    GROUP BY customer_id
    ORDER BY latest_at DESC
    LIMIT 500
  `).all();

  const items = rows.results || [];
  const map = {};
  for (const row of items) {
    map[row.customer_id] = {
      customer_id: row.customer_id,
      customer_name: row.customer_name || row.customer_id,
      pending_count: Number(row.pending_count || 0),
      latest_at: row.latest_at || "",
      labels: text(row.labels)
    };
  }

  return json({ ok: true, count: items.length, items, map });
}

function injectPendingBadgesUi(html) {
  if (!html || !html.includes("</head>") || !html.includes("</body>")) return html;

  const style = `<style id="crm-line-pending-badges-style">
.crm-line-pending-badge{display:inline-flex;align-items:center;gap:5px;width:max-content;max-width:100%;margin-top:6px;border-radius:999px;background:#fff7ed;border:1px solid #fed7aa;color:#9a3412;padding:4px 8px;font-size:.72rem;font-weight:950;line-height:1.25;box-shadow:0 4px 10px rgba(251,146,60,.12)}.crm-line-pending-badge b{font-size:.78rem}.crm-line-pending-badge small{font-size:.68rem;font-weight:800;opacity:.86;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:180px}.crm-line-pending-badge.is-many{background:#fef2f2;border-color:#fecaca;color:#991b1b}.crm-line-pending-badge.is-sent{display:none}.crm-line-pending-toolbar{display:inline-flex;align-items:center;gap:6px;margin-left:6px;border-radius:999px;background:#f8fafc;border:1px solid #e5e7eb;color:#475569;padding:4px 8px;font-size:.7rem;font-weight:900}.crm-line-pending-toast{position:fixed;left:50%;bottom:132px;transform:translateX(-50%);z-index:999999;background:#111827;color:#fff;border-radius:999px;padding:10px 14px;font-size:13px;font-weight:900;box-shadow:0 14px 36px rgba(15,23,42,.25)}@media(max-width:820px){.crm-line-pending-badge{font-size:.7rem;padding:4px 7px}.crm-line-pending-badge small{max-width:140px}.crm-line-pending-toolbar{display:flex;margin:6px 0 0;width:max-content}}
</style>`;

  const script = `<script id="crm-line-pending-badges-script">
(function(){
  if(window.__crmLinePendingBadgesInstalled)return;
  window.__crmLinePendingBadgesInstalled=true;
  var pendingMap={};
  var loading=false;
  function esc(v){return String(v==null?'':v).replace(/[&<>\"]/g,function(m){return {'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[m]})}
  function toast(msg){var old=document.querySelector('.crm-line-pending-toast');if(old)old.remove();var t=document.createElement('div');t.className='crm-line-pending-toast';t.textContent=msg||'更新しました';document.body.appendChild(t);setTimeout(function(){t.remove()},1500)}
  function customerRows(){return Array.prototype.slice.call(document.querySelectorAll('[data-detail]')).map(function(btn){return {button:btn,id:btn.getAttribute('data-detail'),row:btn.closest('tr')}}).filter(function(x){return x.id&&x.row})}
  function ensureRefreshMini(){var title=document.querySelector('.card .toolbar')||document.querySelector('.toolbar');if(!title||document.getElementById('crmLinePendingMini'))return;var b=document.createElement('button');b.id='crmLinePendingMini';b.className='crm-line-pending-toolbar';b.type='button';b.textContent='LINE未送信 更新';b.onclick=function(){loadPending(true)};title.appendChild(b)}
  function applyBadges(){
    ensureRefreshMini();
    customerRows().forEach(function(x){
      var old=x.row.querySelector('.crm-line-pending-badge');
      var info=pendingMap[x.id];
      if(!info||!Number(info.pending_count||0)){if(old)old.remove();return;}
      var badge=old||document.createElement('div');
      badge.className='crm-line-pending-badge'+(Number(info.pending_count||0)>=2?' is-many':'');
      badge.title='未送信LINE文面: '+Number(info.pending_count||0)+'件 / '+(info.labels||'');
      badge.innerHTML='<b>LINE未送信 '+Number(info.pending_count||0)+'件</b>'+(info.labels?'<small>'+esc(String(info.labels).split(',').slice(0,2).join(' / '))+'</small>':'');
      if(!old){
        var first=x.row.querySelector('td');
        var name=first&&first.querySelector('.customer-main-name');
        if(name&&name.parentNode)name.parentNode.appendChild(badge);else if(first)first.appendChild(badge);
      }
    });
  }
  async function loadPending(showToast){
    if(loading)return;
    loading=true;
    try{
      var r=await fetch('/api/line-message-logs/pending-by-customer?_='+Date.now());
      var j=await r.json();
      if(!r.ok||j.ok===false)throw new Error(j.message||'LINE未送信情報の取得に失敗しました');
      pendingMap=j.map||{};
      applyBadges();
      if(showToast)toast('LINE未送信バッジを更新しました');
    }catch(e){console.warn(e)}finally{loading=false;}
  }
  function init(){if(!/\/admin\/?$/.test(location.pathname))return;loadPending(false);setTimeout(applyBadges,300);setTimeout(applyBadges,1000)}
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else setTimeout(init,80);
  var mo=new MutationObserver(function(){if(!/\/admin\/?$/.test(location.pathname))return;clearTimeout(window.__crmLinePendingBadgeTimer);window.__crmLinePendingBadgeTimer=setTimeout(function(){applyBadges();if(!Object.keys(pendingMap).length)loadPending(false)},120)});
  mo.observe(document.documentElement,{childList:true,subtree:true});
  window.crmReloadLinePendingBadges=function(){loadPending(true)};
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

    if (url.pathname === "/api/line-message-logs/pending-by-customer" && request.method === "GET") {
      return await pendingByCustomer(request, env);
    }

    const res = await app.fetch(request, env, ctx);
    const contentType = res.headers.get("content-type") || "";

    if (contentType.includes("text/html")) {
      const body = injectPendingBadgesUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }

    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
