// ======================================================
// CUSTOMER CRM API / RESERVATION STATUS UI WRAPPER
// build: customer-crm-api-reservation-status-ui-20260613-01
// Shows reservation-app created IDs clearly in CRM detail reservation tab.
// ======================================================

import app from "./production-index-crm-reservation-created-sync.js";

const BUILD = "customer-crm-api-reservation-status-ui-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const READ_ROLES = ["root_admin", "admin", "staff", "viewer"];

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
    request.headers.get("x-user-email") ||
    ""
  );
}

async function addColumn(db, table, definition) {
  try {
    await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run();
  } catch (_) {}
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

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_reservation_drafts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT NOT NULL,
    customer_name TEXT,
    genre TEXT,
    shoot_date TEXT,
    start_time TEXT,
    place TEXT,
    plan_label TEXT,
    total_amount REAL DEFAULT 0,
    status TEXT DEFAULT 'draft',
    memo TEXT,
    draft_json TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    converted_at TEXT,
    converted_by TEXT,
    reservation_intake_id TEXT,
    sent_to_reservation_at TEXT,
    sent_to_reservation_by TEXT,
    sent_to_reservation_response TEXT,
    reservation_app_reservation_id TEXT,
    reservation_app_intake_id TEXT,
    reservation_app_created_at TEXT,
    reservation_app_created_by TEXT,
    reservation_app_response TEXT
  )`).run();

  for (const col of [
    "reservation_intake_id TEXT",
    "sent_to_reservation_at TEXT",
    "sent_to_reservation_by TEXT",
    "sent_to_reservation_response TEXT",
    "reservation_app_reservation_id TEXT",
    "reservation_app_intake_id TEXT",
    "reservation_app_created_at TEXT",
    "reservation_app_created_by TEXT",
    "reservation_app_response TEXT"
  ]) await addColumn(env.DB, "crm_reservation_drafts", col);

  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_status_ui
    ON crm_reservation_drafts(customer_id, status, updated_at)`).run();
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

function customerIdFromPath(path) {
  const m = path.match(/^\/api\/customers\/([^/]+)\/reservation-created-summary$/);
  if (!m) return "";
  return decodeURIComponent(m[1]);
}

async function reservationCreatedSummary(request, env, customerId) {
  const auth = await requireReader(request, env);
  if (!auth.ok) return auth.response;

  const rows = await env.DB.prepare(`
    SELECT id, customer_id, customer_name, genre, shoot_date, start_time, place,
           plan_label, total_amount, status, memo, created_by, created_at,
           updated_at, converted_at, converted_by, reservation_intake_id,
           sent_to_reservation_at, sent_to_reservation_by,
           reservation_app_reservation_id, reservation_app_intake_id,
           reservation_app_created_at, reservation_app_created_by
    FROM crm_reservation_drafts
    WHERE customer_id=?
    ORDER BY COALESCE(reservation_app_created_at, sent_to_reservation_at, updated_at, created_at) DESC, id DESC
    LIMIT 50
  `).bind(customerId).all();

  const drafts = rows.results || [];
  const created = drafts.filter((x) => text(x.reservation_app_reservation_id) || text(x.status) === "created");
  const sent = drafts.filter((x) => text(x.sent_to_reservation_at) || text(x.status) === "sent_to_reservation");
  const pending = drafts.filter((x) => !text(x.reservation_app_reservation_id) && text(x.status) !== "created");
  const latestCreated = created[0] || null;

  return json({
    ok: true,
    customer_id: customerId,
    summary: {
      total_drafts: drafts.length,
      created_count: created.length,
      sent_count: sent.length,
      pending_count: pending.length,
      latest_reservation_id: latestCreated ? text(latestCreated.reservation_app_reservation_id) : "",
      latest_created_at: latestCreated ? text(latestCreated.reservation_app_created_at || latestCreated.converted_at) : ""
    },
    created,
    sent,
    pending,
    drafts
  });
}

function injectReservationStatusUi(html) {
  if (!html || !html.includes("</head>") || !html.includes("</body>")) return html;

  const style = `<style id="crm-reservation-status-ui-style">
.crm-reservation-created-card{border:1px solid #bfdbfe;background:linear-gradient(180deg,#eff6ff,#ffffff);border-radius:16px;padding:12px;margin:10px 0;box-shadow:0 8px 22px rgba(37,99,235,.08)}.crm-reservation-created-head{display:flex;justify-content:space-between;gap:8px;align-items:center;flex-wrap:wrap}.crm-reservation-created-title{font-weight:950;color:#1e3a8a}.crm-reservation-created-badge{display:inline-flex;align-items:center;border-radius:999px;background:#dbeafe;color:#1d4ed8;font-size:.76rem;font-weight:950;padding:5px 9px}.crm-reservation-created-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px;margin:9px 0}.crm-reservation-created-mini{background:#fff;border:1px solid #dbeafe;border-radius:13px;padding:9px}.crm-reservation-created-mini span{display:block;color:#64748b;font-size:.75rem;font-weight:800}.crm-reservation-created-mini b{display:block;color:#0f172a;font-size:1rem}.crm-reservation-created-list{border-top:1px solid #dbeafe;margin-top:8px;padding-top:8px}.crm-reservation-created-item{border:1px solid #e0ecff;background:#fff;border-radius:13px;padding:9px;margin-top:7px}.crm-reservation-created-actions{display:flex;gap:6px;flex-wrap:wrap;margin-top:7px}.crm-reservation-created-btn{border:1px solid #bfdbfe;background:#eff6ff;color:#1d4ed8;border-radius:999px;padding:7px 10px;font-size:.75rem;font-weight:950;cursor:pointer}.crm-reservation-created-muted{color:#64748b;font-size:.8rem}.crm-reservation-created-warn{border-color:#fde68a;background:#fffbeb}.crm-reservation-created-warn .crm-reservation-created-badge{background:#fef3c7;color:#92400e}
</style>`;

  const script = `<script id="crm-reservation-status-ui-script">
(function(){
  if(window.__crmReservationStatusUiInstalled)return;
  window.__crmReservationStatusUiInstalled=true;
  function qs(sel,root){return (root||document).querySelector(sel)}
  function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]})}
  function api(url){return fetch(url,{credentials:'same-origin'}).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
  function copy(v){if(navigator.clipboard&&navigator.clipboard.writeText){navigator.clipboard.writeText(v||'').then(function(){toast('コピーしました')})}else{var ta=document.createElement('textarea');ta.value=v||'';document.body.appendChild(ta);ta.select();document.execCommand('copy');ta.remove();toast('コピーしました')}}
  function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:999999;background:#0f172a;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.18)';document.body.appendChild(d);setTimeout(function(){d.remove()},2200)}
  function customerId(){return window.__crmSuiteCustomerId||''}
  function reservePane(){var p=qs('#crmSuiteDetailPanel');return p?qs('[data-pane="reserve"]',p):null}
  function render(){
    var id=customerId(), pane=reservePane();
    if(!id||!pane||!pane.innerHTML)return;
    var existing=qs('#crmReservationCreatedSummaryCard',pane);
    api('/api/customers/'+encodeURIComponent(id)+'/reservation-created-summary').then(function(d){
      if(!d.ok)return;
      var s=d.summary||{}, created=d.created||[], pending=d.pending||[];
      var html='';
      var cls=created.length?'crm-reservation-created-card':'crm-reservation-created-card crm-reservation-created-warn';
      html+='<div id="crmReservationCreatedSummaryCard" class="'+cls+'">';
      html+='<div class="crm-reservation-created-head"><div class="crm-reservation-created-title">予約管理の本予約状況</div><span class="crm-reservation-created-badge">'+(created.length?'本予約作成済み '+created.length+'件':'本予約未作成')+'</span></div>';
      html+='<div class="crm-reservation-created-grid"><div class="crm-reservation-created-mini"><span>最新予約ID</span><b>'+esc(s.latest_reservation_id||'-')+'</b></div><div class="crm-reservation-created-mini"><span>作成日</span><b>'+esc(s.latest_created_at||'-')+'</b></div><div class="crm-reservation-created-mini"><span>送信済み下書き</span><b>'+esc(s.sent_count||0)+'件</b></div><div class="crm-reservation-created-mini"><span>未作成本予約</span><b>'+esc(s.pending_count||0)+'件</b></div></div>';
      if(created.length){
        html+='<div class="crm-reservation-created-list"><div class="crm-reservation-created-muted">予約管理側で作成された本予約</div>'+created.map(function(x){var rid=x.reservation_app_reservation_id||'';return '<div class="crm-reservation-created-item"><b>'+esc(x.genre||x.plan_label||'予約')+'</b><div class="crm-reservation-created-muted">予約ID：'+esc(rid||'-')+' / 候補ID：'+esc(x.reservation_app_intake_id||x.reservation_intake_id||'-')+'</div><div class="crm-reservation-created-muted">'+esc(x.shoot_date||'-')+' '+esc(x.start_time||'')+' / '+esc(x.place||'-')+' / ¥'+Number(x.total_amount||0).toLocaleString()+'</div><div class="crm-reservation-created-actions"><button class="crm-reservation-created-btn" data-copy-reservation-id="'+esc(rid)+'">予約IDコピー</button></div></div>'}).join('')+'</div>';
      } else if(pending.length){
        html+='<div class="crm-reservation-created-muted">予約管理へ送信済み・下書き中の候補があります。予約管理側で本予約を作成すると、ここに予約IDが表示されます。</div>';
      } else {
        html+='<div class="crm-reservation-created-muted">まだ予約下書きがありません。下の予約下書きフォームから作成できます。</div>';
      }
      html+='</div>';
      if(existing){existing.outerHTML=html}else{pane.insertAdjacentHTML('afterbegin',html)}
    })
  }
  document.addEventListener('click',function(e){
    var b=e.target.closest('[data-copy-reservation-id]');
    if(b){copy(b.getAttribute('data-copy-reservation-id')||'');return}
    if(e.target.closest('[data-tab="reserve"]'))setTimeout(render,500)
  });
  var mo=new MutationObserver(function(){clearTimeout(window.__crmReservationStatusTimer);window.__crmReservationStatusTimer=setTimeout(function(){var p=reservePane();if(p&&p.classList.contains('active'))render()},900)});
  mo.observe(document.documentElement,{childList:true,subtree:true});
  setInterval(function(){var p=reservePane();if(p&&p.classList.contains('active'))render()},4000);
})();
</script>`;

  return html.replace("</head>", style + "</head>").replace("</body>", script + "</body>");
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    if (path === "/" || path === "/health" || path === "/api/health") {
      return json({ ok: true, service: "customer-crm-api", build: BUILD, hasDb: !!env.DB, secure: true, auth: "cloudflare-access-google" });
    }

    try {
      const customerId = customerIdFromPath(path);
      if (customerId && request.method === "GET") {
        return await reservationCreatedSummary(request, env, customerId);
      }
    } catch (e) {
      return json({ ok: false, build: BUILD, message: e && e.message ? e.message : String(e) }, 500);
    }

    const res = await app.fetch(request, env, ctx);
    const contentType = res.headers.get("content-type") || "";
    if (request.method === "GET" && contentType.includes("text/html")) {
      const html = await res.text();
      return new Response(injectReservationStatusUi(html), {
        status: res.status,
        statusText: res.statusText,
        headers: securityHeaders({ "content-type": "text/html; charset=utf-8" })
      });
    }
    return res;
  }
};
