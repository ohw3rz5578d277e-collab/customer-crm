// ======================================================
// CUSTOMER CRM API / RESERVATION HISTORY SYNC WRAPPER
// build: customer-crm-api-reservation-history-sync-20260613-01
// Reflects reservation-app created reservations into CRM reservation history.
// ======================================================

import app from "./production-index-crm-reservation-status-ui.js";

const BUILD = "customer-crm-api-reservation-history-sync-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const WRITE_ROLES = ["root_admin", "admin", "staff"];
const READ_ROLES = ["root_admin", "admin", "staff", "viewer"];

function text(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

function normalizeEmail(v) {
  return text(v).toLowerCase();
}

function money(v) {
  const n = Number(String(v === undefined || v === null ? "" : v).replace(/[,円¥\s]/g, ""));
  return Number.isFinite(n) ? n : 0;
}

function nowIso() {
  return new Date().toISOString();
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
  try { return await request.json(); } catch (_) { return {}; }
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

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    name TEXT,
    genre_history TEXT,
    last_shoot_date TEXT,
    repeat_count INTEGER DEFAULT 0,
    total_revenue REAL DEFAULT 0,
    created_at TEXT,
    updated_at TEXT
  )`).run();

  for (const col of [
    "name TEXT",
    "genre_history TEXT",
    "last_shoot_date TEXT",
    "repeat_count INTEGER DEFAULT 0",
    "total_revenue REAL DEFAULT 0",
    "created_at TEXT",
    "updated_at TEXT"
  ]) await addColumn(env.DB, "customers", col);

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
    reservation_app_response TEXT,
    history_synced_at TEXT,
    history_event_key TEXT
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
    "reservation_app_response TEXT",
    "history_synced_at TEXT",
    "history_event_key TEXT"
  ]) await addColumn(env.DB, "crm_reservation_drafts", col);

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS customer_reservations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key TEXT,
    reservation_id TEXT,
    customer_id TEXT NOT NULL,
    customer_name TEXT,
    genre TEXT,
    shoot_date TEXT,
    start_time TEXT,
    end_time TEXT,
    plan_label TEXT,
    place TEXT,
    total_amount REAL DEFAULT 0,
    status TEXT,
    source TEXT,
    raw_json TEXT,
    created_at TEXT,
    updated_at TEXT
  )`).run();

  for (const col of [
    "event_key TEXT",
    "reservation_id TEXT",
    "customer_id TEXT",
    "customer_name TEXT",
    "genre TEXT",
    "shoot_date TEXT",
    "start_time TEXT",
    "end_time TEXT",
    "plan_label TEXT",
    "place TEXT",
    "total_amount REAL DEFAULT 0",
    "status TEXT",
    "source TEXT",
    "raw_json TEXT",
    "created_at TEXT",
    "updated_at TEXT",
    "deleted_at TEXT",
    "deleted_by TEXT",
    "delete_reason TEXT"
  ]) await addColumn(env.DB, "customer_reservations", col);

  await env.DB.prepare(`CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_reservations_event_key
    ON customer_reservations(event_key)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_customer_reservations_customer_date_sync
    ON customer_reservations(customer_id, shoot_date)`).run();

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS customer_timeline (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key TEXT,
    customer_id TEXT NOT NULL,
    event_type TEXT,
    event_title TEXT,
    event_date TEXT,
    amount REAL DEFAULT 0,
    detail_json TEXT,
    created_at TEXT
  )`).run();

  for (const col of [
    "event_key TEXT",
    "customer_id TEXT",
    "event_type TEXT",
    "event_title TEXT",
    "event_date TEXT",
    "amount REAL DEFAULT 0",
    "detail_json TEXT",
    "created_at TEXT",
    "deleted_at TEXT",
    "deleted_by TEXT",
    "delete_reason TEXT"
  ]) await addColumn(env.DB, "customer_timeline", col);

  await env.DB.prepare(`CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_timeline_event_key
    ON customer_timeline(event_key)`).run();
}

async function requireUser(request, env, roles = WRITE_ROLES) {
  await ensureSchema(env);
  const email = getAccessEmail(request);
  if (!email) return { ok: false, response: json({ ok: false, message: "Login required" }, 401) };

  const user = await env.DB.prepare(
    `SELECT email, role, status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`
  ).bind(email).first();

  if (!user) return { ok: false, response: json({ ok: false, message: "User is not allowed" }, 403) };
  if (!roles.includes(user.role || "")) return { ok: false, response: json({ ok: false, message: "Permission denied" }, 403) };
  return { ok: true, email, user };
}

function draftIdFromMarkPath(path) {
  const m = path.match(/^\/api\/reservation-drafts\/([^/]+)\/mark-created-from-reservation$/);
  if (!m) return 0;
  const id = parseInt(decodeURIComponent(m[1]), 10);
  return Number.isFinite(id) && id > 0 ? id : 0;
}

function draftIdFromSyncPath(path) {
  const m = path.match(/^\/api\/reservation-drafts\/([^/]+)\/sync-to-customer-history$/);
  if (!m) return 0;
  const id = parseInt(decodeURIComponent(m[1]), 10);
  return Number.isFinite(id) && id > 0 ? id : 0;
}

function summaryCustomerIdFromPath(path) {
  const m = path.match(/^\/api\/customers\/([^/]+)\/reservation-history-reflect-summary$/);
  return m ? decodeURIComponent(m[1]) : "";
}

function reservationEventKey(draft, reservationId) {
  const rid = text(reservationId || draft.reservation_app_reservation_id || draft.reservation_intake_id || draft.id);
  return rid ? `reservation-app:${rid}` : `crm-reservation-draft:${draft.id}`;
}

async function refreshCustomerStats(env, customerId) {
  await env.DB.prepare(`INSERT OR IGNORE INTO customers(customer_id, name, created_at, updated_at)
    SELECT ?, COALESCE((SELECT customer_name FROM customer_reservations WHERE customer_id=? ORDER BY shoot_date DESC, id DESC LIMIT 1), ''), datetime('now'), datetime('now')`)
    .bind(customerId, customerId)
    .run();

  await env.DB.prepare(`UPDATE customers
    SET last_shoot_date=(
          SELECT MAX(shoot_date) FROM customer_reservations
          WHERE customer_id=? AND COALESCE(deleted_at,'')=''
        ),
        repeat_count=(
          SELECT COUNT(*) FROM customer_reservations
          WHERE customer_id=? AND COALESCE(deleted_at,'')=''
        ),
        total_revenue=(
          SELECT COALESCE(SUM(total_amount),0) FROM customer_reservations
          WHERE customer_id=? AND COALESCE(deleted_at,'')=''
        ),
        genre_history=(
          SELECT GROUP_CONCAT(genre, ',') FROM (
            SELECT DISTINCT genre FROM customer_reservations
            WHERE customer_id=? AND COALESCE(deleted_at,'')='' AND COALESCE(genre,'')<>''
          )
        ),
        updated_at=datetime('now')
    WHERE customer_id=?`)
    .bind(customerId, customerId, customerId, customerId, customerId)
    .run();
}

async function syncDraftToCustomerHistory(env, draft, actorEmail, sourcePayload = {}) {
  const customerId = text(draft.customer_id);
  if (!customerId) throw new Error("customer_id is missing on reservation draft");

  const reservationId = text(
    sourcePayload.reservation_id ||
    sourcePayload.converted_reservation_id ||
    draft.reservation_app_reservation_id ||
    draft.reservation_intake_id ||
    `crm-draft-${draft.id}`
  );
  const eventKey = reservationEventKey(draft, reservationId);
  const timelineKey = `timeline:${eventKey}`;
  const createdAt = text(sourcePayload.created_at || sourcePayload.converted_at || draft.reservation_app_created_at || draft.converted_at) || nowIso();
  const shootDate = text(sourcePayload.shoot_date || draft.shoot_date);
  const amount = money(sourcePayload.total_amount || sourcePayload.amount || draft.total_amount);
  const genre = text(sourcePayload.genre || draft.genre);
  const planLabel = text(sourcePayload.plan_label || sourcePayload.plan || draft.plan_label);
  const raw = {
    source: "crm_reservation_draft_created_sync",
    synced_at: nowIso(),
    synced_by: actorEmail,
    draft,
    payload: sourcePayload
  };

  await env.DB.prepare(`INSERT INTO customer_reservations(
      event_key, reservation_id, customer_id, customer_name, genre, shoot_date,
      start_time, end_time, plan_label, place, total_amount, status, source,
      raw_json, created_at, updated_at
    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'),datetime('now'))
    ON CONFLICT(event_key) DO UPDATE SET
      reservation_id=excluded.reservation_id,
      customer_id=excluded.customer_id,
      customer_name=excluded.customer_name,
      genre=excluded.genre,
      shoot_date=excluded.shoot_date,
      start_time=excluded.start_time,
      end_time=excluded.end_time,
      plan_label=excluded.plan_label,
      place=excluded.place,
      total_amount=excluded.total_amount,
      status=excluded.status,
      source=excluded.source,
      raw_json=excluded.raw_json,
      deleted_at='',
      updated_at=datetime('now')`)
    .bind(
      eventKey,
      reservationId,
      customerId,
      text(sourcePayload.customer_name || draft.customer_name),
      genre,
      shootDate,
      text(sourcePayload.start_time || draft.start_time),
      text(sourcePayload.end_time),
      planLabel,
      text(sourcePayload.place || draft.place),
      amount,
      text(sourcePayload.status) || "予約確定",
      "reservation_app_created_sync",
      JSON.stringify(raw).slice(0, 8000)
    )
    .run();

  await env.DB.prepare(`INSERT INTO customer_timeline(
      event_key, customer_id, event_type, event_title, event_date, amount, detail_json, created_at
    ) VALUES(?,?,?,?,?,?,?,datetime('now'))
    ON CONFLICT(event_key) DO UPDATE SET
      event_title=excluded.event_title,
      event_date=excluded.event_date,
      amount=excluded.amount,
      detail_json=excluded.detail_json,
      deleted_at=''`)
    .bind(
      timelineKey,
      customerId,
      "reservation_created",
      `本予約作成：${genre || planLabel || "予約"}${shootDate ? " / " + shootDate : ""}`,
      shootDate || createdAt,
      amount,
      JSON.stringify({ event_key: eventKey, reservation_id: reservationId, draft_id: draft.id, payload: sourcePayload }).slice(0, 8000)
    )
    .run();

  await refreshCustomerStats(env, customerId);

  await env.DB.prepare(`UPDATE crm_reservation_drafts
    SET history_synced_at=datetime('now'),
        history_event_key=?,
        updated_at=datetime('now')
    WHERE id=?`)
    .bind(eventKey, draft.id)
    .run();

  return { event_key: eventKey, timeline_event_key: timelineKey, reservation_id: reservationId, customer_id: customerId };
}

async function markDraftCreatedAndSync(request, env, draftId) {
  const auth = await requireUser(request, env, WRITE_ROLES);
  if (!auth.ok) return auth.response;

  const body = await readJson(request);
  const reservationId = text(body.reservation_id || body.converted_reservation_id || body.id);
  const intakeId = text(body.reservation_intake_id || body.intake_id || body.crm_intake_id);
  const createdAt = text(body.created_at || body.converted_at) || nowIso();
  if (!reservationId) return json({ ok: false, message: "reservation_id is required" }, 400);

  const before = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts WHERE id=? LIMIT 1`).bind(draftId).first();
  if (!before) return json({ ok: false, message: "CRM reservation draft not found" }, 404);

  await env.DB.prepare(`UPDATE crm_reservation_drafts
    SET status='created',
        converted_at=?,
        converted_by=?,
        reservation_app_reservation_id=?,
        reservation_app_intake_id=COALESCE(NULLIF(?, ''), reservation_intake_id),
        reservation_app_created_at=?,
        reservation_app_created_by=?,
        reservation_app_response=?,
        updated_at=datetime('now')
    WHERE id=?`)
    .bind(createdAt, auth.email, reservationId, intakeId, createdAt, auth.email, JSON.stringify(body).slice(0, 5000), draftId)
    .run();

  const draft = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts WHERE id=? LIMIT 1`).bind(draftId).first();
  const history = await syncDraftToCustomerHistory(env, draft, auth.email, body);
  const item = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts WHERE id=? LIMIT 1`).bind(draftId).first();

  return json({ ok: true, draft_id: draftId, reservation_id: reservationId, reservation_intake_id: intakeId, history_synced: true, history, item });
}

async function syncDraftManually(request, env, draftId) {
  const auth = await requireUser(request, env, WRITE_ROLES);
  if (!auth.ok) return auth.response;
  const body = await readJson(request);
  const draft = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts WHERE id=? LIMIT 1`).bind(draftId).first();
  if (!draft) return json({ ok: false, message: "CRM reservation draft not found" }, 404);
  if (!text(draft.reservation_app_reservation_id) && text(draft.status) !== "created") {
    return json({ ok: false, message: "本予約作成済みの予約下書きだけCRM予約履歴へ反映できます" }, 400);
  }
  const history = await syncDraftToCustomerHistory(env, draft, auth.email, body);
  const item = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts WHERE id=? LIMIT 1`).bind(draftId).first();
  return json({ ok: true, draft_id: draftId, history_synced: true, history, item });
}

async function reservationHistoryReflectSummary(request, env, customerId) {
  const auth = await requireUser(request, env, READ_ROLES);
  if (!auth.ok) return auth.response;

  const drafts = await env.DB.prepare(`SELECT id, customer_id, customer_name, genre, shoot_date, start_time, place,
       plan_label, total_amount, status, reservation_app_reservation_id, reservation_app_intake_id,
       reservation_app_created_at, history_synced_at, history_event_key
    FROM crm_reservation_drafts
    WHERE customer_id=?
    ORDER BY COALESCE(reservation_app_created_at, updated_at, created_at) DESC, id DESC
    LIMIT 100`).bind(customerId).all();

  const rows = drafts.results || [];
  const created = rows.filter((x) => text(x.reservation_app_reservation_id) || text(x.status) === "created");
  const reflected = created.filter((x) => text(x.history_synced_at));
  const notReflected = created.filter((x) => !text(x.history_synced_at));

  return json({
    ok: true,
    customer_id: customerId,
    summary: {
      created_count: created.length,
      reflected_count: reflected.length,
      not_reflected_count: notReflected.length
    },
    reflected,
    not_reflected: notReflected,
    created
  });
}

function injectHistorySyncUi(html) {
  if (!html || !html.includes("</head>") || !html.includes("</body>")) return html;

  const style = `<style id="crm-reservation-history-sync-style">
.crm-history-sync-card{border:1px solid #bbf7d0;background:linear-gradient(180deg,#f0fdf4,#fff);border-radius:16px;padding:12px;margin:10px 0;box-shadow:0 8px 22px rgba(22,163,74,.08)}.crm-history-sync-head{display:flex;justify-content:space-between;gap:8px;align-items:center;flex-wrap:wrap}.crm-history-sync-title{font-weight:950;color:#166534}.crm-history-sync-badge{display:inline-flex;align-items:center;border-radius:999px;background:#dcfce7;color:#15803d;font-size:.76rem;font-weight:950;padding:5px 9px}.crm-history-sync-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px;margin:9px 0}.crm-history-sync-mini{background:#fff;border:1px solid #bbf7d0;border-radius:13px;padding:9px}.crm-history-sync-mini span{display:block;color:#64748b;font-size:.75rem;font-weight:800}.crm-history-sync-mini b{display:block;color:#0f172a;font-size:1rem}.crm-history-sync-item{border:1px solid #dcfce7;background:#fff;border-radius:13px;padding:9px;margin-top:7px}.crm-history-sync-muted{color:#64748b;font-size:.8rem}.crm-history-sync-actions{display:flex;gap:6px;flex-wrap:wrap;margin-top:7px}.crm-history-sync-btn{border:1px solid #86efac;background:#f0fdf4;color:#166534;border-radius:999px;padding:7px 10px;font-size:.75rem;font-weight:950;cursor:pointer}.crm-history-sync-warn{border-color:#fde68a;background:#fffbeb}.crm-history-sync-warn .crm-history-sync-badge{background:#fef3c7;color:#92400e}
</style>`;

  const script = `<script id="crm-reservation-history-sync-script">
(function(){
  if(window.__crmReservationHistorySyncInstalled)return;
  window.__crmReservationHistorySyncInstalled=true;
  function qs(sel,root){return (root||document).querySelector(sel)}
  function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]})}
  function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin'},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
  function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:999999;background:#0f172a;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.18)';document.body.appendChild(d);setTimeout(function(){d.remove()},2200)}
  function customerId(){return window.__crmSuiteCustomerId||''}
  function reservePane(){var p=qs('#crmSuiteDetailPanel');return p?qs('[data-pane="reserve"]',p):null}
  function render(){
    var id=customerId(), pane=reservePane();
    if(!id||!pane||!pane.innerHTML)return;
    api('/api/customers/'+encodeURIComponent(id)+'/reservation-history-reflect-summary').then(function(d){
      if(!d.ok)return;
      var s=d.summary||{}, missing=d.not_reflected||[], reflected=d.reflected||[];
      var cls=missing.length?'crm-history-sync-card crm-history-sync-warn':'crm-history-sync-card';
      var html='<div id="crmReservationHistorySyncCard" class="'+cls+'">';
      html+='<div class="crm-history-sync-head"><div class="crm-history-sync-title">CRM予約履歴への反映状況</div><span class="crm-history-sync-badge">'+(missing.length?'未反映 '+missing.length+'件':'反映済み')+'</span></div>';
      html+='<div class="crm-history-sync-grid"><div class="crm-history-sync-mini"><span>本予約作成済み</span><b>'+esc(s.created_count||0)+'件</b></div><div class="crm-history-sync-mini"><span>CRM予約履歴反映済み</span><b>'+esc(s.reflected_count||0)+'件</b></div><div class="crm-history-sync-mini"><span>未反映</span><b>'+esc(s.not_reflected_count||0)+'件</b></div></div>';
      if(missing.length){
        html+='<div class="crm-history-sync-muted">本予約IDはありますが、CRM予約履歴に未反映の下書きがあります。</div>'+missing.map(function(x){return '<div class="crm-history-sync-item"><b>'+esc(x.genre||x.plan_label||'予約')+'</b><div class="crm-history-sync-muted">予約ID：'+esc(x.reservation_app_reservation_id||'-')+' / '+esc(x.shoot_date||'-')+' '+esc(x.start_time||'')+'</div><div class="crm-history-sync-actions"><button class="crm-history-sync-btn" data-history-sync-draft="'+esc(x.id)+'">CRM予約履歴に反映</button></div></div>'}).join('');
      }else if(reflected.length){
        html+='<div class="crm-history-sync-muted">予約管理で作成された本予約は、CRM予約履歴にも反映済みです。</div>';
      }else{
        html+='<div class="crm-history-sync-muted">本予約作成後、ここにCRM予約履歴への反映状況が表示されます。</div>';
      }
      html+='</div>';
      var existing=qs('#crmReservationHistorySyncCard',pane);
      if(existing){existing.outerHTML=html}else{var base=qs('#crmReservationCreatedSummaryCard',pane); if(base){base.insertAdjacentHTML('afterend',html)}else{pane.insertAdjacentHTML('afterbegin',html)}}
    })
  }
  document.addEventListener('click',function(e){
    var b=e.target.closest('[data-history-sync-draft]');
    if(b){
      if(!confirm('この本予約をCRM予約履歴に反映しますか？'))return;
      api('/api/reservation-drafts/'+encodeURIComponent(b.getAttribute('data-history-sync-draft'))+'/sync-to-customer-history',{method:'POST',headers:{'content-type':'application/json'},body:'{}'}).then(function(d){
        if(!d.ok){alert(d.message||'反映に失敗しました');return}
        toast('CRM予約履歴に反映しました');
        setTimeout(render,400);
      });
      return;
    }
    if(e.target.closest('[data-tab="reserve"]'))setTimeout(render,600)
  });
  var mo=new MutationObserver(function(){clearTimeout(window.__crmHistorySyncTimer);window.__crmHistorySyncTimer=setTimeout(function(){var p=reservePane();if(p&&p.classList.contains('active'))render()},900)});
  mo.observe(document.documentElement,{childList:true,subtree:true});
  document.addEventListener('DOMContentLoaded',function(){setTimeout(render,1500)});
  setInterval(function(){var p=reservePane();if(p&&p.classList.contains('active'))render()},5000);
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
      const markId = draftIdFromMarkPath(path);
      if (markId && request.method === "POST") return await markDraftCreatedAndSync(request, env, markId);

      const syncId = draftIdFromSyncPath(path);
      if (syncId && request.method === "POST") return await syncDraftManually(request, env, syncId);

      const customerId = summaryCustomerIdFromPath(path);
      if (customerId && request.method === "GET") return await reservationHistoryReflectSummary(request, env, customerId);
    } catch (e) {
      return json({ ok: false, build: BUILD, message: e && e.message ? e.message : String(e) }, 500);
    }

    const res = await app.fetch(request, env, ctx);
    const contentType = res.headers.get("content-type") || "";
    if (request.method === "GET" && contentType.includes("text/html")) {
      const html = await res.text();
      return new Response(injectHistorySyncUi(html), {
        status: res.status,
        statusText: res.statusText,
        headers: securityHeaders({ "content-type": "text/html; charset=utf-8" })
      });
    }
    return res;
  }
};
