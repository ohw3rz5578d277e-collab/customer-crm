// ======================================================
// CUSTOMER CRM API / RESERVATION UPDATE SYNC WRAPPER
// build: customer-crm-api-reservation-update-sync-20260613-01
// Syncs reservation-app updates into CRM reservation history.
// ======================================================

import app from "./production-index-crm-reservation-history-sync.js";

const BUILD = "customer-crm-api-reservation-update-sync-20260613-01";
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
    reservation_app_reservation_id TEXT,
    reservation_app_intake_id TEXT,
    reservation_app_created_at TEXT,
    reservation_app_created_by TEXT,
    reservation_app_response TEXT,
    history_synced_at TEXT,
    history_event_key TEXT,
    reservation_app_updated_at TEXT,
    reservation_app_updated_by TEXT,
    reservation_app_update_response TEXT
  )`).run();

  for (const col of [
    "reservation_app_updated_at TEXT",
    "reservation_app_updated_by TEXT",
    "reservation_app_update_response TEXT",
    "history_synced_at TEXT",
    "history_event_key TEXT",
    "reservation_app_reservation_id TEXT",
    "reservation_app_intake_id TEXT",
    "reservation_app_created_at TEXT",
    "reservation_app_created_by TEXT",
    "reservation_app_response TEXT"
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

function draftIdFromUpdatePath(path) {
  const m = path.match(/^\/api\/reservation-drafts\/([^/]+)\/sync-reservation-update$/);
  if (!m) return 0;
  const id = parseInt(decodeURIComponent(m[1]), 10);
  return Number.isFinite(id) && id > 0 ? id : 0;
}

function updateSummaryCustomerIdFromPath(path) {
  const m = path.match(/^\/api\/customers\/([^/]+)\/reservation-update-sync-summary$/);
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

function bodyValue(body, key, fallback) {
  return Object.prototype.hasOwnProperty.call(body, key) ? body[key] : fallback;
}

async function upsertReservationHistoryFromUpdate(env, draft, body, actorEmail) {
  const customerId = text(draft.customer_id);
  if (!customerId) throw new Error("customer_id is missing on reservation draft");

  const reservationId = text(body.reservation_id || draft.reservation_app_reservation_id || draft.reservation_intake_id || `crm-draft-${draft.id}`);
  const eventKey = reservationEventKey(draft, reservationId);
  const timelineKey = `timeline:reservation-update:${reservationId}`;
  const updatedAt = text(body.updated_at || body.reservation_updated_at) || nowIso();

  const genre = text(bodyValue(body, "genre", draft.genre));
  const shootDate = text(bodyValue(body, "shoot_date", draft.shoot_date));
  const startTime = text(bodyValue(body, "start_time", draft.start_time));
  const endTime = text(bodyValue(body, "end_time", ""));
  const place = text(bodyValue(body, "place", draft.place));
  const planLabel = text(bodyValue(body, "plan_label", draft.plan_label));
  const amount = money(bodyValue(body, "total_amount", draft.total_amount));
  const status = text(bodyValue(body, "status", "予約確定")) || "予約確定";

  const raw = {
    source: "reservation_app_update_sync",
    synced_at: nowIso(),
    synced_by: actorEmail,
    draft_id: draft.id,
    reservation_id: reservationId,
    payload: body
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
      text(body.customer_name || draft.customer_name),
      genre,
      shootDate,
      startTime,
      endTime,
      planLabel,
      place,
      amount,
      status,
      "reservation_app_update_sync",
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
      "reservation_updated",
      `本予約更新：${genre || planLabel || "予約"}${shootDate ? " / " + shootDate : ""}`,
      shootDate || updatedAt,
      amount,
      JSON.stringify({ event_key: eventKey, reservation_id: reservationId, draft_id: draft.id, payload: body }).slice(0, 8000)
    )
    .run();

  await env.DB.prepare(`UPDATE crm_reservation_drafts
    SET genre=?,
        shoot_date=?,
        start_time=?,
        place=?,
        plan_label=?,
        total_amount=?,
        status=CASE WHEN COALESCE(status,'')='' OR status='draft' THEN 'created' ELSE status END,
        reservation_app_reservation_id=COALESCE(NULLIF(?, ''), reservation_app_reservation_id),
        reservation_app_updated_at=?,
        reservation_app_updated_by=?,
        reservation_app_update_response=?,
        history_synced_at=datetime('now'),
        history_event_key=?,
        updated_at=datetime('now')
    WHERE id=?`)
    .bind(
      genre,
      shootDate,
      startTime,
      place,
      planLabel,
      amount,
      reservationId,
      updatedAt,
      actorEmail,
      JSON.stringify(body).slice(0, 5000),
      eventKey,
      draft.id
    )
    .run();

  await refreshCustomerStats(env, customerId);

  return { event_key: eventKey, timeline_event_key: timelineKey, reservation_id: reservationId, customer_id: customerId };
}

async function syncReservationUpdate(request, env, draftId) {
  const auth = await requireUser(request, env, WRITE_ROLES);
  if (!auth.ok) return auth.response;

  const body = await readJson(request);
  const draft = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts WHERE id=? LIMIT 1`).bind(draftId).first();
  if (!draft) return json({ ok: false, message: "CRM reservation draft not found" }, 404);

  const reservationId = text(body.reservation_id || draft.reservation_app_reservation_id || draft.reservation_intake_id);
  if (!reservationId) return json({ ok: false, message: "reservation_id is required" }, 400);

  const history = await upsertReservationHistoryFromUpdate(env, draft, { ...body, reservation_id: reservationId }, auth.email);
  const item = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts WHERE id=? LIMIT 1`).bind(draftId).first();

  return json({ ok: true, draft_id: draftId, reservation_id: reservationId, update_synced: true, history, item });
}

async function reservationUpdateSyncSummary(request, env, customerId) {
  const auth = await requireUser(request, env, READ_ROLES);
  if (!auth.ok) return auth.response;

  const rows = (await env.DB.prepare(`SELECT id, customer_id, customer_name, genre, shoot_date, start_time, place,
       plan_label, total_amount, status, reservation_app_reservation_id, reservation_app_intake_id,
       reservation_app_created_at, reservation_app_updated_at, reservation_app_updated_by, history_synced_at, history_event_key
    FROM crm_reservation_drafts
    WHERE customer_id=? AND COALESCE(reservation_app_reservation_id,'')<>''
    ORDER BY COALESCE(reservation_app_updated_at, reservation_app_created_at, updated_at, created_at) DESC, id DESC
    LIMIT 50`).bind(customerId).all()).results || [];

  const updated = rows.filter((x) => text(x.reservation_app_updated_at));
  const createdOnly = rows.filter((x) => !text(x.reservation_app_updated_at));

  return json({
    ok: true,
    customer_id: customerId,
    summary: {
      reservation_count: rows.length,
      updated_count: updated.length,
      created_only_count: createdOnly.length
    },
    updated,
    created_only: createdOnly,
    items: rows
  });
}

function injectUpdateSyncUi(html) {
  if (!html || !html.includes("</head>") || !html.includes("</body>")) return html;

  const style = `<style id="crm-reservation-update-sync-style">
.crm-update-sync-card{border:1px solid #bfdbfe;background:linear-gradient(180deg,#eff6ff,#fff);border-radius:16px;padding:12px;margin:10px 0;box-shadow:0 8px 22px rgba(37,99,235,.08)}.crm-update-sync-head{display:flex;justify-content:space-between;gap:8px;align-items:center;flex-wrap:wrap}.crm-update-sync-title{font-weight:950;color:#1d4ed8}.crm-update-sync-badge{display:inline-flex;align-items:center;border-radius:999px;background:#dbeafe;color:#1d4ed8;font-size:.76rem;font-weight:950;padding:5px 9px}.crm-update-sync-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:8px;margin:9px 0}.crm-update-sync-mini{background:#fff;border:1px solid #bfdbfe;border-radius:13px;padding:9px}.crm-update-sync-mini span{display:block;color:#64748b;font-size:.75rem;font-weight:800}.crm-update-sync-mini b{display:block;color:#0f172a;font-size:1rem}.crm-update-sync-item{border:1px solid #dbeafe;background:#fff;border-radius:13px;padding:9px;margin-top:7px}.crm-update-sync-muted{color:#64748b;font-size:.8rem}
</style>`;

  const script = `<script id="crm-reservation-update-sync-script">
(function(){
  if(window.__crmReservationUpdateSyncInstalled)return;
  window.__crmReservationUpdateSyncInstalled=true;
  function qs(sel,root){return (root||document).querySelector(sel)}
  function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]})}
  function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin'},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
  function customerId(){return window.__crmSuiteCustomerId||''}
  function reservePane(){var p=qs('#crmSuiteDetailPanel');return p?qs('[data-pane="reserve"]',p):null}
  function render(){
    var id=customerId(), pane=reservePane();
    if(!id||!pane||!pane.innerHTML)return;
    api('/api/customers/'+encodeURIComponent(id)+'/reservation-update-sync-summary').then(function(d){
      if(!d.ok)return;
      var s=d.summary||{}, updated=d.updated||[], createdOnly=d.created_only||[];
      var html='<div id="crmReservationUpdateSyncCard" class="crm-update-sync-card">';
      html+='<div class="crm-update-sync-head"><div class="crm-update-sync-title">予約変更の同期状況</div><span class="crm-update-sync-badge">更新同期 '+esc(s.updated_count||0)+'件</span></div>';
      html+='<div class="crm-update-sync-grid"><div class="crm-update-sync-mini"><span>本予約連携済み</span><b>'+esc(s.reservation_count||0)+'件</b></div><div class="crm-update-sync-mini"><span>変更同期済み</span><b>'+esc(s.updated_count||0)+'件</b></div><div class="crm-update-sync-mini"><span>作成後変更なし</span><b>'+esc(s.created_only_count||0)+'件</b></div></div>';
      if(updated.length){
        html+='<div class="crm-update-sync-muted">予約管理側で変更された内容が、CRM予約履歴にも反映されています。</div>'+updated.slice(0,5).map(function(x){return '<div class="crm-update-sync-item"><b>'+esc(x.genre||x.plan_label||'予約')+'</b><div class="crm-update-sync-muted">予約ID：'+esc(x.reservation_app_reservation_id||'-')+' / 更新：'+esc(x.reservation_app_updated_at||'-')+' / '+esc(x.shoot_date||'-')+' '+esc(x.start_time||'')+'</div></div>'}).join('');
      }else if(createdOnly.length){
        html+='<div class="crm-update-sync-muted">本予約作成済みです。予約管理側で日時・場所・金額を変更すると、ここに同期状況が表示されます。</div>';
      }else{
        html+='<div class="crm-update-sync-muted">本予約作成後、予約変更の同期状況が表示されます。</div>';
      }
      html+='</div>';
      var existing=qs('#crmReservationUpdateSyncCard',pane);
      if(existing){existing.outerHTML=html}else{var base=qs('#crmReservationHistorySyncCard',pane)||qs('#crmReservationCreatedSummaryCard',pane); if(base){base.insertAdjacentHTML('afterend',html)}else{pane.insertAdjacentHTML('afterbegin',html)}}
    })
  }
  document.addEventListener('DOMContentLoaded',render);
  document.addEventListener('click',function(e){setTimeout(render,600)});
  setInterval(render,4000);
})();
</script>`;

  return html.replace("</head>", style + "</head>").replace("</body>", script + "</body>");
}

async function wrapHtmlIfNeeded(response) {
  const contentType = response.headers.get("content-type") || "";
  if (!contentType.includes("text/html")) return response;
  const body = await response.text();
  return new Response(injectUpdateSyncUi(body), {
    status: response.status,
    statusText: response.statusText,
    headers: securityHeaders(response.headers)
  });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    try {
      const draftId = draftIdFromUpdatePath(url.pathname);
      if (draftId && request.method === "POST") {
        return await syncReservationUpdate(request, env, draftId);
      }

      const customerId = updateSummaryCustomerIdFromPath(url.pathname);
      if (customerId && request.method === "GET") {
        return await reservationUpdateSyncSummary(request, env, customerId);
      }

      const response = await app.fetch(request, env, ctx);
      return await wrapHtmlIfNeeded(response);
    } catch (e) {
      return json({
        ok: false,
        message: "Server error",
        detail: e && e.message ? e.message : String(e),
        build: BUILD
      }, 500);
    }
  }
};