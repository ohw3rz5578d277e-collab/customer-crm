// ======================================================
// CUSTOMER CRM API / RESERVATION CREATED SYNC WRAPPER
// build: customer-crm-api-reservation-created-sync-20260613-01
// Receives reservation-app "real reservation created" callbacks.
// ======================================================

import app from "./production-index-crm-reservation-send.js";

const BUILD = "customer-crm-api-reservation-created-sync-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const WRITE_ROLES = ["root_admin", "admin", "staff"];

function text(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

function normalizeEmail(v) {
  return text(v).toLowerCase();
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

  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_app_created
    ON crm_reservation_drafts(status, reservation_app_created_at)`).run();
}

async function requireUser(request, env) {
  await ensureSchema(env);
  const email = getAccessEmail(request);
  if (!email) return { ok: false, response: json({ ok: false, message: "Login required" }, 401) };

  const user = await env.DB.prepare(
    `SELECT email, role, status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`
  ).bind(email).first();

  if (!user) return { ok: false, response: json({ ok: false, message: "User is not allowed" }, 403) };
  if (!WRITE_ROLES.includes(user.role || "")) {
    return { ok: false, response: json({ ok: false, message: "Permission denied" }, 403) };
  }
  return { ok: true, email, user };
}

function draftIdFromPath(path) {
  const m = path.match(/^\/api\/reservation-drafts\/([^/]+)\/mark-created-from-reservation$/);
  if (!m) return 0;
  const id = parseInt(decodeURIComponent(m[1]), 10);
  return Number.isFinite(id) && id > 0 ? id : 0;
}

async function markDraftCreatedFromReservation(request, env, draftId) {
  const auth = await requireUser(request, env);
  if (!auth.ok) return auth.response;

  const body = await readJson(request);
  const reservationId = text(body.reservation_id || body.converted_reservation_id || body.id);
  const intakeId = text(body.reservation_intake_id || body.intake_id || body.crm_intake_id);
  const createdAt = text(body.created_at || body.converted_at) || nowIso();

  if (!reservationId) {
    return json({ ok: false, message: "reservation_id is required" }, 400);
  }

  const before = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts WHERE id=? LIMIT 1`)
    .bind(draftId)
    .first();
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
    .bind(
      createdAt,
      auth.email,
      reservationId,
      intakeId,
      createdAt,
      auth.email,
      JSON.stringify(body).slice(0, 5000),
      draftId
    )
    .run();

  const item = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts WHERE id=? LIMIT 1`)
    .bind(draftId)
    .first();

  return json({ ok: true, draft_id: draftId, reservation_id: reservationId, reservation_intake_id: intakeId, item });
}

function injectCreatedSyncUi(html) {
  if (!html || !html.includes("</head>") || !html.includes("</body>")) return html;

  const style = `<style id="crm-reservation-created-sync-style">
.crm-reserve-btn.created{background:#eff6ff!important;border-color:#bfdbfe!important;color:#1d4ed8!important;cursor:default}.crm-reserve-created-note{font-size:.78rem;color:#1d4ed8;font-weight:900;margin-top:3px}
</style>`;

  const script = `<script id="crm-reservation-created-sync-script">
(function(){
  if(window.__crmReservationCreatedSyncInstalled)return;
  window.__crmReservationCreatedSyncInstalled=true;
  function qsa(sel,root){return Array.prototype.slice.call((root||document).querySelectorAll(sel))}
  function enhance(){
    qsa('[data-reserve-draft-id]').forEach(function(row){
      var text=(row.textContent||'');
      if(!/status.?created|ステータス.?created|本予約作成済み|reservation_app_reservation_id|予約ID/i.test(text))return;
      row.querySelectorAll('[data-reserve-send-to-app]').forEach(function(btn){btn.removeAttribute('data-reserve-send-to-app');btn.disabled=true;btn.className='crm-reserve-btn created';btn.textContent='本予約作成済み'});
      var actions=row.querySelector('.crm-reserve-actions')||row;
      if(!actions.querySelector('.crm-reserve-created-note')){
        var note=document.createElement('div');
        note.className='crm-reserve-created-note';
        note.textContent='予約管理側で本予約作成済みです。';
        actions.insertBefore(note, actions.firstChild);
      }
    });
  }
  var mo=new MutationObserver(function(){clearTimeout(window.__crmReserveCreatedTimer);window.__crmReserveCreatedTimer=setTimeout(enhance,300)});
  mo.observe(document.documentElement,{childList:true,subtree:true});
  document.addEventListener('DOMContentLoaded', enhance);
  setInterval(enhance,2000);
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
      const draftId = draftIdFromPath(path);
      if (draftId && request.method === "POST") {
        return await markDraftCreatedFromReservation(request, env, draftId);
      }
    } catch (e) {
      return json({ ok: false, build: BUILD, message: e && e.message ? e.message : String(e) }, 500);
    }

    const res = await app.fetch(request, env, ctx);
    const contentType = res.headers.get("content-type") || "";
    if (request.method === "GET" && contentType.includes("text/html")) {
      const html = await res.text();
      return new Response(injectCreatedSyncUi(html), {
        status: res.status,
        statusText: res.statusText,
        headers: securityHeaders({ "content-type": "text/html; charset=utf-8" })
      });
    }
    return res;
  }
};
