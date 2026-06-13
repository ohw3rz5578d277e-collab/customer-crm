// ======================================================
// CUSTOMER CRM API / SEND RESERVATION DRAFT TO RESERVATION APP
// build: customer-crm-api-crm-reservation-send-20260613-01
// Adds CRM-side "send reservation draft to reservation app" action.
// ======================================================

import app from "./production-index-reservation-bridge.js";

const BUILD = "customer-crm-api-crm-reservation-send-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const DEFAULT_RESERVATION_API_BASE = "https://reservation-app-api.ohw3rz5578d277e.workers.dev";

function text(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

function normalizeEmail(v) {
  return text(v).toLowerCase();
}

function num(v, fallback = 0) {
  if (v === undefined || v === null || v === "") return fallback;
  const n = Number(String(v).replace(/[,円¥\s]/g, ""));
  return Number.isFinite(n) ? n : fallback;
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

async function addColumn(db, table, definition) {
  try {
    await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run();
  } catch (_) {}
}

async function ensureReservationSendSchema(env) {
  if (!env.DB) throw new Error("D1 DB binding(DB) is missing");

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
    converted_by TEXT
  )`).run();

  for (const col of [
    "reservation_intake_id TEXT",
    "sent_to_reservation_at TEXT",
    "sent_to_reservation_by TEXT",
    "sent_to_reservation_response TEXT"
  ]) await addColumn(env.DB, "crm_reservation_drafts", col);

  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_sent
    ON crm_reservation_drafts(status, sent_to_reservation_at)`).run();
}

function draftIdFromPath(path, suffix) {
  if (!path.startsWith("/api/reservation-drafts/") || !path.endsWith(suffix)) return 0;
  const raw = path.slice("/api/reservation-drafts/".length, -suffix.length);
  const id = parseInt(raw, 10);
  return Number.isFinite(id) && id > 0 ? id : 0;
}

async function getDraftWithCustomer(env, draftId) {
  return await env.DB.prepare(`
    SELECT
      d.id, d.customer_id, d.customer_name, d.genre, d.shoot_date, d.start_time,
      d.place, d.plan_label, d.total_amount, d.status, d.memo, d.created_by,
      d.created_at, d.updated_at, d.converted_at, d.converted_by,
      d.reservation_intake_id, d.sent_to_reservation_at, d.sent_to_reservation_by,
      c.name AS crm_customer_name, c.line_display_name, c.phone, c.email, c.address,
      c.genre_history, c.last_shoot_date, c.total_revenue, c.repeat_count
    FROM crm_reservation_drafts d
    LEFT JOIN customers c ON c.customer_id=d.customer_id
    WHERE d.id=?
    LIMIT 1
  `).bind(draftId).first();
}

function buildReservationPayload(draft) {
  const customerName = text(draft.customer_name || draft.crm_customer_name) || "名称未設定";
  const memoLines = [
    text(draft.memo),
    draft.phone ? `電話: ${text(draft.phone)}` : "",
    draft.email ? `メール: ${text(draft.email)}` : "",
    draft.line_display_name ? `LINE名: ${text(draft.line_display_name)}` : "",
    draft.address ? `住所: ${text(draft.address)}` : "",
    draft.genre_history ? `過去ジャンル: ${text(draft.genre_history)}` : "",
    draft.last_shoot_date ? `最終撮影日: ${text(draft.last_shoot_date)}` : ""
  ].filter(Boolean).join("\n");

  return {
    id: `CRM-DRAFT-${draft.id}`,
    source: "customer_crm",
    reservation_draft: {
      id: String(draft.id),
      draft_id: String(draft.id),
      customer_id: text(draft.customer_id),
      customer_name: customerName,
      name: customerName,
      genre: text(draft.genre) || "未設定",
      shoot_date: text(draft.shoot_date),
      start_time: text(draft.start_time),
      place: text(draft.place),
      plan_label: text(draft.plan_label),
      total_amount: num(draft.total_amount),
      memo: memoLines
    }
  };
}

async function postToReservationApp(request, env, payload, current) {
  const endpointPath = "/api/crm-reservation-intake/drafts/import";
  const headers = {
    "content-type": "application/json; charset=utf-8",
    "x-user-email": current.email,
    "cf-access-authenticated-user-email": current.email
  };

  if (env.RESERVATION_SERVICE && typeof env.RESERVATION_SERVICE.fetch === "function") {
    return await env.RESERVATION_SERVICE.fetch(new Request(`https://reservation-app-api${endpointPath}`, {
      method: "POST",
      headers,
      body: JSON.stringify(payload)
    }));
  }

  const base = text(env.RESERVATION_API_BASE) || DEFAULT_RESERVATION_API_BASE;
  return await fetch(new URL(endpointPath, base).toString(), {
    method: "POST",
    headers,
    body: JSON.stringify(payload)
  });
}

async function sendReservationDraftToReservationApp(request, env, draftId) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "edit permission required" }, 403);

  await ensureReservationSendSchema(env);
  const draft = await getDraftWithCustomer(env, draftId);
  if (!draft) return json({ ok: false, message: "reservation draft not found" }, 404);

  const payload = buildReservationPayload(draft);
  let responseData = null;
  let ok = false;
  let status = 0;

  try {
    const res = await postToReservationApp(request, env, payload, current);
    status = res.status;
    responseData = await res.json().catch(async () => ({ ok: false, text: await res.text().catch(() => "") }));
    ok = !!responseData?.ok && res.ok;
  } catch (e) {
    responseData = { ok: false, message: e && e.message ? e.message : String(e) };
  }

  if (!ok) {
    await env.DB.prepare(`UPDATE crm_reservation_drafts
      SET sent_to_reservation_response=?, updated_at=datetime('now')
      WHERE id=?`)
      .bind(JSON.stringify({ status, response: responseData }).slice(0, 5000), draftId)
      .run();
    return json({ ok: false, message: "reservation app import failed", status, response: responseData }, 502);
  }

  const intakeId = text(responseData?.item?.id || responseData?.id || `CRM-DRAFT-${draftId}`);
  await env.DB.prepare(`UPDATE crm_reservation_drafts
    SET status='sent_to_reservation', reservation_intake_id=?, sent_to_reservation_at=datetime('now'),
        sent_to_reservation_by=?, sent_to_reservation_response=?, updated_at=datetime('now')
    WHERE id=?`)
    .bind(intakeId, current.email, JSON.stringify(responseData).slice(0, 5000), draftId)
    .run();

  return json({ ok: true, draft_id: draftId, reservation_intake_id: intakeId, reservation_response: responseData });
}

function injectReservationSendUi(html) {
  if (!html || !html.includes("</head>") || !html.includes("</body>")) return html;

  const style = `<style id="crm-reservation-send-style">
.crm-reserve-btn.send{background:#16a34a;border-color:#16a34a;color:#fff}.crm-reserve-btn.sent{background:#ecfdf5;border-color:#bbf7d0;color:#15803d;cursor:default}.crm-reserve-send-note{font-size:.78rem;color:#15803d;font-weight:900;margin-top:3px}
</style>`;

  const script = `<script id="crm-reservation-send-script">
(function(){
  if(window.__crmReservationSendInstalled)return;
  window.__crmReservationSendInstalled=true;
  function qs(sel,root){return (root||document).querySelector(sel)}
  function qsa(sel,root){return Array.prototype.slice.call((root||document).querySelectorAll(sel))}
  function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin',headers:{'content-type':'application/json'}},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
  function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:999999;background:#0f172a;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.18)';document.body.appendChild(d);setTimeout(function(){d.remove()},2400)}
  function enhance(){
    qsa('[data-reserve-draft-id]').forEach(function(row){
      if(row.getAttribute('data-reserve-send-enhanced')==='1')return;
      row.setAttribute('data-reserve-send-enhanced','1');
      var actions=qs('.crm-reserve-actions',row)||row;
      var statusText=(row.textContent||'');
      if(/sent_to_reservation|予約管理へ送信済み/.test(statusText)){
        var done=document.createElement('span');done.className='crm-reserve-btn sent';done.textContent='予約管理へ送信済み';actions.insertBefore(done,actions.firstChild);return;
      }
      var btn=document.createElement('button');btn.type='button';btn.className='crm-reserve-btn send';btn.setAttribute('data-reserve-send-to-app','1');btn.textContent='予約管理へ送る';actions.insertBefore(btn,actions.firstChild);
    });
  }
  document.addEventListener('click',function(e){
    var btn=e.target.closest('[data-reserve-send-to-app]');
    if(!btn)return;
    var row=btn.closest('[data-reserve-draft-id]');
    var id=row&&row.getAttribute('data-reserve-draft-id');
    if(!id)return;
    if(!confirm('この予約下書きを予約管理へ送りますか？\n※本予約はまだ作成されず、予約管理側のCRM予約候補に入ります。'))return;
    btn.disabled=true;btn.textContent='送信中...';
    api('/api/reservation-drafts/'+encodeURIComponent(id)+'/send-to-reservation',{method:'POST',body:'{}'}).then(function(x){
      if(x.ok){toast('予約管理へ送りました');btn.className='crm-reserve-btn sent';btn.textContent='予約管理へ送信済み';btn.removeAttribute('data-reserve-send-to-app');}
      else{toast('送信に失敗しました');btn.disabled=false;btn.textContent='予約管理へ送る';console.warn('reservation send failed',x)}
    }).catch(function(err){toast('送信に失敗しました');btn.disabled=false;btn.textContent='予約管理へ送る';console.warn(err)})
  });
  var mo=new MutationObserver(function(){clearTimeout(window.__crmReserveSendTimer);window.__crmReserveSendTimer=setTimeout(enhance,400)});
  mo.observe(document.documentElement,{childList:true,subtree:true});
  setTimeout(enhance,1000);
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
      const sendDraftId = draftIdFromPath(path, "/send-to-reservation");
      if (sendDraftId && request.method === "POST") {
        return await sendReservationDraftToReservationApp(request, env, sendDraftId);
      }
    } catch (e) {
      return json({ ok: false, build: BUILD, message: e && e.message ? e.message : String(e) }, 500);
    }

    const res = await app.fetch(request, env, ctx);
    const contentType = res.headers.get("content-type") || "";
    if (request.method === "GET" && contentType.includes("text/html")) {
      const html = await res.text();
      return new Response(injectReservationSendUi(html), {
        status: res.status,
        headers: securityHeaders({ "content-type": "text/html; charset=utf-8" })
      });
    }
    return res;
  }
};
