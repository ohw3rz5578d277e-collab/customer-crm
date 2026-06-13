// ======================================================
// CUSTOMER CRM / RESERVATION LINK RESYNC WRAPPER
// build: customer-crm-api-reservation-link-resync-20260613-01
// Adds safe resync actions to reservation link monitor.
// ======================================================

import app from "./production-index-crm-link-monitor.js";

const BUILD = "customer-crm-api-reservation-link-resync-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const WRITE_ROLES = ["root_admin", "admin", "staff"];

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

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_reservation_link_resync_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    draft_id INTEGER NOT NULL,
    customer_id TEXT,
    requested_action TEXT,
    resolved_action TEXT,
    actor_email TEXT,
    before_stage TEXT,
    after_stage TEXT,
    ok INTEGER DEFAULT 0,
    message TEXT,
    response_json TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();

  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_reservation_link_resync_logs_draft
    ON crm_reservation_link_resync_logs(draft_id, created_at)`).run();

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
}

async function requireEditor(request, env) {
  await ensureSchema(env);
  const email = getAccessEmail(request);
  if (!email) return { ok: false, response: json({ ok: false, message: "Login required" }, 401) };
  const user = await env.DB.prepare(
    `SELECT email, role, status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`
  ).bind(email).first();
  if (!user) return { ok: false, response: json({ ok: false, message: "User is not allowed" }, 403) };
  if (!WRITE_ROLES.includes(user.role || "")) return { ok: false, response: json({ ok: false, message: "Permission denied" }, 403) };
  return { ok: true, email, user };
}

function draftIdFromResyncPath(path) {
  const m = path.match(/^\/api\/reservation-link-monitor\/([^/]+)\/resync$/);
  if (!m) return 0;
  const id = parseInt(decodeURIComponent(m[1]), 10);
  return Number.isFinite(id) && id > 0 ? id : 0;
}

function classifyDraft(row) {
  const cancelled = !!(row.reservation_app_cancelled_at || row.status === "cancelled");
  const sent = !!row.sent_to_reservation_at;
  const created = !!row.reservation_app_created_at || !!row.reservation_app_reservation_id || row.status === "created";
  const historySynced = !!row.history_synced_at;
  const updated = !!row.reservation_app_updated_at;
  if (cancelled) return "cancelled";
  if (updated) return "updated";
  if (historySynced) return "synced";
  if (created) return "created_unreflected";
  if (sent) return "sent_uncreated";
  return "draft";
}

function resolveAction(row, requestedAction) {
  const action = text(requestedAction || "auto");
  const stage = classifyDraft(row);

  if (["send", "history", "cancel", "update"].includes(action)) return action;

  if (stage === "draft") return "send";
  if (stage === "sent_uncreated") return "send"; // Reservation app import uses CRM-DRAFT-id, so this is an upsert/resend.
  if (stage === "created_unreflected") return "history";
  if (stage === "synced") return "history";
  if (stage === "updated") return "history";
  if (stage === "cancelled") return row.cancellation_synced_at ? "noop" : "cancel";
  return "noop";
}

function actionEndpoint(draftId, action) {
  if (action === "send") return `/api/reservation-drafts/${encodeURIComponent(draftId)}/send-to-reservation`;
  if (action === "history") return `/api/reservation-drafts/${encodeURIComponent(draftId)}/sync-to-customer-history`;
  if (action === "cancel") return `/api/reservation-drafts/${encodeURIComponent(draftId)}/sync-reservation-cancel`;
  if (action === "update") return `/api/reservation-drafts/${encodeURIComponent(draftId)}/sync-reservation-update`;
  return "";
}

function buildForwardHeaders(request, email) {
  const h = new Headers();
  h.set("content-type", "application/json; charset=utf-8");
  h.set("x-user-email", email);
  h.set("cf-access-authenticated-user-email", email);
  const cookie = request.headers.get("cookie");
  if (cookie) h.set("cookie", cookie);
  return h;
}

async function callInternalAction(request, env, ctx, draftId, action, body, email) {
  const endpoint = actionEndpoint(draftId, action);
  if (!endpoint) return { ok: true, skipped: true, message: "再同期は不要です。" };

  const internalReq = new Request(`https://customer-crm-api${endpoint}`, {
    method: "POST",
    headers: buildForwardHeaders(request, email),
    body: JSON.stringify(body || {})
  });
  const res = await app.fetch(internalReq, env, ctx);
  const data = await res.json().catch(async () => ({ ok: false, text: await res.text().catch(() => "") }));
  return { ok: res.ok && !!data.ok, status: res.status, response: data };
}

async function writeLog(env, draftId, row, requestedAction, resolvedAction, email, beforeStage, afterStage, result) {
  try {
    await env.DB.prepare(`INSERT INTO crm_reservation_link_resync_logs(
      draft_id, customer_id, requested_action, resolved_action, actor_email,
      before_stage, after_stage, ok, message, response_json, created_at
    ) VALUES(?,?,?,?,?,?,?,?,?,?,datetime('now'))`)
      .bind(
        draftId,
        text(row.customer_id),
        text(requestedAction || "auto"),
        text(resolvedAction),
        email,
        beforeStage,
        afterStage,
        result.ok ? 1 : 0,
        text(result.message || result.response?.message || result.response?.error || ""),
        JSON.stringify(result).slice(0, 8000)
      ).run();
  } catch (_) {}
}

async function resyncDraft(request, env, ctx, draftId) {
  const auth = await requireEditor(request, env);
  if (!auth.ok) return auth.response;

  const body = await readJson(request);
  const requestedAction = text(body.action || "auto");
  const row = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts WHERE id=? LIMIT 1`).bind(draftId).first();
  if (!row) return json({ ok: false, message: "reservation draft not found" }, 404);

  const beforeStage = classifyDraft(row);
  const resolvedAction = resolveAction(row, requestedAction);

  let result;
  if (resolvedAction === "noop") {
    result = { ok: true, skipped: true, message: "この予約連携は再同期不要です。" };
  } else {
    result = await callInternalAction(request, env, ctx, draftId, resolvedAction, body.payload || body, auth.email);
  }

  const after = await env.DB.prepare(`SELECT * FROM crm_reservation_drafts WHERE id=? LIMIT 1`).bind(draftId).first();
  const afterStage = after ? classifyDraft(after) : "missing";
  await writeLog(env, draftId, row, requestedAction, resolvedAction, auth.email, beforeStage, afterStage, result);

  if (!result.ok) {
    return json({
      ok: false,
      build: BUILD,
      draft_id: draftId,
      requested_action: requestedAction,
      resolved_action: resolvedAction,
      before_stage: beforeStage,
      after_stage: afterStage,
      result
    }, result.status && result.status >= 400 ? result.status : 502);
  }

  return json({
    ok: true,
    build: BUILD,
    draft_id: draftId,
    requested_action: requestedAction,
    resolved_action: resolvedAction,
    before_stage: beforeStage,
    after_stage: afterStage,
    result,
    item: after
  });
}

async function listResyncLogs(request, env, draftId) {
  const auth = await requireEditor(request, env);
  if (!auth.ok) return auth.response;
  const result = await env.DB.prepare(`SELECT * FROM crm_reservation_link_resync_logs
    WHERE draft_id=? ORDER BY created_at DESC, id DESC LIMIT 50`).bind(draftId).all();
  return json({ ok: true, draft_id: draftId, logs: result.results || [] });
}

function injectResyncUi(html) {
  if (!html || !html.includes("crmLinkMonitorScript") || html.includes("crmLinkResyncScript")) return html;

  const style = `<style id="crmLinkResyncStyle">
.crm-link-resync-btn{border:1px solid #a7f3d0!important;background:#ecfdf5!important;color:#047857!important}.crm-link-resync-btn.warn{border-color:#fde68a!important;background:#fffbeb!important;color:#92400e!important}.crm-link-resync-btn.danger{border-color:#fecaca!important;background:#fef2f2!important;color:#991b1b!important}
</style>`;

  const script = `<script id="crmLinkResyncScript">
(function(){
  if(window.__crmLinkResyncInstalled)return; window.__crmLinkResyncInstalled=true;
  function esc(v){return String(v == null ? '' : v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
  function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:1000000;background:#0f172a;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.18)';document.body.appendChild(d);setTimeout(function(){d.remove()},2600)}
  function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin',headers:{'content-type':'application/json'}},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
  function actionButtons(rowHtml){return rowHtml.replace('</div></td></tr>','<button class="crm-link-resync-btn" data-crm-link-resync="auto">自動再同期</button><button class="crm-link-resync-btn warn" data-crm-link-resync="send">予約管理へ再送</button><button class="crm-link-resync-btn" data-crm-link-resync="history">CRM履歴へ反映</button></div></td></tr>')}
  function patchRows(){
    var body=document.getElementById('crmLinkMonitorRows'); if(!body || body.getAttribute('data-resync-patched')==='1')return;
    body.setAttribute('data-resync-patched','1');
    var orig=Object.getOwnPropertyDescriptor(Element.prototype,'innerHTML');
  }
  var oldMap=Array.prototype.map;
  function enhanceButtons(){
    var rows=document.querySelectorAll('#crmLinkMonitorRows tr');
    rows.forEach(function(tr){
      if(tr.getAttribute('data-resync-enhanced')==='1')return;
      var copyBtn=tr.querySelector('[data-copy]');
      if(!copyBtn)return;
      var id=copyBtn.getAttribute('data-copy');
      if(!/^\\d+$/.test(id||''))return;
      tr.setAttribute('data-resync-enhanced','1');
      var box=copyBtn.closest('.crm-link-monitor-actions'); if(!box)return;
      var auto=document.createElement('button');auto.className='crm-link-resync-btn';auto.setAttribute('data-crm-link-resync','auto');auto.setAttribute('data-draft-id',id);auto.textContent='自動再同期';box.appendChild(auto);
      var history=document.createElement('button');history.className='crm-link-resync-btn';history.setAttribute('data-crm-link-resync','history');history.setAttribute('data-draft-id',id);history.textContent='CRM履歴へ反映';box.appendChild(history);
      var send=document.createElement('button');send.className='crm-link-resync-btn warn';send.setAttribute('data-crm-link-resync','send');send.setAttribute('data-draft-id',id);send.textContent='予約管理へ再送';box.appendChild(send);
    });
  }
  async function runResync(btn){
    var id=btn.getAttribute('data-draft-id'); var action=btn.getAttribute('data-crm-link-resync')||'auto';
    if(!id)return;
    var msg=action==='send'?'予約管理へ再送します。既存候補IDが同じ場合は上書きされます。実行しますか？':(action==='history'?'CRM予約履歴へ再反映します。実行しますか？':'状態に応じて自動再同期します。実行しますか？');
    if(!confirm(msg))return;
    btn.disabled=true; var old=btn.textContent; btn.textContent='実行中...';
    try{
      var res=await api('/api/reservation-link-monitor/'+encodeURIComponent(id)+'/resync',{method:'POST',body:JSON.stringify({action:action})});
      if(res.ok){toast('再同期しました：'+(res.resolved_action||action)); if(window.crmOpenReservationLinkMonitor){ setTimeout(function(){ var reload=document.getElementById('crmLinkMonitorReload'); if(reload) reload.click(); },450); }}
      else{toast('再同期に失敗しました'); console.warn('resync failed',res);}
    }catch(e){toast('再同期に失敗しました'); console.warn(e)}
    btn.disabled=false; btn.textContent=old;
  }
  document.addEventListener('click',function(e){var b=e.target&&e.target.closest&&e.target.closest('[data-crm-link-resync]'); if(b)runResync(b);});
  var mo=new MutationObserver(function(){enhanceButtons();});
  document.addEventListener('DOMContentLoaded',function(){var el=document.getElementById('crmLinkMonitorRows'); if(el)mo.observe(el,{childList:true,subtree:true}); enhanceButtons();});
  setInterval(enhanceButtons,1000);
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

    const draftId = draftIdFromResyncPath(url.pathname);
    if (draftId && request.method === "POST") {
      return await resyncDraft(request, env, ctx, draftId);
    }
    if (draftId && request.method === "GET") {
      return await listResyncLogs(request, env, draftId);
    }

    const res = await app.fetch(request, env, ctx);
    const type = res.headers.get("content-type") || "";
    if (type.includes("text/html")) {
      const body = injectResyncUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }
    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
