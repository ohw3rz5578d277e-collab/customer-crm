// ======================================================
// CUSTOMER CRM API / RESERVATION BRIDGE WRAPPER
// build: customer-crm-api-reservation-bridge-20260613-01
// Adds CRM-side reservation handoff, reservation drafts, and reservation tab UI.
// ======================================================

import app from "./production-index-crm-ops-polish.js";

const BUILD = "customer-crm-api-reservation-bridge-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const DEFAULT_RESERVATION_ADMIN_URL = "https://reservation-app-api.ohw3rz5578d277e.workers.dev/admin";

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

async function ensureReservationBridgeSchema(env) {
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
    "customer_id TEXT",
    "customer_name TEXT",
    "genre TEXT",
    "shoot_date TEXT",
    "start_time TEXT",
    "place TEXT",
    "plan_label TEXT",
    "total_amount REAL DEFAULT 0",
    "status TEXT DEFAULT 'draft'",
    "memo TEXT",
    "draft_json TEXT",
    "created_by TEXT",
    "created_at TEXT",
    "updated_at TEXT",
    "converted_at TEXT",
    "converted_by TEXT"
  ]) await addColumn(env.DB, "crm_reservation_drafts", col);

  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_customer ON crm_reservation_drafts(customer_id, created_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_reservation_drafts_status ON crm_reservation_drafts(status, created_at)`).run();
}

function customerIdFromPath(path, suffix) {
  const prefix = "/api/customers/";
  if (!path.startsWith(prefix) || !path.endsWith(suffix)) return "";
  return decodeURIComponent(path.slice(prefix.length, -suffix.length)).replace(/^\/+|\/+$/g, "");
}

function draftIdFromPath(path, suffix = "") {
  let raw = path.replace("/api/reservation-drafts/", "");
  if (suffix && raw.endsWith(suffix)) raw = raw.slice(0, -suffix.length);
  const id = parseInt(raw, 10);
  return Number.isFinite(id) && id > 0 ? id : 0;
}

function genreSuggest(customer) {
  const g = text(customer?.genre_history);
  const ageMemo = text(customer?.memo);
  if (/お宮参り/.test(g)) return "バースデー / 七五三 / ファミリー";
  if (/七五三/.test(g)) return "入学・卒業 / 兄弟撮影 / ファミリー";
  if (/マタニティ/.test(g)) return "ニューボーン / お宮参り";
  if (/バースデー/.test(g)) return "次回バースデー / ファミリー";
  if (/成人/.test(g)) return "家族写真 / 卒業";
  if (/3歳|5歳|7歳|七五三/.test(ageMemo)) return "七五三";
  return "ファミリー / 季節撮影";
}

function buildReservationAdminUrl(env, customer) {
  const base = text(env.RESERVATION_ADMIN_URL) || DEFAULT_RESERVATION_ADMIN_URL;
  const u = new URL(base);
  u.searchParams.set("from", "customer_crm");
  u.searchParams.set("customer_id", text(customer.customer_id));
  u.searchParams.set("name", text(customer.name || customer.customer_name));
  if (customer.phone) u.searchParams.set("phone", text(customer.phone));
  if (customer.email) u.searchParams.set("email", text(customer.email));
  if (customer.line_display_name) u.searchParams.set("line", text(customer.line_display_name));
  return u.toString();
}

function reservationCopyText(customer, draft = {}) {
  return [
    "【予約作成用メモ】",
    `顧客ID：${text(customer.customer_id)}`,
    `お名前：${text(customer.name || customer.customer_name)}`,
    `LINE名：${text(customer.line_display_name) || "-"}`,
    `電話：${text(customer.phone) || "-"}`,
    `メール：${text(customer.email) || "-"}`,
    `住所：${text(customer.address) || "-"}`,
    `過去ジャンル：${text(customer.genre_history) || "-"}`,
    `最終撮影日：${text(customer.last_shoot_date) || "-"}`,
    `累計売上：${text(customer.total_revenue) || "0"}`,
    "",
    "【今回の予約候補】",
    `ジャンル：${text(draft.genre) || genreSuggest(customer)}`,
    `撮影日：${text(draft.shoot_date) || "未定"}`,
    `開始時間：${text(draft.start_time) || "未定"}`,
    `場所：${text(draft.place) || "未定"}`,
    `プラン：${text(draft.plan_label) || "未定"}`,
    `メモ：${text(draft.memo) || ""}`
  ].join("\n");
}

async function getCustomer(env, customerId) {
  return await env.DB.prepare(`
    SELECT customer_id, name, furigana, line_display_name, phone, address, email,
           genre_history, last_shoot_date, repeat_count, total_revenue,
           acquisition_source, child1_name, child1_birthdate, child2_name, child2_birthdate,
           child3_name, child3_birthdate, photo_public_ok, memo, line_user_id,
           dormant_days, updated_at
    FROM customers
    WHERE customer_id=? AND (deleted_at IS NULL OR deleted_at='')
    LIMIT 1
  `).bind(customerId).first();
}

async function getLocalReservations(env, customerId) {
  const rows = await env.DB.prepare(`
    SELECT id, event_key, reservation_id, customer_id, customer_name, genre,
           shoot_date, start_time, end_time, plan_label, place, total_amount,
           status, source, created_at, updated_at
    FROM customer_reservations
    WHERE customer_id=? AND (deleted_at IS NULL OR deleted_at='')
    ORDER BY COALESCE(shoot_date, created_at) DESC, id DESC
    LIMIT 30
  `).bind(customerId).all();
  return rows.results || [];
}

async function getReservationDrafts(request, env, customerId) {
  const current = await getCurrentUser(request, env);
  if (!current) return json({ ok: false, message: "login required" }, 401);
  await ensureReservationBridgeSchema(env);
  const rows = await env.DB.prepare(`
    SELECT id, customer_id, customer_name, genre, shoot_date, start_time, place,
           plan_label, total_amount, status, memo, created_by, created_at,
           updated_at, converted_at, converted_by
    FROM crm_reservation_drafts
    WHERE customer_id=?
    ORDER BY created_at DESC, id DESC
    LIMIT 50
  `).bind(customerId).all();
  return json({ ok: true, customer_id: customerId, drafts: rows.results || [] });
}

async function getReservationHandoff(request, env, customerId) {
  const current = await getCurrentUser(request, env);
  if (!current) return json({ ok: false, message: "login required" }, 401);
  await ensureReservationBridgeSchema(env);
  const customer = await getCustomer(env, customerId);
  if (!customer) return json({ ok: false, message: "customer not found" }, 404);
  const reservations = await getLocalReservations(env, customerId);
  const drafts = await env.DB.prepare(`
    SELECT id, genre, shoot_date, start_time, place, plan_label, total_amount,
           status, memo, created_by, created_at, converted_at
    FROM crm_reservation_drafts
    WHERE customer_id=?
    ORDER BY created_at DESC, id DESC
    LIMIT 10
  `).bind(customerId).all();
  const latest = reservations[0] || null;
  const suggestedGenre = genreSuggest(customer);
  const suggestedDraft = {
    genre: suggestedGenre,
    shoot_date: "",
    start_time: "",
    place: "",
    plan_label: "",
    total_amount: 0,
    memo: ""
  };
  return json({
    ok: true,
    customer,
    reservations,
    drafts: drafts.results || [],
    summary: {
      reservation_count: reservations.length,
      latest_reservation: latest,
      suggested_genre: suggestedGenre,
      reservation_admin_url: buildReservationAdminUrl(env, customer),
      copy_text: reservationCopyText(customer, suggestedDraft)
    }
  });
}

async function createReservationDraft(request, env, customerId) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "edit permission required" }, 403);
  await ensureReservationBridgeSchema(env);
  const customer = await getCustomer(env, customerId);
  if (!customer) return json({ ok: false, message: "customer not found" }, 404);
  const body = await readJson(request);
  const draft = {
    genre: text(body.genre) || genreSuggest(customer),
    shoot_date: text(body.shoot_date || body.shootDate),
    start_time: text(body.start_time || body.startTime),
    place: text(body.place || body.location),
    plan_label: text(body.plan_label || body.plan),
    total_amount: Number(String(body.total_amount || body.amount || 0).replace(/[,円¥\s]/g, "")) || 0,
    memo: text(body.memo || body.note)
  };

  const res = await env.DB.prepare(`
    INSERT INTO crm_reservation_drafts(
      customer_id, customer_name, genre, shoot_date, start_time, place,
      plan_label, total_amount, status, memo, draft_json, created_by,
      created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?, ?, datetime('now'), datetime('now'))
  `).bind(
    customerId,
    text(customer.name || customer.customer_name),
    draft.genre,
    draft.shoot_date,
    draft.start_time,
    draft.place,
    draft.plan_label,
    draft.total_amount,
    draft.memo,
    JSON.stringify({ customer, draft, copy_text: reservationCopyText(customer, draft) }),
    current.email
  ).run();

  return json({ ok: true, id: res.meta?.last_row_id || null, draft, copy_text: reservationCopyText(customer, draft) });
}

async function markReservationDraftCreated(request, env, draftId) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "edit permission required" }, 403);
  await ensureReservationBridgeSchema(env);
  const res = await env.DB.prepare(`
    UPDATE crm_reservation_drafts
    SET status='created', converted_at=datetime('now'), converted_by=?, updated_at=datetime('now')
    WHERE id=?
  `).bind(current.email, draftId).run();
  return json({ ok: true, updated: res.meta?.changes || 0 });
}

async function deleteReservationDraft(request, env, draftId) {
  const current = await getCurrentUser(request, env);
  if (!isEditor(current)) return json({ ok: false, message: "edit permission required" }, 403);
  await ensureReservationBridgeSchema(env);
  const res = await env.DB.prepare(`DELETE FROM crm_reservation_drafts WHERE id=?`).bind(draftId).run();
  return json({ ok: true, deleted: res.meta?.changes || 0 });
}

function injectReservationBridgeUi(html) {
  if (!html || !html.includes("</head>") || !html.includes("</body>")) return html;

  const style = `<style id="crm-reservation-bridge-style">
.crm-reserve-box{border:1px solid #ddd6fe;background:#fbfaff;border-radius:16px;padding:12px;margin:10px 0}.crm-reserve-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px;margin:8px 0}.crm-reserve-mini{background:#fff;border:1px solid #e9d5ff;border-radius:13px;padding:9px}.crm-reserve-mini b{display:block;font-size:1.05rem}.crm-reserve-actions{display:flex;gap:6px;flex-wrap:wrap;margin:8px 0}.crm-reserve-btn{border:1px solid #c4b5fd;background:#f5f3ff;color:#5b21b6;border-radius:999px;padding:8px 11px;font-size:.78rem;font-weight:950;cursor:pointer;text-decoration:none;display:inline-flex}.crm-reserve-btn.primary{background:#2563eb;border-color:#2563eb;color:#fff}.crm-reserve-btn.danger{background:#fff1f2;border-color:#fecaca;color:#be123c}.crm-reserve-input{width:100%;box-sizing:border-box;border:1px solid #cbd5e1;border-radius:12px;padding:9px 10px;background:#fff;font-size:.9rem;margin:4px 0}.crm-reserve-muted{color:#64748b;font-size:.82rem}.crm-reserve-log{border-top:1px solid #e9d5ff;padding:8px 0}.crm-reserve-log:first-child{border-top:0}
</style>`;

  const script = `<script id="crm-reservation-bridge-script">
(function(){
  if(window.__crmReservationBridgeInstalled)return;
  window.__crmReservationBridgeInstalled=true;
  function qs(sel,root){return (root||document).querySelector(sel)}
  function qsa(sel,root){return Array.prototype.slice.call((root||document).querySelectorAll(sel))}
  function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]})}
  function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin',headers:{'content-type':'application/json'}},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
  function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:999999;background:#0f172a;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.18)';document.body.appendChild(d);setTimeout(function(){d.remove()},2200)}
  function customerId(){return window.__crmSuiteCustomerId||''}
  function reservePane(){var p=qs('#crmSuiteDetailPanel');return p?qs('[data-pane="reserve"]',p):null}
  function copyText(v){if(navigator.clipboard&&navigator.clipboard.writeText){navigator.clipboard.writeText(v).then(function(){toast('コピーしました')})}else{var ta=document.createElement('textarea');ta.value=v;document.body.appendChild(ta);ta.select();document.execCommand('copy');ta.remove();toast('コピーしました')}}

  function renderReservationBridge(){
    var id=customerId(); var pane=reservePane();
    if(!id||!pane)return;
    if(pane.getAttribute('data-reserve-bridge-rendering')==='1')return;
    pane.setAttribute('data-reserve-bridge-rendering','1');
    api('/api/customers/'+encodeURIComponent(id)+'/reservation-handoff').then(function(d){
      pane.removeAttribute('data-reserve-bridge-rendering');
      if(!d.ok){pane.innerHTML='<div class="crm-reserve-muted">予約連携情報を取得できませんでした。</div>';return}
      var c=d.customer||{}, s=d.summary||{}, reservations=d.reservations||[], drafts=d.drafts||[];
      pane.innerHTML='<div class="crm-reserve-box"><div style="font-weight:950">予約管理連携</div><div class="crm-reserve-muted">CRMの顧客情報を使って、予約作成用メモと下書きを作成します。</div>'+ 
        '<div class="crm-reserve-grid"><div class="crm-reserve-mini"><span>予約履歴</span><b>'+esc(s.reservation_count||0)+'件</b></div><div class="crm-reserve-mini"><span>提案ジャンル</span><b>'+esc(s.suggested_genre||'-')+'</b></div><div class="crm-reserve-mini"><span>最終撮影</span><b>'+esc(c.last_shoot_date||'-')+'</b></div></div>'+ 
        '<div class="crm-reserve-actions"><button class="crm-reserve-btn primary" id="crmReserveCopy">予約作成用メモをコピー</button><a class="crm-reserve-btn" href="'+esc(s.reservation_admin_url||'#')+'" target="_blank" rel="noopener">予約管理を開く</a></div>'+ 
        '<div class="crm-reserve-grid"><input class="crm-reserve-input" id="crmReserveGenre" placeholder="ジャンル" value="'+esc(s.suggested_genre||'')+'"><input class="crm-reserve-input" id="crmReserveDate" type="date"><input class="crm-reserve-input" id="crmReserveTime" type="time"><input class="crm-reserve-input" id="crmReservePlace" placeholder="撮影場所"><input class="crm-reserve-input" id="crmReservePlan" placeholder="プラン"><input class="crm-reserve-input" id="crmReserveAmount" placeholder="金額"></div><textarea class="crm-reserve-input" id="crmReserveMemo" placeholder="予約メモ" style="min-height:70px"></textarea><div class="crm-reserve-actions"><button class="crm-reserve-btn primary" id="crmReserveDraftSave">予約下書き保存</button></div></div>'+ 
        '<div class="crm-reserve-box"><div style="font-weight:950">予約下書き</div>'+(drafts.length?drafts.map(function(x){return '<div class="crm-reserve-log" data-reserve-draft-id="'+esc(x.id)+'"><b>'+esc(x.genre||'予約下書き')+'</b><div class="crm-reserve-muted">'+esc(x.shoot_date||'日付未定')+' / '+esc(x.place||'場所未定')+' / '+esc(x.status||'draft')+'</div><div>'+esc(x.memo||'')+'</div><div class="crm-reserve-actions">'+(x.status==='created'?'':'<button class="crm-reserve-btn primary" data-reserve-created>予約作成済みにする</button>')+'<button class="crm-reserve-btn danger" data-reserve-delete>削除</button></div></div>'}).join(''):'<div class="crm-reserve-muted">予約下書きはありません。</div>')+'</div>'+ 
        '<div class="crm-reserve-box"><div style="font-weight:950">CRM内の予約履歴</div>'+(reservations.length?reservations.map(function(r){return '<div class="crm-reserve-log"><b>'+esc(r.genre||r.plan_label||'予約')+'</b><div class="crm-reserve-muted">'+esc(r.shoot_date||'-')+' '+esc(r.start_time||'')+' / '+esc(r.place||'-')+' / ¥'+Number(r.total_amount||0).toLocaleString()+' / '+esc(r.status||'')+'</div></div>'}).join(''):'<div class="crm-reserve-muted">CRM内の予約履歴はまだありません。</div>')+'</div>';
      qs('#crmReserveCopy',pane).onclick=function(){copyText(s.copy_text||'')};
      qs('#crmReserveDraftSave',pane).onclick=function(){
        var body={genre:qs('#crmReserveGenre',pane).value,shoot_date:qs('#crmReserveDate',pane).value,start_time:qs('#crmReserveTime',pane).value,place:qs('#crmReservePlace',pane).value,plan_label:qs('#crmReservePlan',pane).value,total_amount:qs('#crmReserveAmount',pane).value,memo:qs('#crmReserveMemo',pane).value};
        api('/api/customers/'+encodeURIComponent(id)+'/reservation-drafts',{method:'POST',body:JSON.stringify(body)}).then(function(x){toast(x.ok?'予約下書きを保存しました':'保存に失敗しました');renderReservationBridge()})
      };
    });
  }

  document.addEventListener('click',function(e){
    var id=customerId();
    if(e.target.closest('[data-tab="reserve"]'))setTimeout(renderReservationBridge,200);
    var d=e.target.closest('[data-reserve-draft-id]');
    if(d&&e.target.closest('[data-reserve-created]')){api('/api/reservation-drafts/'+d.getAttribute('data-reserve-draft-id')+'/mark-created',{method:'POST',body:'{}'}).then(function(x){toast(x.ok?'予約作成済みにしました':'更新に失敗しました');renderReservationBridge()})}
    if(d&&e.target.closest('[data-reserve-delete]')){if(!confirm('この予約下書きを削除しますか？'))return;api('/api/reservation-drafts/'+d.getAttribute('data-reserve-draft-id'),{method:'DELETE',body:'{}'}).then(function(x){toast(x.ok?'削除しました':'削除に失敗しました');renderReservationBridge()})}
  });
  var mo=new MutationObserver(function(){clearTimeout(window.__crmReserveBridgeTimer);window.__crmReserveBridgeTimer=setTimeout(function(){if(reservePane()&&reservePane().classList.contains('active'))renderReservationBridge()},700)});
  mo.observe(document.documentElement,{childList:true,subtree:true});
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
      const handoffId = customerIdFromPath(path, "/reservation-handoff");
      if (handoffId && request.method === "GET") return await getReservationHandoff(request, env, handoffId);

      const draftsId = customerIdFromPath(path, "/reservation-drafts");
      if (draftsId && request.method === "GET") return await getReservationDrafts(request, env, draftsId);
      if (draftsId && request.method === "POST") return await createReservationDraft(request, env, draftsId);

      if (path.startsWith("/api/reservation-drafts/") && path.endsWith("/mark-created") && request.method === "POST") {
        return await markReservationDraftCreated(request, env, draftIdFromPath(path, "/mark-created"));
      }

      if (path.startsWith("/api/reservation-drafts/") && request.method === "DELETE") {
        return await deleteReservationDraft(request, env, draftIdFromPath(path));
      }
    } catch (err) {
      return json({ ok: false, message: "reservation bridge error", error: String(err && err.message ? err.message : err) }, 500);
    }

    const res = await app.fetch(request, env, ctx);
    const contentType = res.headers.get("content-type") || "";

    if (contentType.includes("text/html")) {
      const body = injectReservationBridgeUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }

    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
