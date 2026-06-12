// ======================================================
// CUSTOMER CRM API / GOOGLE ACCESS SECURE ENTRYPOINT
// build: customer-crm-api-google-access-20260612-01
// ======================================================

import app from "./index.js";

const BUILD = "customer-crm-api-google-access-20260612-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const ADMIN_ROLES = ["admin", "root_admin"];

function text(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

function escHtml(v) {
  return String(v ?? "").replace(/[&<>\"]/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[m]));
}

function toFloat(value, fallback = 0) {
  if (value === undefined || value === null || value === "") return fallback;
  const n = parseFloat(String(value).replace(/[,円¥\s]/g, ""));
  return Number.isFinite(n) ? n : fallback;
}

function toInt(value, fallback = 0) {
  if (value === undefined || value === null || value === "") return fallback;
  const n = parseInt(String(value).replace(/[,円¥\s]/g, ""), 10);
  return Number.isFinite(n) ? n : fallback;
}

function secureHeaders(headers = {}) {
  const h = new Headers(headers);
  h.set("cache-control", "no-store, no-cache, must-revalidate, max-age=0");
  h.set("pragma", "no-cache");
  h.set("expires", "0");
  h.set("x-robots-tag", "noindex, nofollow, noarchive");
  h.set("x-content-type-options", "nosniff");
  h.set("referrer-policy", "no-referrer");
  h.set("x-frame-options", "DENY");
  h.set("permissions-policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=()");
  return h;
}

function response(body, status = 200, headers = {}) {
  return new Response(body, { status, headers: secureHeaders(headers) });
}

function json(data, status = 200) {
  return response(JSON.stringify(data, null, 2), status, { "content-type": "application/json; charset=utf-8" });
}

function html(body, status = 200) {
  return response(body, status, { "content-type": "text/html; charset=utf-8" });
}

async function readJson(request) {
  try {
    return await request.json();
  } catch (_) {
    return {};
  }
}

async function addColumn(db, table, definition) {
  try {
    await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run();
  } catch (_) {}
}

function hasUrlToken(url) {
  return url.searchParams.has("token") || /token=/i.test(url.search || "");
}

function accessEmail(request) {
  return (text(request.headers.get("cf-access-authenticated-user-email")) || text(request.headers.get("cf-access-user-email"))).toLowerCase();
}

function loginRequiredPage() {
  return `<!doctype html><html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow,noarchive"><title>Googleログインが必要です</title><style>body{margin:0;min-height:100vh;display:grid;place-items:center;background:#f6f7fb;color:#111827;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans JP',sans-serif}.box{width:min(92vw,460px);background:#fff;border:1px solid #e5e7eb;border-radius:22px;padding:24px;box-shadow:0 18px 60px rgba(15,23,42,.12)}h1{font-size:24px;margin:0 0 8px}.muted{color:#64748b;font-size:14px;line-height:1.8}.warn{background:#fff7ed;border:1px solid #fed7aa;color:#9a3412;border-radius:14px;padding:12px;margin-top:14px;font-size:13px;line-height:1.7}</style></head><body><main class="box"><h1>Googleログインが必要です</h1><div class="muted">この管理画面はCloudflare Zero Trust AccessのGoogleログインで保護します。</div><div class="warn">Cloudflare Access設定が未完了、または許可メールではありません。</div></main></body></html>`;
}

async function ensureAdminUsers(env) {
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users (email TEXT PRIMARY KEY, role TEXT, status TEXT, created_by TEXT, created_at TEXT, updated_at TEXT)`).run();
  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email, role, status, created_by, created_at, updated_at) VALUES(?, 'admin', 'active', 'system', datetime('now'), datetime('now'))`).bind(ROOT_ADMIN_EMAIL).run();
}

async function ensureSoftDeleteSchema(env) {
  if (!env.DB) throw new Error("D1 DB binding(DB) is missing");
  for (const table of ["customers", "customer_reservations", "customer_items", "customer_timeline", "customer_line_messages", "customer_tags"]) {
    await addColumn(env.DB, table, "deleted_at TEXT");
    await addColumn(env.DB, table, "deleted_by TEXT");
    await addColumn(env.DB, table, "delete_reason TEXT");
  }
}

async function getCurrentUser(request, env) {
  const email = accessEmail(request);
  if (!email) return null;
  await ensureAdminUsers(env);
  const row = await env.DB.prepare(`SELECT email, role, status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`).bind(email).first();
  return row ? { email: row.email, role: row.role || "viewer" } : null;
}

function isManager(current) {
  return current && ADMIN_ROLES.includes(current.role);
}

async function listAdminUsers(env) {
  await ensureAdminUsers(env);
  const rows = await env.DB.prepare(`SELECT email, role, status, created_by, created_at, updated_at FROM crm_admin_users ORDER BY CASE WHEN lower(email)=lower(?) THEN 0 ELSE 1 END, status ASC, role ASC, email ASC`).bind(ROOT_ADMIN_EMAIL).all();
  return { ok: true, root_admin_email: ROOT_ADMIN_EMAIL, items: rows.results || [] };
}

async function addAdminUser(request, env, current) {
  if (!isManager(current)) return { ok: false, message: "Only admin can add users" };
  const body = await readJson(request);
  const email = text(body.email).toLowerCase();
  const role = text(body.role || "viewer") === "admin" ? "admin" : "viewer";
  if (!/^.+@.+\..+$/.test(email)) return { ok: false, message: "email is required" };
  await ensureAdminUsers(env);
  await env.DB.prepare(`INSERT INTO crm_admin_users(email, role, status, created_by, created_at, updated_at) VALUES(?, ?, 'active', ?, datetime('now'), datetime('now')) ON CONFLICT(email) DO UPDATE SET role=excluded.role, status='active', updated_at=datetime('now')`).bind(email, role, current.email).run();
  return { ok: true, email, role };
}

async function removeAdminUser(request, env, current) {
  if (!isManager(current)) return { ok: false, message: "Only admin can remove users" };
  const body = await readJson(request);
  const email = text(body.email).toLowerCase();
  if (!email) return { ok: false, message: "email is required" };
  if (email === ROOT_ADMIN_EMAIL) return { ok: false, message: "Root admin cannot be removed" };
  await ensureAdminUsers(env);
  await env.DB.prepare(`UPDATE crm_admin_users SET status='disabled', updated_at=datetime('now') WHERE lower(email)=lower(?)`).bind(email).run();
  return { ok: true, email };
}

function customerSelectSql() {
  return `
    SELECT customer_id,name,furigana,line_display_name,phone,email,address,genre_history,first_shoot_date,last_shoot_date,
      repeat_count,repeat_count_1y,repeat_count_90d,repeat_count_365d,repeat_count_730d,total_revenue,avg_order_value,
      acquisition_source,referrer,child1_name,child1_birthdate,child2_name,child2_birthdate,child3_name,child3_birthdate,
      anniversary,nps,photo_public_ok,memo,line_user_id,dormant_days,square_avg_payment,square_last_payment_date,created_at,updated_at,
      deleted_at,deleted_by,delete_reason
    FROM customers
  `;
}

function segmentWhere(segment) {
  switch (text(segment)) {
    case "repeaters": return "repeat_count >= 2";
    case "first_time": return "repeat_count <= 1";
    case "dormant_180": return "dormant_days >= 180";
    case "dormant_365": return "dormant_days >= 365";
    case "high_value": return "total_revenue >= 100000";
    case "omiyamairi": return "genre_history LIKE '%お宮参り%'";
    case "shichigosan": return "genre_history LIKE '%七五三%'";
    case "line": return "line_user_id IS NOT NULL AND line_user_id <> ''";
    case "no_phone": return "phone IS NULL OR phone = ''";
    case "photo_public_ok": return "photo_public_ok = 1";
    default: return "";
  }
}

async function listActiveCustomers(env, url) {
  await ensureSoftDeleteSchema(env);
  const keyword = text(url.searchParams.get("keyword"));
  const segment = text(url.searchParams.get("segment"));
  const genre = text(url.searchParams.get("genre"));
  const source = text(url.searchParams.get("source"));
  const minRevenue = toFloat(url.searchParams.get("min_revenue"), 0);
  const maxRevenue = toFloat(url.searchParams.get("max_revenue"), 0);
  const minRepeat = toInt(url.searchParams.get("min_repeat"), 0);
  const minDormant = toInt(url.searchParams.get("min_dormant"), 0);
  const photoPublicOk = text(url.searchParams.get("photo_public_ok"));
  const hasChild = text(url.searchParams.get("has_child"));
  const sort = text(url.searchParams.get("sort")) || "updated_at";
  const limitRaw = parseInt(url.searchParams.get("limit") || "100", 10);
  const limit = Math.max(1, Math.min(Number.isFinite(limitRaw) ? limitRaw : 100, 500));

  const where = ["(deleted_at IS NULL OR deleted_at = '')"];
  const params = [];

  if (keyword) {
    const like = `%${keyword}%`;
    where.push(`(name LIKE ? OR furigana LIKE ? OR line_display_name LIKE ? OR phone LIKE ? OR email LIKE ? OR customer_id LIKE ? OR genre_history LIKE ? OR memo LIKE ?)`);
    params.push(like, like, like, like, like, like, like, like);
  }

  const segWhere = segmentWhere(segment);
  if (segWhere) where.push(segWhere);
  if (genre) { where.push("genre_history LIKE ?"); params.push(`%${genre}%`); }
  if (source) { where.push("acquisition_source = ?"); params.push(source); }
  if (minRevenue > 0) { where.push("total_revenue >= ?"); params.push(minRevenue); }
  if (maxRevenue > 0) { where.push("total_revenue <= ?"); params.push(maxRevenue); }
  if (minRepeat > 0) { where.push("repeat_count >= ?"); params.push(minRepeat); }
  if (minDormant > 0) { where.push("dormant_days >= ?"); params.push(minDormant); }
  if (photoPublicOk === "1") where.push("photo_public_ok = 1");
  if (hasChild === "1") where.push("(child1_name IS NOT NULL AND child1_name <> '')");

  const orderBy =
    sort === "revenue" ? "total_revenue DESC, updated_at DESC" :
    sort === "last_shoot" ? "last_shoot_date DESC, updated_at DESC" :
    sort === "repeat" ? "repeat_count DESC, updated_at DESC" :
    sort === "dormant" ? "dormant_days DESC, updated_at DESC" :
    sort === "aov" ? "avg_order_value DESC, updated_at DESC" :
    "updated_at DESC";

  const sql = `${customerSelectSql()} WHERE ${where.join(" AND ")} ORDER BY ${orderBy} LIMIT ?`;
  params.push(limit);

  const result = await env.DB.prepare(sql).bind(...params).all();
  const items = result.results || [];
  return { ok: true, count: items.length, items, filters: { keyword, segment, genre, source, min_revenue: minRevenue, max_revenue: maxRevenue, min_repeat: minRepeat, min_dormant: minDormant, photo_public_ok: photoPublicOk, has_child: hasChild, sort } };
}

async function getActiveCustomerOrNull(env, customerId) {
  await ensureSoftDeleteSchema(env);
  return env.DB.prepare(`${customerSelectSql()} WHERE customer_id=? AND (deleted_at IS NULL OR deleted_at='') LIMIT 1`).bind(customerId).first();
}

async function getActiveSegmentSummary(env) {
  await ensureSoftDeleteSchema(env);
  const active = "(deleted_at IS NULL OR deleted_at='')";
  const row = await env.DB.prepare(`
    SELECT
      COUNT(*) AS total,
      SUM(CASE WHEN repeat_count>=2 THEN 1 ELSE 0 END) AS repeaters,
      SUM(CASE WHEN repeat_count<=1 THEN 1 ELSE 0 END) AS first_time,
      SUM(CASE WHEN dormant_days>=90 THEN 1 ELSE 0 END) AS dormant_90,
      SUM(CASE WHEN dormant_days>=180 THEN 1 ELSE 0 END) AS dormant_180,
      SUM(CASE WHEN dormant_days>=365 THEN 1 ELSE 0 END) AS dormant_365,
      SUM(CASE WHEN total_revenue>=100000 THEN 1 ELSE 0 END) AS high_value,
      SUM(CASE WHEN genre_history LIKE '%お宮参り%' THEN 1 ELSE 0 END) AS omiyamairi,
      SUM(CASE WHEN genre_history LIKE '%七五三%' THEN 1 ELSE 0 END) AS shichigosan,
      SUM(CASE WHEN line_user_id IS NOT NULL AND line_user_id<>'' THEN 1 ELSE 0 END) AS line,
      SUM(CASE WHEN photo_public_ok=1 THEN 1 ELSE 0 END) AS photo_public_ok,
      COALESCE(SUM(total_revenue),0) AS total_revenue,
      COALESCE(AVG(NULLIF(total_revenue,0)),0) AS avg_ltv,
      COALESCE(AVG(NULLIF(avg_order_value,0)),0) AS avg_order_value,
      COALESCE(AVG(NULLIF(repeat_count,0)),0) AS avg_repeat_count
    FROM customers
    WHERE ${active}
  `).first();

  const reservationRow = await env.DB.prepare(`SELECT COUNT(*) AS reservations_total, COALESCE(SUM(total_amount),0) AS reservations_revenue, COALESCE(AVG(NULLIF(total_amount,0)),0) AS reservation_aov FROM customer_reservations WHERE deleted_at IS NULL OR deleted_at=''`).first();
  const itemRow = await env.DB.prepare(`SELECT COUNT(*) AS item_count, COALESCE(SUM(item_amount),0) AS item_revenue, COALESCE(AVG(NULLIF(item_amount,0)),0) AS item_avg_amount FROM customer_items WHERE deleted_at IS NULL OR deleted_at=''`).first();
  const genreResult = await env.DB.prepare(`SELECT COALESCE(genre,'未設定') AS genre, COUNT(*) AS count, COALESCE(SUM(total_amount),0) AS revenue FROM customer_reservations WHERE deleted_at IS NULL OR deleted_at='' GROUP BY COALESCE(genre,'未設定') ORDER BY revenue DESC, count DESC LIMIT 30`).all();
  const sourceResult = await env.DB.prepare(`SELECT COALESCE(acquisition_source,'未設定') AS source, COUNT(*) AS count, COALESCE(SUM(total_revenue),0) AS revenue FROM customers WHERE ${active} GROUP BY COALESCE(acquisition_source,'未設定') ORDER BY count DESC LIMIT 30`).all();
  const itemRanking = await env.DB.prepare(`SELECT COALESCE(item_category,'item') AS item_category, COALESCE(item_name,'未設定') AS item_name, COUNT(*) AS count, COALESCE(SUM(item_amount),0) AS revenue FROM customer_items WHERE deleted_at IS NULL OR deleted_at='' GROUP BY COALESCE(item_category,'item'), COALESCE(item_name,'未設定') ORDER BY revenue DESC, count DESC LIMIT 30`).all();

  const summary = { ...(row || {}), ...(reservationRow || {}), ...(itemRow || {}) };
  const total = Number(summary.total || 0);
  summary.repeat_rate = total > 0 ? Number(summary.repeaters || 0) / total * 100 : 0;
  summary.line_rate = total > 0 ? Number(summary.line || 0) / total * 100 : 0;
  summary.photo_public_rate = total > 0 ? Number(summary.photo_public_ok || 0) / total * 100 : 0;

  return { ok: true, summary, genres: genreResult.results || [], sources: sourceResult.results || [], itemRanking: itemRanking.results || [] };
}

async function softDeleteCustomers(request, env, current, forcedIds = null) {
  if (!isManager(current)) return json({ ok: false, message: "Only admin can delete customers" }, 403);
  await ensureSoftDeleteSchema(env);
  const body = forcedIds ? {} : await readJson(request);
  const rawIds = forcedIds || body.customer_ids || body.customer_id || body.ids || [];
  const ids = (Array.isArray(rawIds) ? rawIds : [rawIds]).map(text).filter(Boolean);
  const reason = text(body.delete_reason || body.reason || body.memo || "顧客管理画面から削除");
  if (!ids.length) return json({ ok: false, message: "customer_ids required" }, 400);

  for (const id of ids) {
    await env.DB.prepare(`UPDATE customers SET deleted_at=datetime('now'), deleted_by=?, delete_reason=?, updated_at=datetime('now') WHERE customer_id=?`).bind(current.email, reason, id).run();
    await env.DB.prepare(`UPDATE customer_reservations SET deleted_at=datetime('now'), deleted_by=?, delete_reason=?, updated_at=datetime('now') WHERE customer_id=?`).bind(current.email, reason, id).run();
    await env.DB.prepare(`UPDATE customer_items SET deleted_at=datetime('now'), deleted_by=?, delete_reason=?, updated_at=datetime('now') WHERE customer_id=?`).bind(current.email, reason, id).run();
    await env.DB.prepare(`UPDATE customer_timeline SET deleted_at=datetime('now'), deleted_by=?, delete_reason=? WHERE customer_id=?`).bind(current.email, reason, id).run();
    await env.DB.prepare(`UPDATE customer_line_messages SET deleted_at=datetime('now'), deleted_by=?, delete_reason=? WHERE customer_id=?`).bind(current.email, reason, id).run();
    await env.DB.prepare(`UPDATE customer_tags SET deleted_at=datetime('now'), deleted_by=?, delete_reason=? WHERE customer_id=?`).bind(current.email, reason, id).run();
  }

  return json({ ok: true, mode: "soft_delete", deleted: ids.length, customer_ids: ids, deleted_by: current.email });
}

function injectUserAdminUi(body, current) {
  if (!body || !body.includes("</body>")) return body;
  const currentEmail = escHtml(current.email);
  const currentRole = escHtml(current.role);
  const currentEmailJson = JSON.stringify(current.email || "");
  const currentRoleJson = JSON.stringify(current.role || "viewer");
  const rootEmailJson = JSON.stringify(ROOT_ADMIN_EMAIL);

  const panel = `
<style id="crm-admin-users-style">
  .crm-user-fab{position:fixed;right:14px;bottom:14px;z-index:99999;background:#fff;border:1px solid #e5e7eb;border-radius:18px;padding:12px 13px;box-shadow:0 16px 44px rgba(15,23,42,.20);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans JP',sans-serif;font-size:12px;line-height:1.5;color:#111827;min-width:210px}.crm-user-fab b{font-size:13px}.crm-user-fab .sub{color:#64748b;font-size:11px;margin:2px 0 8px;word-break:break-all}.crm-user-fab button,.crm-user-modal button{border:1px solid #d1d5db;background:#fff;border-radius:10px;padding:7px 9px;font-weight:800;font-size:12px;cursor:pointer}.crm-user-fab button.primary,.crm-user-modal button.primary{background:#111827;color:#fff;border-color:#111827}.crm-user-backdrop{position:fixed;inset:0;z-index:100000;background:rgba(15,23,42,.38);display:none}.crm-user-backdrop.show{display:block}.crm-user-modal{position:fixed;right:18px;bottom:18px;z-index:100001;width:min(94vw,620px);max-height:min(86vh,760px);overflow:auto;background:#fff;border:1px solid #e5e7eb;border-radius:24px;box-shadow:0 24px 80px rgba(15,23,42,.32);font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans JP',sans-serif;color:#111827;display:none}.crm-user-modal.show{display:block}.crm-user-head{position:sticky;top:0;background:#fff;border-bottom:1px solid #e5e7eb;padding:16px 18px;display:flex;align-items:flex-start;justify-content:space-between;gap:12px;border-radius:24px 24px 0 0}.crm-user-head h2{margin:0;font-size:20px}.crm-user-body{padding:16px 18px 20px}.crm-user-add{display:grid;grid-template-columns:1.4fr .6fr auto;gap:8px;margin:0 0 14px;padding:12px;background:#f8fafc;border:1px solid #e5e7eb;border-radius:16px}.crm-user-add input,.crm-user-add select{width:100%;box-sizing:border-box;border:1px solid #d1d5db;border-radius:12px;padding:10px;font-size:14px;background:#fff}.crm-user-list{display:grid;gap:8px}.crm-user-card{display:grid;grid-template-columns:1fr auto;gap:10px;align-items:center;border:1px solid #e5e7eb;border-radius:16px;padding:12px;background:#fff}.crm-user-email{font-weight:900;word-break:break-all}.crm-user-meta{display:flex;gap:6px;flex-wrap:wrap;margin-top:6px}.crm-badge{display:inline-flex;align-items:center;border-radius:999px;padding:4px 8px;background:#f1f5f9;color:#334155;font-size:11px;font-weight:800}.crm-user-msg{margin:10px 0 0;color:#64748b;font-size:12px;line-height:1.6}.crm-user-error{color:#b91c1c}.crm-user-empty{padding:16px;border:1px dashed #cbd5e1;border-radius:16px;color:#64748b;background:#f8fafc}@media(max-width:760px){.crm-user-fab{left:12px;right:12px;bottom:12px}.crm-user-modal{left:10px;right:10px;bottom:10px;width:auto;max-height:88vh}.crm-user-add{grid-template-columns:1fr}.crm-user-card{grid-template-columns:1fr}}
</style>
<div class="crm-user-fab" id="crmUserFab"><b>Googleログイン</b><div class="sub">${currentEmail}<br>role: ${currentRole}</div><button class="primary" id="crmOpenUsers">ユーザー管理</button></div>
<div class="crm-user-backdrop" id="crmUserBackdrop"></div>
<div class="crm-user-modal" id="crmUserModal" aria-hidden="true"><div class="crm-user-head"><div><h2>管理ユーザー</h2><p style="margin:4px 0 0;color:#64748b;font-size:12px;line-height:1.6">登録済みメールだけCRMを利用できます。</p></div><button id="crmCloseUsers">閉じる</button></div><div class="crm-user-body"><div id="crmUserAddBox"></div><div id="crmUserList" class="crm-user-list"><div class="crm-user-empty">読み込み中...</div></div><div id="crmUserMsg" class="crm-user-msg"></div></div></div>
<script id="crm-admin-users-script">
(function(){
  var CURRENT_EMAIL=${currentEmailJson}; var CURRENT_ROLE=${currentRoleJson}; var ROOT_EMAIL=${rootEmailJson};
  function el(id){return document.getElementById(id)}
  function esc(v){return String(v==null?'':v).replace(/[&<>\"]/g,function(m){return {'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;'}[m]})}
  function setMsg(v,isErr){var m=el('crmUserMsg'); if(!m)return; m.className='crm-user-msg'+(isErr?' crm-user-error':''); m.textContent=v||''}
  async function api(path,opt){opt=opt||{}; opt.headers=opt.headers||{}; if(opt.body&&!opt.headers['content-type'])opt.headers['content-type']='application/json'; var r=await fetch(path,opt); var j=await r.json().catch(function(){return {ok:false,message:'JSON parse error'}}); if(!r.ok||j.ok===false)throw new Error(j.message||('HTTP '+r.status)); return j}
  function openModal(){el('crmUserBackdrop').classList.add('show');el('crmUserModal').classList.add('show');loadUsers()}
  function closeModal(){el('crmUserBackdrop').classList.remove('show');el('crmUserModal').classList.remove('show')}
  function renderAddBox(){var box=el('crmUserAddBox'); if(!box)return; if(CURRENT_ROLE!=='admin'&&CURRENT_ROLE!=='root_admin'){box.innerHTML='<div class="crm-user-empty">ユーザー追加・無効化は管理者のみ操作できます。</div>';return;} box.innerHTML='<div class="crm-user-add"><input id="crmNewUserEmail" type="email" placeholder="追加するGoogleメール"><select id="crmNewUserRole"><option value="viewer">viewer</option><option value="admin">admin</option></select><button class="primary" id="crmCreateUser">追加</button></div>'; var b=el('crmCreateUser'); if(b)b.onclick=addUser}
  function renderUsers(items){var list=el('crmUserList'); if(!list)return; if(!items||!items.length){list.innerHTML='<div class="crm-user-empty">管理ユーザーはまだありません。</div>';return;} list.innerHTML=items.map(function(u){var email=String(u.email||''); var status=String(u.status||''); var role=String(u.role||'viewer'); var root=email.toLowerCase()===ROOT_EMAIL.toLowerCase(); var disabled=status!=='active'; var canRemove=(CURRENT_ROLE==='admin'||CURRENT_ROLE==='root_admin')&&!root&&!disabled; return '<div class="crm-user-card"><div><div class="crm-user-email">'+esc(email)+'</div><div class="crm-user-meta"><span class="crm-badge">'+esc(role)+'</span><span class="crm-badge">'+esc(status)+'</span></div></div><div>'+(canRemove?'<button data-remove="'+esc(email)+'">無効化</button>':'')+'</div></div>'}).join(''); list.querySelectorAll('[data-remove]').forEach(function(b){b.onclick=function(){removeUser(b.getAttribute('data-remove'))}})}
  async function loadUsers(){try{renderAddBox();setMsg('読み込み中...');var j=await api('/api/admin-users');renderUsers(j.items||[]);setMsg('')}catch(e){setMsg(e.message,true)}}
  async function addUser(){try{var email=el('crmNewUserEmail').value;var role=el('crmNewUserRole').value;setMsg('追加中...');await api('/api/admin-users/add',{method:'POST',body:JSON.stringify({email:email,role:role})});el('crmNewUserEmail').value='';await loadUsers();setMsg('追加しました: '+email)}catch(e){setMsg(e.message,true)}}
  async function removeUser(email){if(!email)return; if(!confirm(email+' を無効化しますか？'))return; try{setMsg('無効化中...');await api('/api/admin-users/remove',{method:'POST',body:JSON.stringify({email:email})}); await loadUsers(); setMsg('無効化しました: '+email)}catch(e){setMsg(e.message,true)}}
  var open=el('crmOpenUsers'), close=el('crmCloseUsers'), bg=el('crmUserBackdrop'); if(open)open.onclick=openModal; if(close)close.onclick=closeModal; if(bg)bg.onclick=closeModal;
})();
</script>`;

  return body.replace("</body>", panel + "</body>");
}

async function callOriginal(request, env, ctx, current) {
  const token = text(env.ADMIN_TOKEN);
  if (!token) return json({ ok: false, message: "ADMIN_TOKEN is not configured" }, 503);
  const url = new URL(request.url);
  url.searchParams.delete("token");
  const headers = new Headers(request.headers);
  headers.set("x-admin-token", token);
  headers.set("authorization", `Bearer ${token}`);
  const init = { method: request.method, headers, redirect: "manual" };
  if (request.method !== "GET" && request.method !== "HEAD") init.body = request.body;
  const res = await app.fetch(new Request(url.toString(), init), env, ctx);
  const contentType = res.headers.get("content-type") || "";
  if (contentType.includes("text/html")) {
    let body = await res.text();
    body = body.replaceAll(token, "").replace(/\/admin\?token=[^'"\s<)]+/g, "/admin");
    return html(injectUserAdminUi(body, current), res.status);
  }
  return new Response(res.body, { status: res.status, statusText: res.statusText, headers: secureHeaders(res.headers) });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    if (request.method === "OPTIONS") return response(null, 204);
    if (path === "/robots.txt") return response("User-agent: *\nDisallow: /\n", 200, { "content-type": "text/plain; charset=utf-8" });
    if (path === "/" || path === "/health" || path === "/api/health") return json({ ok: true, service: "customer-crm-api", build: BUILD, hasDb: !!env.DB, secure: true, auth: "cloudflare-access-google" });
    if (hasUrlToken(url)) {
      if (path === "/admin") { url.search = ""; return Response.redirect(url.toString(), 302); }
      return json({ ok: false, message: "URL token authentication is disabled." }, 401);
    }
    if (path.startsWith("/api/sync/")) return await app.fetch(request, env, ctx);
    const current = await getCurrentUser(request, env);
    if (!current) {
      if (path === "/admin") return html(loginRequiredPage(), 401);
      return json({ ok: false, message: "Google login through Cloudflare Access is required" }, 401);
    }
    if (path === "/api/admin-users" && request.method === "GET") return json(await listAdminUsers(env));
    if (path === "/api/admin-users/add" && request.method === "POST") return json(await addAdminUser(request, env, current));
    if (path === "/api/admin-users/remove" && request.method === "POST") return json(await removeAdminUser(request, env, current));

    if (path === "/api/customers/delete" && request.method === "POST") return await softDeleteCustomers(request, env, current);
    if (path === "/api/customers/delete-test" && request.method === "POST") return await softDeleteCustomers(request, env, current, ["26000099", "CRM-LIVE-TEST-001"]);
    if (path === "/api/customers" && request.method === "GET") return json(await listActiveCustomers(env, url));
    if (path === "/api/segments/summary" && request.method === "GET") return json(await getActiveSegmentSummary(env));

    if (path.startsWith("/api/customers/") && request.method === "GET") {
      const customerId = decodeURIComponent(path.replace("/api/customers/", "").replace("/line-history", ""));
      const activeCustomer = await getActiveCustomerOrNull(env, customerId);
      if (!activeCustomer) return json({ ok: false, message: "customer not found" }, 404);
    }

    if (path === "/admin" || path.startsWith("/api/")) return await callOriginal(request, env, ctx, current);
    return json({ ok: false, message: "Not Found" }, 404);
  }
};
