// ======================================================
// CUSTOMER CRM / GROWTH SUITE WRAPPER
// build: customer-crm-api-growth-suite-20260613-01
// Adds: duplicate warning, auto follow-up tasks, delivery progress, customer ranks, LINE templates.
// ======================================================

import app from "./production-index-crm-today-action-filters.js";

const BUILD = "customer-crm-api-growth-suite-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const READ_ROLES = ["root_admin", "admin", "staff", "viewer"];
const WRITE_ROLES = ["root_admin", "admin", "staff"];

function text(v) { return v === undefined || v === null ? "" : String(v).trim(); }
function lower(v) { return text(v).toLowerCase(); }
function num(v) { const n = Number(v || 0); return Number.isFinite(n) ? n : 0; }
function todayJst() { return new Date(Date.now() + 9 * 60 * 60 * 1000).toISOString().slice(0, 10); }
function isoNow() { return new Date().toISOString(); }
function addDays(dateText, days) {
  const base = text(dateText) || todayJst();
  const d = new Date(`${base.slice(0, 10)}T00:00:00+09:00`);
  d.setDate(d.getDate() + Number(days || 0));
  return new Date(d.getTime() + 9 * 60 * 60 * 1000).toISOString().slice(0, 10);
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
async function readJson(request) { try { return await request.json(); } catch (_) { return {}; } }
function getAccessEmail(request) {
  return lower(request.headers.get("cf-access-authenticated-user-email") || request.headers.get("Cf-Access-Authenticated-User-Email") || request.headers.get("x-user-email") || "");
}
async function addColumn(db, table, definition) { try { await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run(); } catch (_) {} }
async function safeAll(env, sql, bindings = []) {
  try { const s = env.DB.prepare(sql); const r = bindings.length ? await s.bind(...bindings).all() : await s.all(); return r.results || []; } catch (_) { return []; }
}
async function safeFirst(env, sql, bindings = []) {
  try { const s = env.DB.prepare(sql); return bindings.length ? await s.bind(...bindings).first() : await s.first(); } catch (_) { return null; }
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
    VALUES(?, 'admin', 'active', 'system', datetime('now'), datetime('now'))`).bind(ROOT_ADMIN_EMAIL).run();

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_line_templates (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT,
    genre TEXT,
    body TEXT NOT NULL,
    variables_json TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    usage_count INTEGER NOT NULL DEFAULT 0,
    created_by TEXT,
    updated_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    deleted_at TEXT,
    deleted_by TEXT
  )`).run();

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_reservation_progress (
    reservation_key TEXT PRIMARY KEY,
    crm_draft_id TEXT,
    reservation_app_reservation_id TEXT,
    customer_id TEXT,
    customer_name TEXT,
    genre TEXT,
    shoot_date TEXT,
    progress_status TEXT NOT NULL DEFAULT '予約確定',
    progress_step INTEGER NOT NULL DEFAULT 1,
    next_action TEXT,
    next_due_date TEXT,
    completed_at TEXT,
    created_by TEXT,
    updated_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    deleted_at TEXT,
    deleted_by TEXT
  )`).run();

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_reservation_duplicate_checks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    crm_draft_id TEXT,
    customer_id TEXT,
    customer_name TEXT,
    shoot_date TEXT,
    start_time TEXT,
    genre TEXT,
    duplicate_level TEXT,
    duplicate_count INTEGER DEFAULT 0,
    result_json TEXT,
    checked_by TEXT,
    checked_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_growth_suite_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action_type TEXT,
    target_type TEXT,
    target_id TEXT,
    customer_id TEXT,
    customer_name TEXT,
    result TEXT,
    detail_json TEXT,
    actor_email TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();

  for (const col of [
    "customer_rank TEXT", "customer_rank_reason TEXT", "customer_rank_updated_at TEXT",
    "delivery_progress_status TEXT", "delivery_progress_updated_at TEXT"
  ]) await addColumn(env.DB, "customers", col);
  for (const col of ["duplicate_checked_at TEXT", "duplicate_level TEXT", "followup_created_at TEXT", "delivery_progress_status TEXT", "rank_synced_at TEXT"]) {
    await addColumn(env.DB, "crm_reservation_drafts", col);
  }
  for (const col of ["delivery_progress_status TEXT", "progress_step INTEGER", "progress_updated_at TEXT"]) {
    await addColumn(env.DB, "customer_reservations", col);
  }
  await env.DB.prepare(`CREATE UNIQUE INDEX IF NOT EXISTS idx_crm_line_templates_name_active ON crm_line_templates(name, COALESCE(deleted_at,''))`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_progress_customer ON crm_reservation_progress(customer_id, progress_status, shoot_date)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_progress_draft ON crm_reservation_progress(crm_draft_id)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_dup_checks_draft ON crm_reservation_duplicate_checks(crm_draft_id, checked_at)`).run();
}

async function requireUser(request, env, roles = READ_ROLES) {
  await ensureSchema(env);
  const email = getAccessEmail(request);
  if (!email) return { ok: false, response: json({ ok: false, message: "Login required" }, 401) };
  const user = await env.DB.prepare(`SELECT email, role, status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`).bind(email).first();
  if (!user) return { ok: false, response: json({ ok: false, message: "User is not allowed" }, 403) };
  if (roles.length && !roles.includes(user.role || "")) return { ok: false, response: json({ ok: false, message: "Permission denied" }, 403) };
  return { ok: true, email, user };
}

function draftIdFromPath(path, tail) {
  const m = path.match(new RegExp(`^/api/reservation-drafts/([^/]+)/${tail}$`));
  return m ? decodeURIComponent(m[1]) : "";
}
function customerIdFromProgressPath(path) {
  const m = path.match(/^\/api\/customers\/([^/]+)\/reservation-progress$/);
  return m ? decodeURIComponent(m[1]) : "";
}
function templateIdFromPath(path) {
  const m = path.match(/^\/api\/line-templates\/([^/]+)$/);
  return m ? decodeURIComponent(m[1]) : "";
}

function timeToMinutes(v) {
  const s = text(v);
  const m = s.match(/^(\d{1,2}):(\d{2})/);
  if (!m) return null;
  return Number(m[1]) * 60 + Number(m[2]);
}
function normalizePlace(v) { return text(v).replace(/\s+/g, "").toLowerCase(); }
function samePlace(a, b) {
  const x = normalizePlace(a), y = normalizePlace(b);
  if (!x || !y) return false;
  return x === y || x.includes(y) || y.includes(x);
}
function duplicateScore(target, row) {
  let score = 0;
  const reasons = [];
  if (text(target.customer_id) && text(row.customer_id) && text(target.customer_id) === text(row.customer_id)) { score += 40; reasons.push("同じ顧客"); }
  if (text(target.customer_name) && text(row.customer_name) && lower(target.customer_name) === lower(row.customer_name)) { score += 25; reasons.push("同じ顧客名"); }
  if (text(target.shoot_date) && text(row.shoot_date) && text(target.shoot_date).slice(0,10) === text(row.shoot_date).slice(0,10)) { score += 35; reasons.push("同じ撮影日"); }
  const ta = timeToMinutes(target.start_time), tb = timeToMinutes(row.start_time);
  if (ta !== null && tb !== null) {
    const diff = Math.abs(ta - tb);
    if (diff === 0) { score += 35; reasons.push("同じ開始時間"); }
    else if (diff <= 60) { score += 20; reasons.push("開始時間が近い"); }
  }
  if (text(target.genre) && text(row.genre) && lower(target.genre) === lower(row.genre)) { score += 15; reasons.push("同じジャンル"); }
  if (samePlace(target.place, row.place)) { score += 10; reasons.push("場所が近い"); }
  let level = "none";
  if (score >= 100) level = "high";
  else if (score >= 75) level = "medium";
  else if (score >= 55) level = "low";
  return { score, level, reasons };
}
async function getDraft(env, draftId) {
  return await safeFirst(env, `SELECT * FROM crm_reservation_drafts WHERE id=? LIMIT 1`, [draftId]);
}
async function findDuplicates(env, target, excludeDraftId = "") {
  const rows = [];
  const bindings = [];
  let where = `WHERE COALESCE(deleted_at,'')=''`;
  if (text(target.shoot_date)) { where += ` AND substr(COALESCE(shoot_date,''),1,10)=?`; bindings.push(text(target.shoot_date).slice(0,10)); }
  const drafts = await safeAll(env, `SELECT id, customer_id, customer_name, genre, shoot_date, start_time, place, plan_label, total_amount, status, 'draft' AS source FROM crm_reservation_drafts ${where} LIMIT 200`, bindings);
  for (const d of drafts) if (text(d.id) !== text(excludeDraftId)) rows.push(d);
  const reservations = await safeAll(env, `SELECT id, event_key, reservation_id, customer_id, customer_name, genre, shoot_date, start_time, place, plan_label, total_amount, status, 'history' AS source FROM customer_reservations WHERE COALESCE(deleted_at,'')='' AND COALESCE(status,'') NOT IN ('cancelled','キャンセル') ${text(target.shoot_date) ? "AND substr(COALESCE(shoot_date,''),1,10)=?" : ""} LIMIT 200`, text(target.shoot_date) ? [text(target.shoot_date).slice(0,10)] : []);
  for (const r of reservations) rows.push(r);
  return rows.map((row) => ({ row, match: duplicateScore(target, row) })).filter((x) => x.match.level !== "none").sort((a,b) => b.match.score - a.match.score).slice(0, 20);
}
async function duplicateCheckApi(request, env, draftId = "") {
  const auth = await requireUser(request, env, READ_ROLES);
  if (!auth.ok) return auth.response;
  const url = new URL(request.url);
  let payload = {};
  if (request.method === "POST") payload = await readJson(request);
  let target = payload || {};
  if (draftId) target = await getDraft(env, draftId) || {};
  if (!target || (!target.shoot_date && !target.customer_id && !target.customer_name)) return json({ ok: false, message: "check target is empty" }, 400);
  const matches = await findDuplicates(env, target, draftId);
  const high = matches.filter((m) => m.match.level === "high").length;
  const medium = matches.filter((m) => m.match.level === "medium").length;
  const level = high ? "high" : medium ? "medium" : matches.length ? "low" : "none";
  await env.DB.prepare(`INSERT INTO crm_reservation_duplicate_checks(crm_draft_id, customer_id, customer_name, shoot_date, start_time, genre, duplicate_level, duplicate_count, result_json, checked_by, checked_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)`).bind(
      draftId || text(payload.crm_draft_id), text(target.customer_id), text(target.customer_name), text(target.shoot_date), text(target.start_time), text(target.genre), level, matches.length, JSON.stringify(matches), auth.email
    ).run();
  if (draftId) await env.DB.prepare(`UPDATE crm_reservation_drafts SET duplicate_checked_at=CURRENT_TIMESTAMP, duplicate_level=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`).bind(level, draftId).run();
  return json({ ok: true, build: BUILD, level, counts: { total: matches.length, high, medium, low: matches.filter((m)=>m.match.level==='low').length }, matches });
}

function rankCustomer(customer, stats = {}) {
  const revenue = num(stats.total_revenue || customer.total_revenue);
  const count = num(stats.repeat_count || customer.repeat_count);
  const last = text(stats.last_shoot_date || customer.last_shoot_date);
  const daysSince = last ? Math.floor((new Date(`${todayJst()}T00:00:00+09:00`) - new Date(`${last.slice(0,10)}T00:00:00+09:00`)) / 86400000) : null;
  if (revenue >= 100000) return { rank: "VIP", reason: "累計売上10万円以上" };
  if (count >= 2) return { rank: "リピーター", reason: "撮影2回以上" };
  if (daysSince !== null && daysSince >= 180) return { rank: "休眠", reason: "最終撮影から180日以上" };
  if (count <= 1 && revenue > 0) return { rank: "新規", reason: "初回予約あり" };
  return { rank: "要フォロー", reason: "予約・売上情報が不足、または次回提案が必要" };
}
async function updateCustomerStatsAndRank(env, customerId) {
  const stats = await safeFirst(env, `SELECT COUNT(*) AS repeat_count, COALESCE(SUM(COALESCE(total_amount,0)),0) AS total_revenue, MAX(shoot_date) AS last_shoot_date,
      GROUP_CONCAT(DISTINCT genre) AS genre_history
    FROM customer_reservations
    WHERE customer_id=? AND COALESCE(deleted_at,'')='' AND COALESCE(status,'') NOT IN ('cancelled','キャンセル')`, [customerId]) || {};
  const customer = await safeFirst(env, `SELECT * FROM customers WHERE id=? LIMIT 1`, [customerId]) || {};
  const rank = rankCustomer(customer, stats);
  await env.DB.prepare(`UPDATE customers SET repeat_count=?, total_revenue=?, last_shoot_date=?, genre_history=?, customer_rank=?, customer_rank_reason=?, customer_rank_updated_at=CURRENT_TIMESTAMP WHERE id=?`)
    .bind(num(stats.repeat_count), num(stats.total_revenue), text(stats.last_shoot_date), text(stats.genre_history), rank.rank, rank.reason, customerId).run();
  return { ...stats, ...rank };
}
async function customerRankApi(request, env) {
  const auth = await requireUser(request, env, READ_ROLES);
  if (!auth.ok) return auth.response;
  const url = new URL(request.url);
  const limit = Math.min(Math.max(num(url.searchParams.get("limit") || 200), 1), 1000);
  const rows = await safeAll(env, `SELECT id, name, kana, phone, email, repeat_count, total_revenue, last_shoot_date, customer_rank, customer_rank_reason, customer_rank_updated_at FROM customers WHERE COALESCE(deleted_at,'')='' ORDER BY COALESCE(total_revenue,0) DESC, COALESCE(repeat_count,0) DESC LIMIT ?`, [limit]);
  return json({ ok: true, build: BUILD, items: rows, counts: rows.reduce((a,r)=>{ const k=text(r.customer_rank||"未判定"); a[k]=(a[k]||0)+1; return a; }, {}) });
}
async function recalcRanksApi(request, env) {
  const auth = await requireUser(request, env, WRITE_ROLES);
  if (!auth.ok) return auth.response;
  const rows = await safeAll(env, `SELECT id FROM customers WHERE COALESCE(deleted_at,'')='' LIMIT 1000`);
  let updated = 0;
  for (const row of rows) { await updateCustomerStatsAndRank(env, row.id); updated++; }
  return json({ ok: true, updated });
}

const PROGRESS_STEPS = [
  { status: "予約確定", step: 1, next_action: "撮影前連絡", offset: -2 },
  { status: "撮影前連絡済み", step: 2, next_action: "撮影完了登録", offset: 0 },
  { status: "撮影完了", step: 3, next_action: "先行納品", offset: 7 },
  { status: "先行納品済み", step: 4, next_action: "本納品", offset: 60 },
  { status: "本納品準備中", step: 5, next_action: "本納品", offset: 60 },
  { status: "本納品済み", step: 6, next_action: "口コミ依頼", offset: 7 },
  { status: "口コミ依頼済み", step: 7, next_action: "完了", offset: 0 },
  { status: "完了", step: 8, next_action: "", offset: 0 }
];
function progressInfo(status, shootDate) {
  const s = text(status) || "予約確定";
  const p = PROGRESS_STEPS.find((x) => x.status === s) || PROGRESS_STEPS[0];
  return { ...p, next_due_date: p.next_action ? addDays(shootDate || todayJst(), p.offset) : "" };
}
function progressKey(draft) { return text(draft.reservation_app_reservation_id || draft.history_event_key || `draft-${draft.id}`); }
async function upsertProgress(env, draft, actorEmail) {
  const info = progressInfo(draft.delivery_progress_status || "予約確定", draft.shoot_date);
  const key = progressKey(draft);
  await env.DB.prepare(`INSERT INTO crm_reservation_progress(reservation_key, crm_draft_id, reservation_app_reservation_id, customer_id, customer_name, genre, shoot_date, progress_status, progress_step, next_action, next_due_date, created_by, updated_by, created_at, updated_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
    ON CONFLICT(reservation_key) DO UPDATE SET crm_draft_id=excluded.crm_draft_id, reservation_app_reservation_id=excluded.reservation_app_reservation_id, customer_id=excluded.customer_id, customer_name=excluded.customer_name, genre=excluded.genre, shoot_date=excluded.shoot_date, progress_status=excluded.progress_status, progress_step=excluded.progress_step, next_action=excluded.next_action, next_due_date=excluded.next_due_date, updated_by=excluded.updated_by, updated_at=CURRENT_TIMESTAMP`)
    .bind(key, text(draft.id), text(draft.reservation_app_reservation_id), text(draft.customer_id), text(draft.customer_name), text(draft.genre), text(draft.shoot_date), info.status, info.step, info.next_action, info.next_due_date, actorEmail || "system", actorEmail || "system").run();
  await env.DB.prepare(`UPDATE crm_reservation_drafts SET delivery_progress_status=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`).bind(info.status, draft.id).run();
  await env.DB.prepare(`UPDATE customer_reservations SET delivery_progress_status=?, progress_step=?, progress_updated_at=CURRENT_TIMESTAMP WHERE customer_id=? AND (reservation_id=? OR event_key=?)`)
    .bind(info.status, info.step, text(draft.customer_id), text(draft.reservation_app_reservation_id), text(draft.history_event_key)).run();
  return { reservation_key: key, ...info };
}
async function progressListApi(request, env, customerId = "") {
  const auth = await requireUser(request, env, READ_ROLES);
  if (!auth.ok) return auth.response;
  const url = new URL(request.url);
  const status = text(url.searchParams.get("status"));
  const bindings = [];
  let where = `WHERE COALESCE(deleted_at,'')=''`;
  if (customerId) { where += ` AND customer_id=?`; bindings.push(customerId); }
  if (status) { where += ` AND progress_status=?`; bindings.push(status); }
  const rows = await safeAll(env, `SELECT * FROM crm_reservation_progress ${where} ORDER BY date(COALESCE(next_due_date,'2999-12-31')) ASC, datetime(updated_at) DESC LIMIT 300`, bindings);
  return json({ ok: true, build: BUILD, items: rows, statuses: PROGRESS_STEPS.map(x=>x.status) });
}
async function progressUpdateApi(request, env, key) {
  const auth = await requireUser(request, env, WRITE_ROLES);
  if (!auth.ok) return auth.response;
  const body = await readJson(request);
  const status = text(body.status || body.progress_status || "");
  if (!status) return json({ ok: false, message: "status is required" }, 400);
  const current = await safeFirst(env, `SELECT * FROM crm_reservation_progress WHERE reservation_key=? LIMIT 1`, [key]);
  if (!current) return json({ ok: false, message: "progress not found" }, 404);
  const info = progressInfo(status, current.shoot_date);
  await env.DB.prepare(`UPDATE crm_reservation_progress SET progress_status=?, progress_step=?, next_action=?, next_due_date=?, completed_at=?, updated_by=?, updated_at=CURRENT_TIMESTAMP WHERE reservation_key=?`)
    .bind(info.status, info.step, info.next_action, info.next_due_date, info.status === "完了" ? isoNow() : text(current.completed_at), auth.email, key).run();
  await env.DB.prepare(`UPDATE customer_reservations SET delivery_progress_status=?, progress_step=?, progress_updated_at=CURRENT_TIMESTAMP WHERE customer_id=? AND (reservation_id=? OR event_key=?)`)
    .bind(info.status, info.step, text(current.customer_id), text(current.reservation_app_reservation_id), key).run();
  await env.DB.prepare(`UPDATE crm_reservation_drafts SET delivery_progress_status=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`).bind(info.status, text(current.crm_draft_id)).run();
  return json({ ok: true, reservation_key: key, progress: info });
}

async function createFollowTask(env, customerId, customerName, type, title, dueDate, priority, createdBy, sourceKey) {
  const exists = await safeFirst(env, `SELECT id FROM crm_follow_tasks WHERE customer_id=? AND title=? AND due_date=? AND COALESCE(status,'') NOT IN ('completed','done','完了') LIMIT 1`, [customerId, title, dueDate]);
  if (exists) return { skipped: true, id: exists.id };
  const id = `ft-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  await env.DB.prepare(`INSERT INTO crm_follow_tasks(id, customer_id, customer_name, task_type, title, due_date, priority, status, memo, created_by, created_at, updated_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`).bind(id, customerId, customerName, type, title, dueDate, priority, "open", `自動作成: ${sourceKey || "reservation"}`, createdBy || "system").run();
  return { skipped: false, id };
}
async function createPostShootFollowups(env, draft, actorEmail) {
  const customerId = text(draft.customer_id);
  if (!customerId || !text(draft.shoot_date)) return { ok: false, skipped: true, reason: "customer_id or shoot_date is empty" };
  if (draft.followup_created_at) return { ok: true, skipped: true, reason: "already created" };
  const customerName = text(draft.customer_name);
  const shootDate = text(draft.shoot_date).slice(0, 10);
  const sourceKey = text(draft.reservation_app_reservation_id || draft.id);
  const tasks = [
    ["thanks", "撮影翌日：お礼LINE", addDays(shootDate, 1), "high"],
    ["preview", "撮影7日後：先行納品案内", addDays(shootDate, 7), "medium"],
    ["review", "撮影30日後：口コミ依頼", addDays(shootDate, 30), "medium"],
    ["repeat_offer", "撮影90日後：次回提案", addDays(shootDate, 90), "low"]
  ];
  const results = [];
  for (const [type, title, due, pri] of tasks) results.push(await createFollowTask(env, customerId, customerName, type, title, due, pri, actorEmail, sourceKey));
  await env.DB.prepare(`UPDATE crm_reservation_drafts SET followup_created_at=CURRENT_TIMESTAMP WHERE id=?`).bind(draft.id).run();
  return { ok: true, created: results.filter(x=>!x.skipped).length, results };
}

async function afterReservationCreated(request, env, draftId, baseResponse) {
  const actor = getAccessEmail(request) || "system";
  const draft = await getDraft(env, draftId);
  if (!draft) return;
  try {
    await duplicateCheckApi(new Request(request.url, { method: "POST", headers: request.headers, body: JSON.stringify({}) }), env, draftId);
  } catch (_) {}
  try { await createPostShootFollowups(env, draft, actor); } catch (_) {}
  try { await upsertProgress(env, draft, actor); } catch (_) {}
  try { if (draft.customer_id) await updateCustomerStatsAndRank(env, draft.customer_id); } catch (_) {}
}

async function templatesListApi(request, env) {
  const auth = await requireUser(request, env, READ_ROLES);
  if (!auth.ok) return auth.response;
  const url = new URL(request.url);
  const category = text(url.searchParams.get("category"));
  const genre = text(url.searchParams.get("genre"));
  const bindings = [];
  let where = `WHERE COALESCE(deleted_at,'')='' AND status='active'`;
  if (category) { where += ` AND category=?`; bindings.push(category); }
  if (genre) { where += ` AND (genre=? OR COALESCE(genre,'')='')`; bindings.push(genre); }
  const rows = await safeAll(env, `SELECT * FROM crm_line_templates ${where} ORDER BY category, name LIMIT 300`, bindings);
  return json({ ok: true, build: BUILD, items: rows });
}
async function templateSaveApi(request, env, templateId = "") {
  const auth = await requireUser(request, env, WRITE_ROLES);
  if (!auth.ok) return auth.response;
  const body = await readJson(request);
  const id = templateId || text(body.id) || `tpl-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const name = text(body.name);
  const templateBody = text(body.body || body.template_body);
  if (!name || !templateBody) return json({ ok: false, message: "name and body are required" }, 400);
  await env.DB.prepare(`INSERT INTO crm_line_templates(id, name, category, genre, body, variables_json, status, created_by, updated_by, created_at, updated_at)
    VALUES(?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
    ON CONFLICT(id) DO UPDATE SET name=excluded.name, category=excluded.category, genre=excluded.genre, body=excluded.body, variables_json=excluded.variables_json, status=excluded.status, updated_by=excluded.updated_by, updated_at=CURRENT_TIMESTAMP`)
    .bind(id, name, text(body.category || "汎用"), text(body.genre || ""), templateBody, text(body.variables_json || ""), text(body.status || "active"), auth.email, auth.email).run();
  return json({ ok: true, id });
}
async function templateDeleteApi(request, env, templateId) {
  const auth = await requireUser(request, env, WRITE_ROLES);
  if (!auth.ok) return auth.response;
  await env.DB.prepare(`UPDATE crm_line_templates SET deleted_at=CURRENT_TIMESTAMP, deleted_by=?, status='deleted', updated_at=CURRENT_TIMESTAMP WHERE id=?`).bind(auth.email, templateId).run();
  return json({ ok: true, id: templateId });
}
function renderTemplate(body, data) {
  const map = data || {};
  return text(body).replace(/\{\{\s*([a-zA-Z0-9_\-.]+)\s*\}\}/g, (_, key) => text(map[key] || ""));
}
async function templateRenderApi(request, env, templateId) {
  const auth = await requireUser(request, env, READ_ROLES);
  if (!auth.ok) return auth.response;
  const body = await readJson(request);
  const tpl = await safeFirst(env, `SELECT * FROM crm_line_templates WHERE id=? AND COALESCE(deleted_at,'')='' LIMIT 1`, [templateId]);
  if (!tpl) return json({ ok: false, message: "template not found" }, 404);
  const rendered = renderTemplate(tpl.body, body.data || body.customer || {});
  await env.DB.prepare(`UPDATE crm_line_templates SET usage_count=COALESCE(usage_count,0)+1, updated_at=CURRENT_TIMESTAMP WHERE id=?`).bind(templateId).run();
  return json({ ok: true, id: templateId, rendered });
}
async function seedTemplatesApi(request, env) {
  const auth = await requireUser(request, env, WRITE_ROLES);
  if (!auth.ok) return auth.response;
  const seeds = [
    ["tpl-thanks", "撮影翌日：お礼", "撮影後", "", "{{customer_name}}様\n昨日は撮影ありがとうございました。ご家族の大切な時間をご一緒できて嬉しかったです。先行納品まで楽しみにお待ちください。"],
    ["tpl-preview", "先行納品案内", "納品", "", "{{customer_name}}様\n先行カットの準備ができました。まずは数枚お送りしますので、ご確認ください。"],
    ["tpl-review", "口コミ依頼", "撮影後", "", "{{customer_name}}様\n撮影のご感想をいただけると励みになります。よろしければ一言だけでもお願いいたします。"],
    ["tpl-repeat", "次回提案", "リピート", "", "{{customer_name}}様\n前回の撮影から少し経ちましたので、季節の記念やご家族写真もおすすめです。気になる時期があればお気軽にご相談ください。"]
  ];
  for (const s of seeds) {
    await env.DB.prepare(`INSERT OR IGNORE INTO crm_line_templates(id,name,category,genre,body,status,created_by,updated_by,created_at,updated_at) VALUES(?,?,?,?,?,'active',?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`).bind(s[0], s[1], s[2], s[3], s[4], auth.email, auth.email).run();
  }
  return json({ ok: true, inserted: seeds.length });
}

function injectGrowthUi(html) {
  if (!html || html.includes("crmGrowthSuiteScript")) return html;
  const style = `<style id="crmGrowthSuiteStyle">
.crm-growth-panel{margin:10px auto 18px;max-width:1180px;border:1px solid #bbf7d0;background:linear-gradient(135deg,#f0fdf4,#ffffff);border-radius:18px;padding:12px;box-shadow:0 12px 30px rgba(22,163,74,.08);font-family:inherit;color:#052e16}.crm-growth-head{display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap}.crm-growth-title{font-size:17px;font-weight:950;margin:0}.crm-growth-sub{font-size:12px;color:#166534;margin:4px 0 0}.crm-growth-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}.crm-growth-actions button{border:0;background:#16a34a;color:#fff;border-radius:10px;padding:8px 10px;font-size:12px;font-weight:950;cursor:pointer}.crm-growth-grid{display:grid;grid-template-columns:repeat(5,minmax(130px,1fr));gap:8px;margin-top:10px}.crm-growth-card{background:#fff;border:1px solid #dcfce7;border-radius:14px;padding:10px}.crm-growth-card b{display:block;font-size:20px}.crm-growth-card span{font-size:11px;color:#166534;font-weight:900}.crm-growth-list{display:grid;grid-template-columns:repeat(2,1fr);gap:8px;margin-top:10px}.crm-growth-row{background:#fff;border:1px solid #dcfce7;border-radius:14px;padding:10px;font-size:12px}.crm-growth-row b{font-weight:950}.crm-growth-row small{display:block;color:#64748b;margin-top:3px;line-height:1.45}.crm-growth-row button{border:0;background:#16a34a;color:#fff;border-radius:9px;padding:6px 8px;font-size:11px;font-weight:900;margin-top:7px;cursor:pointer}.crm-growth-empty{background:#fff;border:1px dashed #86efac;border-radius:14px;padding:12px;font-size:13px;color:#166534;margin-top:10px}@media(max-width:860px){.crm-growth-panel{margin:10px}.crm-growth-grid{grid-template-columns:repeat(2,1fr)}.crm-growth-list{grid-template-columns:1fr}}
</style>`;
  const script = `<script id="crmGrowthSuiteScript">
(function(){
 if(window.__crmGrowthSuiteInstalled)return; window.__crmGrowthSuiteInstalled=true;
 function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
 function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
 function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:1000007;background:#166534;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900;box-shadow:0 10px 30px rgba(0,0,0,.18)';document.body.appendChild(d);setTimeout(function(){d.remove()},2600)}
 function panel(){return '<section id="crmGrowthPanel" class="crm-growth-panel"><div class="crm-growth-head"><div><h2 class="crm-growth-title">CRM実務強化パネル</h2><p class="crm-growth-sub">重複警告・撮影後フォロー・納品進捗・顧客ランク・LINEテンプレをまとめて確認できます。</p></div><div class="crm-growth-actions"><button id="crmGrowthLoad">更新</button><button id="crmGrowthRanks">ランク再計算</button><button id="crmGrowthSeedTpl">テンプレ初期作成</button><button id="crmGrowthTpl">テンプレ一覧</button></div></div><div id="crmGrowthKpis" class="crm-growth-grid"></div><div id="crmGrowthList" class="crm-growth-list"><div class="crm-growth-empty">読み込み中...</div></div></section>'}
 function kpis(data){var c=data.counts||{};var a=[['duplicate_high','重複高リスク'],['progress_open','納品未完了'],['rank_vip','VIP'],['rank_repeat','リピーター'],['templates','テンプレ']];return a.map(function(x){return '<div class="crm-growth-card"><b>'+esc(c[x[0]]||0)+'</b><span>'+x[1]+'</span></div>'}).join('')}
 function row(x){return '<div class="crm-growth-row"><b>'+esc(x.title||'-')+'</b><small>'+esc(x.meta||'')+'</small>'+(x.action?'<button data-growth-action="'+esc(x.action)+'">'+esc(x.action_label||'実行')+'</button>':'')+'</div>'}
 async function load(){var kp=document.getElementById('crmGrowthKpis'),list=document.getElementById('crmGrowthList');try{var d=await api('/api/growth-suite/overview');if(!d.ok)throw new Error(d.message||'load failed');if(kp)kp.innerHTML=kpis(d);var rows=(d.items||[]);if(list)list.innerHTML=rows.length?rows.map(row).join(''):'<div class="crm-growth-empty">今すぐ確認する項目はありません。</div>';}catch(e){if(list)list.innerHTML='<div class="crm-growth-empty">読み込み失敗：'+esc(e.message||e)+'</div>';}}
 async function run(url){if(!confirm('実行しますか？'))return;var d=await api(url,{method:'POST',body:'{}'});if(!d.ok){toast('失敗：'+(d.message||d.status||'unknown'));return;}toast('完了しました');load();}
 async function showTpl(){var d=await api('/api/line-templates');if(!d.ok){toast('テンプレ取得失敗');return;}alert((d.items||[]).map(function(t){return '['+(t.category||'-')+'] '+t.name+'\n'+t.body}).join('\n\n')||'テンプレはまだありません');}
 function install(){if(document.getElementById('crmGrowthPanel'))return;var base=document.getElementById('crmTodayFilterPanel')||document.getElementById('crmTodayActionPanel')||document.getElementById('crmTodayDashboard');if(base)base.insertAdjacentHTML('afterend',panel());else (document.querySelector('main')||document.body).insertAdjacentHTML('afterbegin',panel());load();}
 document.addEventListener('click',function(e){var t=e.target;if(!t)return;if(t.id==='crmGrowthLoad')load();if(t.id==='crmGrowthRanks')run('/api/customer-ranks/recalculate');if(t.id==='crmGrowthSeedTpl')run('/api/line-templates/seed');if(t.id==='crmGrowthTpl')showTpl();var u=t.getAttribute&&t.getAttribute('data-growth-action');if(u)run(u);});
 document.addEventListener('DOMContentLoaded',install);setTimeout(install,1400);setInterval(load,180000);
})();
</script>`;
  return html.replace("</head>", `${style}</head>`).replace("</body>", `${script}</body>`);
}

async function overviewApi(request, env) {
  const auth = await requireUser(request, env, READ_ROLES);
  if (!auth.ok) return auth.response;
  const dup = await safeFirst(env, `SELECT COUNT(*) AS high FROM crm_reservation_duplicate_checks WHERE duplicate_level='high' AND checked_at >= datetime('now','-30 days')`, []);
  const progressOpen = await safeFirst(env, `SELECT COUNT(*) AS n FROM crm_reservation_progress WHERE COALESCE(deleted_at,'')='' AND progress_status!='完了'`, []);
  const rankRows = await safeAll(env, `SELECT customer_rank, COUNT(*) AS n FROM customers WHERE COALESCE(deleted_at,'')='' GROUP BY customer_rank`, []);
  const tpl = await safeFirst(env, `SELECT COUNT(*) AS n FROM crm_line_templates WHERE COALESCE(deleted_at,'')='' AND status='active'`, []);
  const progress = await safeAll(env, `SELECT reservation_key, customer_name, genre, shoot_date, progress_status, next_action, next_due_date FROM crm_reservation_progress WHERE COALESCE(deleted_at,'')='' AND progress_status!='完了' ORDER BY date(COALESCE(next_due_date,'2999-12-31')) ASC LIMIT 6`, []);
  const ranks = Object.fromEntries(rankRows.map(r => [text(r.customer_rank || '未判定'), num(r.n)]));
  const items = progress.map(p => ({ title: `${p.customer_name || '-'} / ${p.progress_status}`, meta: `${p.genre || '-'} ${p.shoot_date || '-'} / 次: ${p.next_action || '-'} ${p.next_due_date || ''}`, action: `/api/reservation-progress/${encodeURIComponent(p.reservation_key)}`, action_label: '進捗更新' }));
  return json({ ok: true, build: BUILD, counts: { duplicate_high: num(dup && dup.high), progress_open: num(progressOpen && progressOpen.n), rank_vip: num(ranks.VIP), rank_repeat: num(ranks['リピーター']), templates: num(tpl && tpl.n) }, items });
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    if ((path === "/" || path === "/health" || path === "/api/health") && request.method === "GET") {
      return json({ ok: true, service: "customer-crm-api", build: BUILD, time: isoNow() });
    }

    if (path === "/api/growth-suite/overview" && request.method === "GET") return overviewApi(request, env);
    if (path === "/api/reservation-duplicates/check" && request.method === "POST") return duplicateCheckApi(request, env);
    const dupDraftId = draftIdFromPath(path, "duplicate-check");
    if (dupDraftId && request.method === "POST") return duplicateCheckApi(request, env, dupDraftId);
    if (path === "/api/customer-ranks" && request.method === "GET") return customerRankApi(request, env);
    if (path === "/api/customer-ranks/recalculate" && request.method === "POST") return recalcRanksApi(request, env);
    if (path === "/api/reservation-progress" && request.method === "GET") return progressListApi(request, env);
    const progressCustomer = customerIdFromProgressPath(path);
    if (progressCustomer && request.method === "GET") return progressListApi(request, env, progressCustomer);
    const progressMatch = path.match(/^\/api\/reservation-progress\/([^/]+)$/);
    if (progressMatch && request.method === "POST") return progressUpdateApi(request, env, decodeURIComponent(progressMatch[1]));
    if (path === "/api/line-templates" && request.method === "GET") return templatesListApi(request, env);
    if (path === "/api/line-templates" && request.method === "POST") return templateSaveApi(request, env);
    if (path === "/api/line-templates/seed" && request.method === "POST") return seedTemplatesApi(request, env);
    const templateId = templateIdFromPath(path);
    if (templateId && request.method === "PUT") return templateSaveApi(request, env, templateId);
    if (templateId && request.method === "DELETE") return templateDeleteApi(request, env, templateId);
    const renderMatch = path.match(/^\/api\/line-templates\/([^/]+)\/render$/);
    if (renderMatch && request.method === "POST") return templateRenderApi(request, env, decodeURIComponent(renderMatch[1]));

    const createdDraftId = draftIdFromPath(path, "mark-created-from-reservation");
    if (createdDraftId && request.method === "POST") {
      const res = await app.fetch(request, env, ctx);
      if (res.status >= 200 && res.status < 300) ctx && ctx.waitUntil && ctx.waitUntil(afterReservationCreated(request, env, createdDraftId, res.clone()));
      return res;
    }

    const res = await app.fetch(request, env, ctx);
    const type = res.headers.get("content-type") || "";
    if (type.includes("text/html")) {
      const body = injectGrowthUi(await res.text());
      return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }
    return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
