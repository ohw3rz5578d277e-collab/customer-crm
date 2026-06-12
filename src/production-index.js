// ======================================================
// CUSTOMER CRM API / PRODUCTION SAFETY WRAPPER
// build: customer-crm-api-production-wrapper-20260612-01
// ======================================================

import secureApp from "./secure-index.js";

const BUILD = "customer-crm-api-production-wrapper-20260612-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const ADMIN_ROLES = ["admin", "root_admin"];

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

function isManager(current) {
  return current && ADMIN_ROLES.includes(current.role);
}

async function ensureSoftDeleteSchema(env) {
  if (!env.DB) throw new Error("D1 DB binding(DB) is missing");
  for (const table of ["customers", "customer_reservations", "customer_items", "customer_timeline", "customer_line_messages", "customer_tags"]) {
    await addColumn(env.DB, table, "deleted_at TEXT");
    await addColumn(env.DB, table, "deleted_by TEXT");
    await addColumn(env.DB, table, "delete_reason TEXT");
  }
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

async function listDeletedCustomers(env, url) {
  await ensureSoftDeleteSchema(env);
  const keyword = text(url.searchParams.get("keyword"));
  const limitRaw = parseInt(url.searchParams.get("limit") || "100", 10);
  const limit = Math.max(1, Math.min(Number.isFinite(limitRaw) ? limitRaw : 100, 500));
  const where = ["deleted_at IS NOT NULL", "deleted_at <> ''"];
  const params = [];

  if (keyword) {
    const like = `%${keyword}%`;
    where.push(`(name LIKE ? OR furigana LIKE ? OR line_display_name LIKE ? OR phone LIKE ? OR email LIKE ? OR customer_id LIKE ? OR memo LIKE ?)`);
    params.push(like, like, like, like, like, like, like);
  }

  params.push(limit);
  const rs = await env.DB.prepare(`${customerSelectSql()} WHERE ${where.join(" AND ")} ORDER BY deleted_at DESC, updated_at DESC LIMIT ?`)
    .bind(...params)
    .all();

  return { ok: true, count: (rs.results || []).length, items: rs.results || [] };
}

async function restoreCustomers(request, env, current) {
  if (!isManager(current)) return json({ ok: false, message: "Only admin can restore customers" }, 403);
  await ensureSoftDeleteSchema(env);

  const body = await readJson(request);
  const rawIds = body.customer_ids || body.customer_id || body.ids || [];
  const ids = (Array.isArray(rawIds) ? rawIds : [rawIds]).map(text).filter(Boolean);
  if (!ids.length) return json({ ok: false, message: "customer_ids required" }, 400);

  for (const id of ids) {
    await env.DB.prepare(`UPDATE customers SET deleted_at=NULL, deleted_by=NULL, delete_reason=NULL, updated_at=datetime('now') WHERE customer_id=?`).bind(id).run();
    await env.DB.prepare(`UPDATE customer_reservations SET deleted_at=NULL, deleted_by=NULL, delete_reason=NULL, updated_at=datetime('now') WHERE customer_id=?`).bind(id).run();
    await env.DB.prepare(`UPDATE customer_items SET deleted_at=NULL, deleted_by=NULL, delete_reason=NULL, updated_at=datetime('now') WHERE customer_id=?`).bind(id).run();
    await env.DB.prepare(`UPDATE customer_timeline SET deleted_at=NULL, deleted_by=NULL, delete_reason=NULL WHERE customer_id=?`).bind(id).run();
    await env.DB.prepare(`UPDATE customer_line_messages SET deleted_at=NULL, deleted_by=NULL, delete_reason=NULL WHERE customer_id=?`).bind(id).run();
    await env.DB.prepare(`UPDATE customer_tags SET deleted_at=NULL, deleted_by=NULL, delete_reason=NULL WHERE customer_id=?`).bind(id).run();
  }

  return json({ ok: true, mode: "restore", restored: ids.length, customer_ids: ids, restored_by: current.email });
}

function hideDevControls(html) {
  if (!html || !html.includes("</head>")) return html;
  const style = `<style id="crm-production-safe-controls">.header .danger{display:none!important;visibility:hidden!important;pointer-events:none!important}</style>`;
  return html.replace("</head>", style + "</head>");
}

function hasLegacyQuery(url) {
  const key = ["to", "ken"].join("");
  return url.searchParams.has(key);
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);

    if (url.pathname === "/" || url.pathname === "/health" || url.pathname === "/api/health") {
      return json({ ok: true, service: "customer-crm-api", build: BUILD, hasDb: !!env.DB, secure: true, auth: "cloudflare-access-google" });
    }

    if (hasLegacyQuery(url)) return json({ ok: false, message: "Legacy query auth is disabled." }, 401);
    if (!env.ADMIN_TOKEN) return json({ ok: false, message: "Required admin setting is missing" }, 503);
    if (url.pathname.startsWith("/api/sync/") && !env.SYNC_TOKEN) return json({ ok: false, message: "Required sync setting is missing" }, 503);

    if (url.pathname === "/api/customers/deleted" || url.pathname === "/api/customers/restore") {
      const current = await getCurrentUser(request, env);
      if (!current) return json({ ok: false, message: "Google login through Cloudflare Access is required" }, 401);
      if (!isManager(current)) return json({ ok: false, message: "Only admin can manage deleted customers" }, 403);
      if (url.pathname === "/api/customers/deleted" && request.method === "GET") return json(await listDeletedCustomers(env, url));
      if (url.pathname === "/api/customers/restore" && request.method === "POST") return await restoreCustomers(request, env, current);
      return json({ ok: false, message: "Method Not Allowed" }, 405);
    }

    const res = await secureApp.fetch(request, env, ctx);
    const contentType = res.headers.get("content-type") || "";

    if (!contentType.includes("text/html")) {
      return new Response(res.body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
    }

    const body = hideDevControls(await res.text());
    return new Response(body, { status: res.status, statusText: res.statusText, headers: securityHeaders(res.headers) });
  }
};
