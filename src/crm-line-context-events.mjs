// TASK-CRM-LINE-OUTBOUND-CONTEXT-001-DATA-01
// READ-only LINE context event APIs for reservation AI bridge.
// This module does not send LINE messages and does not create sent events.

const BUILD = "crm-line-context-events-20260814-01";
const TABLE = "customer_line_message_events";
const MAX_LIMIT = 100;
const DEFAULT_LIMIT = 20;

function text(v){ return String(v == null ? "" : v).trim(); }
function clampLimit(v){
  const n = Number(v || DEFAULT_LIMIT);
  if(!Number.isFinite(n) || n <= 0) return DEFAULT_LIMIT;
  return Math.min(Math.floor(n), MAX_LIMIT);
}
function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store, no-cache, must-revalidate, max-age=0",
      "x-crm-line-context-events-build": BUILD
    }
  });
}
function bearer(req){
  const h = text(req.headers.get("authorization"));
  return /^Bearer\s+/i.test(h) ? h.replace(/^Bearer\s+/i, "").trim() : "";
}
function internalAllowed(req, env){
  const supplied = text(req.headers.get("x-internal-token") || req.headers.get("x-admin-token") || bearer(req));
  const expected = text(env && (env.CRM_INTERNAL_TOKEN || env.INTERNAL_API_TOKEN || env.ADMIN_TOKEN));
  return !!expected && supplied === expected;
}
async function safeAll(env, sql, params = []){
  try{
    let stmt = env.DB.prepare(sql);
    if(params.length) stmt = stmt.bind(...params);
    return await stmt.all();
  }catch(e){ return { results: [], error: String(e && e.message || e) }; }
}
async function safeFirst(env, sql, params = []){
  try{
    let stmt = env.DB.prepare(sql);
    if(params.length) stmt = stmt.bind(...params);
    return await stmt.first();
  }catch(_){ return null; }
}
async function tableExists(env, table){
  const r = await safeFirst(env, "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", [table]);
  return !!r;
}
async function ensureSchema(env){
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS customer_line_message_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    customer_id TEXT NOT NULL,
    line_user_id TEXT NOT NULL,
    direction TEXT NOT NULL CHECK(direction IN ('incoming','outbound')),
    message_type TEXT NOT NULL DEFAULT 'text',
    message_text TEXT NOT NULL,
    source TEXT NOT NULL,
    send_status TEXT NOT NULL CHECK(send_status IN ('pending','sent','failed','received')),
    send_error TEXT,
    line_message_id TEXT,
    idempotency_key TEXT UNIQUE,
    sender_type TEXT,
    occurred_at TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    raw_json TEXT
  )`).run();
  await env.DB.prepare("CREATE INDEX IF NOT EXISTS idx_customer_line_message_events_customer_time ON customer_line_message_events(customer_id, occurred_at, id)").run();
  await env.DB.prepare("CREATE INDEX IF NOT EXISTS idx_customer_line_message_events_line_user_time ON customer_line_message_events(line_user_id, occurred_at, id)").run();
  await env.DB.prepare("CREATE INDEX IF NOT EXISTS idx_customer_line_message_events_context ON customer_line_message_events(line_user_id, direction, send_status, occurred_at)").run();
}
async function customerColumns(env){
  const r = await safeAll(env, "PRAGMA table_info(customers)");
  return new Set((r.results || []).map(x => x.name));
}
function pickLineUserId(row){
  return text(row && (row.line_user_id || row.lineUserId || row.line_id || row.line_uid || row.line_mid));
}
async function findCustomer(env, customerId){
  if(!await tableExists(env, "customers")) return { found:false, line_user_id:"", reason:"customers_table_missing" };
  const cols = await customerColumns(env);
  const wheres = [];
  const params = [];
  if(cols.has("id")){ wheres.push("CAST(id AS TEXT)=?"); params.push(customerId); }
  if(cols.has("customer_id")){ wheres.push("CAST(customer_id AS TEXT)=?"); params.push(customerId); }
  if(!wheres.length) return { found:false, line_user_id:"", reason:"customer_id_columns_missing" };
  const row = await safeFirst(env, `SELECT * FROM customers WHERE ${wheres.join(" OR ")} LIMIT 1`, params);
  if(!row) return { found:false, line_user_id:"", reason:"customer_not_found" };
  return { found:true, customer: row, customer_id: text(row.customer_id || row.id || customerId), line_user_id: pickLineUserId(row), reason:"" };
}
function formalContext(row){
  return row.direction === "outbound" && row.send_status === "sent";
}
function eventOut(row){
  return {
    event_id: text(row.event_id),
    customer_id: text(row.customer_id),
    line_user_id: text(row.line_user_id),
    direction: text(row.direction),
    message_type: text(row.message_type || "text"),
    message_text: text(row.message_text),
    source: text(row.source),
    send_status: text(row.send_status),
    line_message_id: text(row.line_message_id),
    sender_type: text(row.sender_type),
    occurred_at: text(row.occurred_at),
    created_at: text(row.created_at),
    formal_context: formalContext(row)
  };
}
async function listEvents(env, whereSql, params, limit){
  await ensureSchema(env);
  const r = await safeAll(env, `SELECT event_id, customer_id, line_user_id, direction, message_type, message_text, source, send_status, line_message_id, sender_type, occurred_at, created_at FROM ${TABLE} WHERE ${whereSql} ORDER BY COALESCE(occurred_at, created_at, '') DESC, id DESC LIMIT ${limit}`, params);
  return (r.results || []).map(eventOut);
}
async function handleCustomerEvents(req, env, customerId){
  if(req.method !== "GET") return json({ ok:false, error:"Method Not Allowed" }, 405);
  if(!env || !env.DB) return json({ ok:false, error:"D1 DB binding missing" }, 500);
  const url = new URL(req.url);
  const limit = clampLimit(url.searchParams.get("limit"));
  const customer = await findCustomer(env, customerId);
  if(!customer.found){
    return json({ ok:true, build:BUILD, customer_id:customerId, line_user_id:"", line_linked:false, reason:customer.reason, count:0, events:[] });
  }
  if(!customer.line_user_id){
    return json({ ok:true, build:BUILD, customer_id:customer.customer_id, line_user_id:"", line_linked:false, reason:"line_user_id_not_linked", count:0, events:[] });
  }
  const events = await listEvents(env, "line_user_id=?", [customer.line_user_id], limit);
  return json({ ok:true, build:BUILD, customer_id:customer.customer_id, line_user_id:customer.line_user_id, line_linked:true, limit, count:events.length, events });
}
async function handleInternalEvents(req, env){
  if(req.method !== "GET") return json({ ok:false, error:"Method Not Allowed" }, 405);
  if(!env || !env.DB) return json({ ok:false, error:"D1 DB binding missing" }, 500);
  if(!internalAllowed(req, env)) return json({ ok:false, error:"Internal auth unavailable or invalid" }, 401);
  const url = new URL(req.url);
  const lineUserId = text(url.searchParams.get("line_user_id"));
  const limit = clampLimit(url.searchParams.get("limit"));
  if(!lineUserId) return json({ ok:false, error:"line_user_id is required" }, 400);
  const events = await listEvents(env, "line_user_id=?", [lineUserId], limit);
  return json({ ok:true, build:BUILD, line_user_id:lineUserId, limit, count:events.length, events });
}
export async function handleLineContextEvents(req, env){
  const url = new URL(req.url);
  const customerMatch = url.pathname.match(/^\/api\/customers\/([^/]+)\/line-context-events$/);
  if(customerMatch) return handleCustomerEvents(req, env, decodeURIComponent(customerMatch[1]));
  if(url.pathname === "/api/internal/line-context-events") return handleInternalEvents(req, env);
  return null;
}
export async function lineContextHealth(env){
  let tablePresent = false;
  try{
    if(env && env.DB){ await ensureSchema(env); tablePresent = await tableExists(env, TABLE); }
  }catch(_){ tablePresent = false; }
  return {
    line_context_events_enabled: tablePresent,
    line_context_events_table: TABLE,
    line_context_sent_only_formal: true,
    line_live_send_enabled: false,
    line_context_build: BUILD
  };
}
