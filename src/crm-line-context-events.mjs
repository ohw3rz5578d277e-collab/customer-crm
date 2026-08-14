// TASK-CRM-LINE-OUTBOUND-CONTEXT-001-DATA-02
// READ-only LINE context event APIs for reservation AI bridge.
// This module does not send LINE messages, does not create sent events,
// and does not run DDL. Schema is managed only by migrations.

const BUILD = "crm-line-context-events-20260814-02";
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
function accessEmail(req){
  return text(
    req.headers.get("cf-access-authenticated-user-email") ||
    req.headers.get("Cf-Access-Authenticated-User-Email") ||
    req.headers.get("cf-access-user-email") ||
    ""
  );
}
function bearer(req){
  const h = text(req.headers.get("authorization"));
  return /^Bearer\s+/i.test(h) ? h.replace(/^Bearer\s+/i, "").trim() : "";
}
function internalAllowed(req, env){
  const supplied = text(req.headers.get("x-internal-token") || bearer(req));
  const expected = text(env && env.CRM_INTERNAL_TOKEN);
  return !!expected && supplied === expected;
}
async function dbAll(env, sql, params = []){
  try{
    let stmt = env.DB.prepare(sql);
    if(params.length) stmt = stmt.bind(...params);
    const result = await stmt.all();
    return { ok:true, results: result.results || [] };
  }catch(e){
    return { ok:false, results: [], error: String(e && e.message || e) };
  }
}
async function dbFirst(env, sql, params = []){
  try{
    let stmt = env.DB.prepare(sql);
    if(params.length) stmt = stmt.bind(...params);
    const result = await stmt.first();
    return { ok:true, row: result || null };
  }catch(e){
    return { ok:false, row: null, error: String(e && e.message || e) };
  }
}
async function tableExists(env, table){
  if(!env || !env.DB) return { ok:false, present:false, error:"D1 DB binding missing" };
  const r = await dbFirst(env, "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", [table]);
  if(!r.ok) return { ok:false, present:false, error:r.error };
  return { ok:true, present:!!r.row };
}
async function getColumns(env, table){
  const r = await dbAll(env, `PRAGMA table_info(${table})`);
  if(!r.ok) return { ok:false, columns:[], error:r.error };
  return { ok:true, columns:r.results || [] };
}
async function getIndexes(env, table){
  const r = await dbAll(env, `PRAGMA index_list(${table})`);
  if(!r.ok) return { ok:false, indexes:[], error:r.error };
  return { ok:true, indexes:r.results || [] };
}
async function requireContextTable(env){
  const exists = await tableExists(env, TABLE);
  if(!exists.ok) return { ok:false, status:500, error:"line_context_table_check_failed", detail:exists.error };
  if(!exists.present) return { ok:false, status:503, error:"line_context_table_missing", detail:"Apply migration migrations/20260814_customer_line_message_events.sql" };
  return { ok:true };
}
async function customerColumns(env){
  const cols = await getColumns(env, "customers");
  if(!cols.ok) return { ok:false, columns:new Set(), error:cols.error };
  return { ok:true, columns:new Set(cols.columns.map(x => x.name)) };
}
function pickLineUserId(row){
  return text(row && (row.line_user_id || row.lineUserId || row.line_id || row.line_uid || row.line_mid));
}
async function findCustomer(env, customerId){
  const customerTable = await tableExists(env, "customers");
  if(!customerTable.ok) return { ok:false, status:500, found:false, line_user_id:"", reason:"customers_table_check_failed", error:customerTable.error };
  if(!customerTable.present) return { ok:false, status:503, found:false, line_user_id:"", reason:"customers_table_missing" };

  const colsResult = await customerColumns(env);
  if(!colsResult.ok) return { ok:false, status:500, found:false, line_user_id:"", reason:"customers_schema_check_failed", error:colsResult.error };
  const cols = colsResult.columns;
  const wheres = [];
  const params = [];
  if(cols.has("id")){ wheres.push("CAST(id AS TEXT)=?"); params.push(customerId); }
  if(cols.has("customer_id")){ wheres.push("CAST(customer_id AS TEXT)=?"); params.push(customerId); }
  if(!wheres.length) return { ok:false, status:500, found:false, line_user_id:"", reason:"customer_id_columns_missing" };

  const row = await dbFirst(env, `SELECT * FROM customers WHERE ${wheres.join(" OR ")} LIMIT 1`, params);
  if(!row.ok) return { ok:false, status:500, found:false, line_user_id:"", reason:"customer_lookup_failed", error:row.error };
  if(!row.row) return { ok:true, found:false, line_user_id:"", reason:"customer_not_found" };
  return { ok:true, found:true, customer: row.row, customer_id: text(row.row.customer_id || row.row.id || customerId), line_user_id: pickLineUserId(row.row), reason:"" };
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
  const table = await requireContextTable(env);
  if(!table.ok) return { ok:false, status:table.status, error:table.error, detail:table.detail, events:[] };

  const r = await dbAll(env, `SELECT event_id, customer_id, line_user_id, direction, message_type, message_text, source, send_status, line_message_id, sender_type, occurred_at, created_at FROM ${TABLE} WHERE ${whereSql} ORDER BY COALESCE(occurred_at, created_at, '') DESC, id DESC LIMIT ${limit}`, params);
  if(!r.ok) return { ok:false, status:500, error:"line_context_query_failed", detail:r.error, events:[] };
  return { ok:true, events:(r.results || []).map(eventOut) };
}
async function handleCustomerEvents(req, env, customerId){
  if(req.method !== "GET") return json({ ok:false, error:"Method Not Allowed" }, 405);
  if(!env || !env.DB) return json({ ok:false, error:"D1 DB binding missing" }, 500);
  if(!accessEmail(req)) return json({ ok:false, error:"Cloudflare Access authentication required" }, 401);

  const url = new URL(req.url);
  const limit = clampLimit(url.searchParams.get("limit"));
  const customer = await findCustomer(env, customerId);
  if(!customer.ok){
    return json({ ok:false, build:BUILD, customer_id:customerId, line_user_id:"", line_linked:false, error:customer.reason, detail:customer.error || "" }, customer.status || 500);
  }
  if(!customer.found){
    return json({ ok:true, build:BUILD, customer_id:customerId, line_user_id:"", line_linked:false, reason:customer.reason, limit, count:0, events:[] });
  }
  if(!customer.line_user_id){
    return json({ ok:true, build:BUILD, customer_id:customer.customer_id, line_user_id:"", line_linked:false, reason:"line_user_id_not_linked", limit, count:0, events:[] });
  }
  const result = await listEvents(env, "line_user_id=?", [customer.line_user_id], limit);
  if(!result.ok){
    return json({ ok:false, build:BUILD, customer_id:customer.customer_id, line_user_id:customer.line_user_id, line_linked:true, error:result.error, table:TABLE, detail:result.detail || "" }, result.status || 500);
  }
  return json({ ok:true, build:BUILD, customer_id:customer.customer_id, line_user_id:customer.line_user_id, line_linked:true, limit, count:result.events.length, events:result.events });
}
async function handleInternalEvents(req, env){
  if(req.method !== "GET") return json({ ok:false, error:"Method Not Allowed" }, 405);
  if(!env || !env.DB) return json({ ok:false, error:"D1 DB binding missing" }, 500);
  if(!internalAllowed(req, env)) return json({ ok:false, error:"CRM_INTERNAL_TOKEN authentication required" }, 401);
  const url = new URL(req.url);
  const lineUserId = text(url.searchParams.get("line_user_id"));
  const limit = clampLimit(url.searchParams.get("limit"));
  if(!lineUserId) return json({ ok:false, error:"line_user_id is required" }, 400);
  const result = await listEvents(env, "line_user_id=?", [lineUserId], limit);
  if(!result.ok){
    return json({ ok:false, build:BUILD, line_user_id:lineUserId, error:result.error, table:TABLE, detail:result.detail || "" }, result.status || 500);
  }
  return json({ ok:true, build:BUILD, line_user_id:lineUserId, limit, count:result.events.length, events:result.events });
}
export async function handleLineContextEvents(req, env){
  const url = new URL(req.url);
  const customerMatch = url.pathname.match(/^\/api\/customers\/([^/]+)\/line-context-events$/);
  if(customerMatch) return handleCustomerEvents(req, env, decodeURIComponent(customerMatch[1]));
  if(url.pathname === "/api/internal/line-context-events") return handleInternalEvents(req, env);
  return null;
}
export async function lineContextHealth(env){
  const table = await tableExists(env, TABLE);
  let columns = [];
  let indexes = [];
  if(table.ok && table.present){
    const c = await getColumns(env, TABLE);
    const i = await getIndexes(env, TABLE);
    columns = c.ok ? c.columns.map(x => x.name) : [];
    indexes = i.ok ? i.indexes.map(x => x.name).filter(Boolean) : [];
  }
  const requiredColumns = ["event_id", "customer_id", "line_user_id", "direction", "message_type", "message_text", "source", "send_status", "line_message_id", "sender_type", "occurred_at", "created_at"];
  const missingColumns = table.present ? requiredColumns.filter(x => !columns.includes(x)) : requiredColumns;
  return {
    line_context_events_enabled: !!(table.ok && table.present && missingColumns.length === 0),
    line_context_events_table: TABLE,
    line_context_events_table_present: !!(table.ok && table.present),
    line_context_events_columns_ok: missingColumns.length === 0,
    line_context_events_missing_columns: missingColumns,
    line_context_events_indexes: indexes,
    line_context_sent_only_formal: true,
    line_live_send_enabled: false,
    line_context_build: BUILD
  };
}
