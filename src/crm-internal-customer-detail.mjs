// TASK-RESERVATION-DETAIL-CRM-POPUP-001
// Strict READ-only customer detail API for reservation Workspace.
// customer_id is the only lookup key. No name/id/LINE fallback. No DDL or writes.

const BUILD = "crm-internal-customer-detail-20260816-01";
const MAX_RESERVATIONS = 30;

function text(v){ return String(v == null ? "" : v).trim(); }
function numberOrNull(v){
  if(v === null || v === undefined || text(v) === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}
function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "content-type": "application/json; charset=utf-8",
      "cache-control": "no-store, no-cache, must-revalidate, max-age=0",
      "x-crm-internal-customer-detail-build": BUILD
    }
  });
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
async function dbFirst(env, sql, params = []){
  try{
    let stmt = env.DB.prepare(sql);
    if(params.length) stmt = stmt.bind(...params);
    return { ok:true, row:(await stmt.first()) || null };
  }catch(e){
    return { ok:false, row:null, error:text(e && e.message || e) };
  }
}
async function dbAll(env, sql, params = []){
  try{
    let stmt = env.DB.prepare(sql);
    if(params.length) stmt = stmt.bind(...params);
    const r = await stmt.all();
    return { ok:true, results:r.results || [] };
  }catch(e){
    return { ok:false, results:[], error:text(e && e.message || e) };
  }
}
async function tableExists(env, table){
  const r = await dbFirst(env, "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", [table]);
  return { ok:r.ok, present:!!r.row, error:r.error || "" };
}
function customerOut(row){
  return {
    customer_id: text(row.customer_id),
    name: text(row.name),
    furigana: text(row.furigana),
    line_display_name: text(row.line_display_name),
    line_linked: !!text(row.line_user_id),
    phone: text(row.phone),
    email: text(row.email),
    address: text(row.address),
    repeat_count: numberOrNull(row.repeat_count),
    total_revenue: numberOrNull(row.total_revenue),
    customer_rank: text(row.customer_rank),
    memo: text(row.memo),
    last_shoot_date: text(row.last_shoot_date),
    genre_history: text(row.genre_history),
    acquisition_source: text(row.acquisition_source),
    updated_at: text(row.updated_at)
  };
}
function reservationOut(row){
  return {
    reservation_id: text(row.reservation_id || row.id),
    customer_id: text(row.customer_id),
    customer_name: text(row.customer_name),
    genre: text(row.genre),
    shoot_date: text(row.shoot_date),
    start_time: text(row.start_time),
    end_time: text(row.end_time),
    plan_label: text(row.plan_label),
    place: text(row.place),
    total_amount: numberOrNull(row.total_amount),
    status: text(row.status),
    source: text(row.source),
    created_at: text(row.created_at),
    updated_at: text(row.updated_at)
  };
}

export async function handleInternalCustomerDetail(req, env){
  const url = new URL(req.url);
  if(url.pathname !== "/api/internal/customer-detail") return null;
  if(req.method !== "GET") return json({ ok:false, error:"Method Not Allowed" }, 405);
  if(!env || !env.DB) return json({ ok:false, error:"D1 DB binding missing" }, 500);
  if(!internalAllowed(req, env)) return json({ ok:false, error:"CRM_INTERNAL_TOKEN authentication required" }, 401);

  const customerId = text(url.searchParams.get("customer_id"));
  if(!customerId) return json({ ok:false, build:BUILD, error:"customer_id is required", code:"customer_id_missing" }, 400);

  const customersTable = await tableExists(env, "customers");
  if(!customersTable.ok) return json({ ok:false, build:BUILD, error:"customers_table_check_failed" }, 500);
  if(!customersTable.present) return json({ ok:false, build:BUILD, error:"customers_table_missing" }, 503);

  const customerResult = await dbFirst(env, `
    SELECT customer_id, name, furigana, line_display_name, line_user_id,
           phone, email, address, repeat_count, total_revenue, customer_rank,
           memo, last_shoot_date, genre_history, acquisition_source, updated_at
    FROM customers
    WHERE CAST(customer_id AS TEXT)=?
      AND COALESCE(deleted_at, '')=''
    LIMIT 1
  `, [customerId]);
  if(!customerResult.ok) return json({ ok:false, build:BUILD, error:"customer_lookup_failed" }, 500);
  if(!customerResult.row) return json({ ok:false, build:BUILD, code:"customer_not_found", customer_id:customerId, error:"customer not found" }, 404);

  const customer = customerOut(customerResult.row);
  if(customer.customer_id !== customerId){
    return json({ ok:false, build:BUILD, code:"customer_id_mismatch", customer_id:customerId, error:"customer id mismatch" }, 409);
  }

  let historyAvailable = false;
  let reservations = [];
  const reservationsTable = await tableExists(env, "customer_reservations");
  if(reservationsTable.ok && reservationsTable.present){
    const reservationResult = await dbAll(env, `
      SELECT id, reservation_id, customer_id, customer_name, genre,
             shoot_date, start_time, end_time, plan_label, place, total_amount,
             status, source, created_at, updated_at
      FROM customer_reservations
      WHERE CAST(customer_id AS TEXT)=?
        AND COALESCE(deleted_at, '')=''
      ORDER BY COALESCE(shoot_date, created_at, '') DESC, id DESC
      LIMIT ${MAX_RESERVATIONS}
    `, [customerId]);
    if(reservationResult.ok){
      historyAvailable = true;
      reservations = reservationResult.results.map(reservationOut).filter(x => x.customer_id === customerId);
    }
  }

  return json({
    ok:true,
    build:BUILD,
    source:"customer-crm",
    lookup_key:"customer_id",
    fallback_used:false,
    customer,
    reservation_history:{
      available:historyAvailable,
      source:historyAvailable ? "customer-crm.customer_reservations" : "",
      count:historyAvailable ? reservations.length : null,
      latest:historyAvailable && reservations.length ? reservations[0] : null,
      items:reservations
    },
    cumulative_sales:{
      available:customer.total_revenue !== null,
      source:customer.total_revenue !== null ? "customer-crm.customers.total_revenue" : "",
      amount:customer.total_revenue
    },
    crm_detail_url:null
  });
}

export function internalCustomerDetailHealth(env){
  return {
    internal_customer_detail_enabled:true,
    internal_customer_detail_read_only:true,
    internal_customer_detail_lookup_key:"customer_id",
    internal_customer_detail_fallback:false,
    internal_customer_detail_service_auth:"CRM_INTERNAL_TOKEN",
    internal_customer_detail_token_configured:!!text(env && env.CRM_INTERNAL_TOKEN),
    internal_customer_detail_build:BUILD
  };
}
