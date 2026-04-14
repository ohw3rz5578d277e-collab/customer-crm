const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,PATCH,DELETE,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, x-sync-token, Authorization"
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      ...corsHeaders
    }
  });
}

function toInt(value, fallback = 0) {
  const num = parseInt(value ?? fallback, 10);
  return Number.isFinite(num) ? num : fallback;
}

function toFloat(value, fallback = 0) {
  const num = parseFloat(value ?? fallback);
  return Number.isFinite(num) ? num : fallback;
}

function normalizeText(value) {
  if (value === undefined || value === null) return null;
  const text = String(value).trim();
  return text === "" ? null : text;
}

function normalizeDate(value) {
  const text = normalizeText(value);
  return text || null;
}

function normalizeBool01(value) {
  if (value === true || value === "true" || value === 1 || value === "1") return 1;
  return 0;
}

function getSyncTokenFromRequest(request) {
  const xSyncToken = request.headers.get("x-sync-token");
  if (xSyncToken) return xSyncToken;

  const auth = request.headers.get("Authorization");
  if (auth && auth.startsWith("Bearer ")) {
    return auth.slice("Bearer ".length);
  }

  return "";
}

function normalizeCustomer(item) {
  const customerId = normalizeText(item.customer_id);
  const name =
    normalizeText(item.name) ||
    normalizeText(item.line_display_name) ||
    "名称未設定";

  if (!customerId) {
    throw new Error("customer_id is required");
  }

  return {
    customer_id: customerId,
    name,
    furigana: normalizeText(item.furigana),
    line_display_name: normalizeText(item.line_display_name),
    phone: normalizeText(item.phone),
    address: normalizeText(item.address),
    email: normalizeText(item.email),
    genre_history: normalizeText(item.genre_history),
    last_shoot_date: normalizeDate(item.last_shoot_date),
    repeat_count: toInt(item.repeat_count, 0),
    repeat_count_1y: toInt(item.repeat_count_1y, 0),
    total_revenue: toInt(item.total_revenue, 0),
    avg_order_value: toFloat(item.avg_order_value, 0),
    acquisition_source: normalizeText(item.acquisition_source),
    referrer: normalizeText(item.referrer),
    child1_name: normalizeText(item.child1_name),
    child1_birthdate: normalizeDate(item.child1_birthdate),
    child2_name: normalizeText(item.child2_name),
    child2_birthdate: normalizeDate(item.child2_birthdate),
    child3_name: normalizeText(item.child3_name),
    child3_birthdate: normalizeDate(item.child3_birthdate),
    anniversary: normalizeDate(item.anniversary),
    nps:
      item.nps === undefined || item.nps === null || item.nps === ""
        ? null
        : toInt(item.nps, 0),
    photo_public_ok: normalizeBool01(item.photo_public_ok),
    memo: normalizeText(item.memo),
    genre_revenue_breakdown: normalizeText(item.genre_revenue_breakdown),
    line_user_id: normalizeText(item.line_user_id),
    created_at: normalizeDate(item.created_at) || new Date().toISOString(),
    updated_at: new Date().toISOString(),
    first_shoot_date: normalizeDate(item.first_shoot_date),
    repeat_count_90d: toInt(item.repeat_count_90d, 0),
    repeat_count_365d: toInt(item.repeat_count_365d, 0),
    repeat_count_730d: toInt(item.repeat_count_730d, 0),
    dormant_days: toInt(item.dormant_days, 0),
    square_avg_payment: toFloat(item.square_avg_payment, 0),
    square_last_payment_date: normalizeDate(item.square_last_payment_date)
  };
}

async function upsertCustomer(env, customer) {
  const stmt = env.DB.prepare(`
    INSERT INTO customers (
      customer_id,
      name,
      furigana,
      line_display_name,
      phone,
      address,
      email,
      genre_history,
      last_shoot_date,
      repeat_count,
      repeat_count_1y,
      total_revenue,
      avg_order_value,
      acquisition_source,
      referrer,
      child1_name,
      child1_birthdate,
      child2_name,
      child2_birthdate,
      child3_name,
      child3_birthdate,
      anniversary,
      nps,
      photo_public_ok,
      memo,
      genre_revenue_breakdown,
      line_user_id,
      created_at,
      updated_at,
      first_shoot_date,
      repeat_count_90d,
      repeat_count_365d,
      repeat_count_730d,
      dormant_days,
      square_avg_payment,
      square_last_payment_date
    ) VALUES (
      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    ON CONFLICT(customer_id) DO UPDATE SET
      name = excluded.name,
      furigana = excluded.furigana,
      line_display_name = excluded.line_display_name,
      phone = excluded.phone,
      address = excluded.address,
      email = excluded.email,
      genre_history = excluded.genre_history,
      last_shoot_date = excluded.last_shoot_date,
      repeat_count = excluded.repeat_count,
      repeat_count_1y = excluded.repeat_count_1y,
      total_revenue = excluded.total_revenue,
      avg_order_value = excluded.avg_order_value,
      acquisition_source = excluded.acquisition_source,
      referrer = excluded.referrer,
      child1_name = excluded.child1_name,
      child1_birthdate = excluded.child1_birthdate,
      child2_name = excluded.child2_name,
      child2_birthdate = excluded.child2_birthdate,
      child3_name = excluded.child3_name,
      child3_birthdate = excluded.child3_birthdate,
      anniversary = excluded.anniversary,
      nps = excluded.nps,
      photo_public_ok = excluded.photo_public_ok,
      memo = excluded.memo,
      genre_revenue_breakdown = excluded.genre_revenue_breakdown,
      line_user_id = excluded.line_user_id,
      updated_at = excluded.updated_at,
      first_shoot_date = excluded.first_shoot_date,
      repeat_count_90d = excluded.repeat_count_90d,
      repeat_count_365d = excluded.repeat_count_365d,
      repeat_count_730d = excluded.repeat_count_730d,
      dormant_days = excluded.dormant_days,
      square_avg_payment = excluded.square_avg_payment,
      square_last_payment_date = excluded.square_last_payment_date
  `).bind(
    customer.customer_id,
    customer.name,
    customer.furigana,
    customer.line_display_name,
    customer.phone,
    customer.address,
    customer.email,
    customer.genre_history,
    customer.last_shoot_date,
    customer.repeat_count,
    customer.repeat_count_1y,
    customer.total_revenue,
    customer.avg_order_value,
    customer.acquisition_source,
    customer.referrer,
    customer.child1_name,
    customer.child1_birthdate,
    customer.child2_name,
    customer.child2_birthdate,
    customer.child3_name,
    customer.child3_birthdate,
    customer.anniversary,
    customer.nps,
    customer.photo_public_ok,
    customer.memo,
    customer.genre_revenue_breakdown,
    customer.line_user_id,
    customer.created_at,
    customer.updated_at,
    customer.first_shoot_date,
    customer.repeat_count_90d,
    customer.repeat_count_365d,
    customer.repeat_count_730d,
    customer.dormant_days,
    customer.square_avg_payment,
    customer.square_last_payment_date
  );

  return stmt.run();
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders
      });
    }

    if (url.pathname === "/api/health") {
      return json({
        ok: true,
        service: "customer-crm-api",
        time: new Date().toISOString()
      });
    }

    if (url.pathname === "/api/debug-env") {
      return json({
        ok: true,
        hasDB: !!env.DB,
        hasSyncToken: !!env.SYNC_TOKEN,
        keys: Object.keys(env).sort()
      });
    }

    if (url.pathname === "/api/customers" && request.method === "GET") {
      try {
        const keyword = (url.searchParams.get("keyword") || "").trim();
        const limitRaw = parseInt(url.searchParams.get("limit") || "50", 10);
        const limit = Math.max(1, Math.min(limitRaw, 100));

        let result;

        if (keyword) {
          const like = "%" + keyword + "%";
          result = await env.DB.prepare(`
            SELECT
              customer_id,
              name,
              furigana,
              line_display_name,
              phone,
              email,
              last_shoot_date,
              repeat_count,
              repeat_count_1y,
              total_revenue,
              avg_order_value,
              acquisition_source,
              dormant_days,
              photo_public_ok,
              created_at,
              updated_at
            FROM customers
            WHERE
              name LIKE ?1
              OR furigana LIKE ?1
              OR line_display_name LIKE ?1
              OR phone LIKE ?1
              OR email LIKE ?1
            ORDER BY updated_at DESC
            LIMIT ?2
          `).bind(like, limit).all();
        } else {
          result = await env.DB.prepare(`
            SELECT
              customer_id,
              name,
              furigana,
              line_display_name,
              phone,
              email,
              last_shoot_date,
              repeat_count,
              repeat_count_1y,
              total_revenue,
              avg_order_value,
              acquisition_source,
              dormant_days,
              photo_public_ok,
              created_at,
              updated_at
            FROM customers
            ORDER BY updated_at DESC
            LIMIT ?1
          `).bind(limit).all();
        }

        return json({
          ok: true,
          count: result.results ? result.results.length : 0,
          items: result.results || []
        });
      } catch (error) {
        return json(
          {
            ok: false,
            message: error && error.message ? error.message : "Unknown error"
          },
          500
        );
      }
    }

    if (url.pathname === "/api/sync/customers/upsert" && request.method === "POST") {
      try {
        const reqToken = getSyncTokenFromRequest(request);
        const workerToken = env.SYNC_TOKEN;

        if (!workerToken) {
          return json({
            ok: false,
            message: "SYNC_TOKEN is not configured"
          }, 500);
        }

        if (!reqToken || reqToken !== workerToken) {
          return json({
            ok: false,
            message: "Unauthorized",
            debug: {
              hasRequestToken: !!reqToken,
              requestTokenLength: reqToken ? reqToken.length : 0,
              hasWorkerToken: !!workerToken,
              workerTokenLength: workerToken ? workerToken.length : 0,
              tokensMatch: !!reqToken && !!workerToken && reqToken === workerToken
            }
          }, 401);
        }

        const body = await request.json();
        const rawItems = Array.isArray(body)
          ? body
          : Array.isArray(body.items)
            ? body.items
            : [body];

        const items = rawItems.filter(Boolean);

        if (items.length === 0) {
          return json({
            ok: false,
            message: "No customer items found"
          }, 400);
        }

        const results = [];

        for (const item of items) {
          const customer = normalizeCustomer(item);
          await upsertCustomer(env, customer);
          results.push({
            customer_id: customer.customer_id,
            name: customer.name
          });
        }

        return json({
          ok: true,
          upserted: results.length,
          items: results
        });
      } catch (error) {
        return json(
          {
            ok: false,
            message: error && error.message ? error.message : "Unknown error"
          },
          500
        );
      }
    }

    return json({
      ok: false,
      message: "Not Found"
    }, 404);
  }
};
