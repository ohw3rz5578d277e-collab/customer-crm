// ======================================================
// CUSTOMER CRM API / COMPLETE WORKER
// build: customer-crm-api-complete-20260516-marketing-filter-items-01
// ======================================================
// customer-crm-api / src / index.js を「全削除 → 全貼り替え」してください。
//
// 今回の内容:
// 1) 既存の /api/customers と /api/sync/customers/upsert を維持
// 2) /admin 顧客管理画面を追加
// 3) 顧客一覧・検索・セグメント抽出を追加
// 4) 顧客詳細・撮影履歴・タイムラインを追加
// 5) テスト顧客削除APIを追加
// 6) reservation-app-api からの同期内容を customer_reservations / customer_timeline にも保存
// 7) 既存D1テーブルがある場合も壊れにくいよう、CREATE IF NOT EXISTS と ADD COLUMN で吸収
// ======================================================

const BUILD = "customer-crm-api-complete-20260516-marketing-filter-items-01";
const DEFAULT_ADMIN_TOKEN = "mizuno-admin-2026-secret-001";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,PATCH,DELETE,OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, x-sync-token, x-admin-token, Authorization",
  "Cache-Control": "no-store"
};

function json(data, status = 200) {
  return new Response(JSON.stringify(data, null, 2), {
    status,
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      ...corsHeaders
    }
  });
}

function html(body, status = 200) {
  return new Response(body, {
    status,
    headers: {
      "Content-Type": "text/html; charset=utf-8",
      ...corsHeaders
    }
  });
}

function text(value) {
  if (value === undefined || value === null) return "";
  return String(value).trim();
}

function nullableText(value) {
  const v = text(value);
  return v === "" ? null : v;
}

function toInt(value, fallback = 0) {
  if (value === undefined || value === null || value === "") return fallback;
  const n = parseInt(String(value).replace(/[,円¥\s]/g, ""), 10);
  return Number.isFinite(n) ? n : fallback;
}

function toFloat(value, fallback = 0) {
  if (value === undefined || value === null || value === "") return fallback;
  const n = parseFloat(String(value).replace(/[,円¥\s]/g, ""));
  return Number.isFinite(n) ? n : fallback;
}

function normalizeDate(value) {
  const v = text(value);
  if (!v) return null;
  const m = v.match(/^(20\d{2})[\/\-.年](\d{1,2})[\/\-.月](\d{1,2})日?/);
  if (m) return `${m[1]}-${String(m[2]).padStart(2, "0")}-${String(m[3]).padStart(2, "0")}`;
  return v;
}

function bool01(value) {
  if (value === true || value === 1 || value === "1") return 1;
  if (/^(true|yes|ok|公開可|可|許可)$/i.test(text(value))) return 1;
  return 0;
}

function nowIso() {
  return new Date().toISOString();
}

function getAdminToken(env) {
  return text(env.ADMIN_TOKEN) || DEFAULT_ADMIN_TOKEN;
}

function tokenFromRequest(request) {
  const url = new URL(request.url);
  const auth = text(request.headers.get("Authorization"));
  return (
    text(url.searchParams.get("token")) ||
    text(request.headers.get("x-admin-token")) ||
    (/^Bearer\s+/i.test(auth) ? auth.replace(/^Bearer\s+/i, "").trim() : "")
  );
}

function isAdmin(request, env) {
  return tokenFromRequest(request) === getAdminToken(env);
}

function getSyncTokenFromRequest(request) {
  const xSyncToken = request.headers.get("x-sync-token");
  if (xSyncToken) return xSyncToken;

  const auth = request.headers.get("Authorization");
  if (auth && auth.startsWith("Bearer ")) return auth.slice("Bearer ".length);

  return "";
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

async function ensureSchema(db) {
  if (!db) throw new Error("D1 DB binding(DB) が見つかりません");

  await db.prepare(`
    CREATE TABLE IF NOT EXISTS customers (
      customer_id TEXT PRIMARY KEY,
      name TEXT,
      furigana TEXT,
      line_display_name TEXT,
      phone TEXT,
      address TEXT,
      email TEXT,
      genre_history TEXT,
      last_shoot_date TEXT,
      repeat_count INTEGER DEFAULT 0,
      repeat_count_1y INTEGER DEFAULT 0,
      total_revenue REAL DEFAULT 0,
      avg_order_value REAL DEFAULT 0,
      acquisition_source TEXT,
      referrer TEXT,
      child1_name TEXT,
      child1_birthdate TEXT,
      child2_name TEXT,
      child2_birthdate TEXT,
      child3_name TEXT,
      child3_birthdate TEXT,
      anniversary TEXT,
      nps INTEGER,
      photo_public_ok INTEGER DEFAULT 0,
      memo TEXT,
      genre_revenue_breakdown TEXT,
      line_user_id TEXT,
      created_at TEXT,
      updated_at TEXT,
      first_shoot_date TEXT,
      repeat_count_90d INTEGER DEFAULT 0,
      repeat_count_365d INTEGER DEFAULT 0,
      repeat_count_730d INTEGER DEFAULT 0,
      dormant_days INTEGER DEFAULT 0,
      square_avg_payment REAL DEFAULT 0,
      square_last_payment_date TEXT
    )
  `).run();

  for (const col of [
    "furigana TEXT",
    "line_display_name TEXT",
    "phone TEXT",
    "address TEXT",
    "email TEXT",
    "genre_history TEXT",
    "last_shoot_date TEXT",
    "repeat_count INTEGER DEFAULT 0",
    "repeat_count_1y INTEGER DEFAULT 0",
    "total_revenue REAL DEFAULT 0",
    "avg_order_value REAL DEFAULT 0",
    "acquisition_source TEXT",
    "referrer TEXT",
    "child1_name TEXT",
    "child1_birthdate TEXT",
    "child2_name TEXT",
    "child2_birthdate TEXT",
    "child3_name TEXT",
    "child3_birthdate TEXT",
    "anniversary TEXT",
    "nps INTEGER",
    "photo_public_ok INTEGER DEFAULT 0",
    "memo TEXT",
    "genre_revenue_breakdown TEXT",
    "line_user_id TEXT",
    "created_at TEXT",
    "updated_at TEXT",
    "first_shoot_date TEXT",
    "repeat_count_90d INTEGER DEFAULT 0",
    "repeat_count_365d INTEGER DEFAULT 0",
    "repeat_count_730d INTEGER DEFAULT 0",
    "dormant_days INTEGER DEFAULT 0",
    "square_avg_payment REAL DEFAULT 0",
    "square_last_payment_date TEXT"
  ]) await addColumn(db, "customers", col);

  await db.prepare(`
    CREATE TABLE IF NOT EXISTS customer_reservations (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_key TEXT,
      reservation_id TEXT,
      customer_id TEXT NOT NULL,
      customer_name TEXT,
      genre TEXT,
      shoot_date TEXT,
      start_time TEXT,
      end_time TEXT,
      plan_label TEXT,
      place TEXT,
      total_amount REAL DEFAULT 0,
      status TEXT,
      source TEXT,
      raw_json TEXT,
      created_at TEXT,
      updated_at TEXT
    )
  `).run();

  for (const col of [
    "event_key TEXT",
    "reservation_id TEXT",
    "customer_id TEXT",
    "customer_name TEXT",
    "genre TEXT",
    "shoot_date TEXT",
    "start_time TEXT",
    "end_time TEXT",
    "plan_label TEXT",
    "place TEXT",
    "total_amount REAL DEFAULT 0",
    "status TEXT",
    "source TEXT",
    "raw_json TEXT",
    "created_at TEXT",
    "updated_at TEXT"
  ]) await addColumn(db, "customer_reservations", col);

  await db.prepare(`CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_reservations_event_key ON customer_reservations(event_key)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_customer_reservations_customer_date ON customer_reservations(customer_id, shoot_date)`).run();

  await db.prepare(`
    CREATE TABLE IF NOT EXISTS customer_items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      item_key TEXT,
      customer_id TEXT NOT NULL,
      reservation_id TEXT,
      item_category TEXT,
      item_name TEXT,
      item_amount REAL DEFAULT 0,
      purchase_date TEXT,
      source TEXT,
      raw_json TEXT,
      created_at TEXT,
      updated_at TEXT
    )
  `).run();

  for (const col of [
    "item_key TEXT",
    "customer_id TEXT",
    "reservation_id TEXT",
    "item_category TEXT",
    "item_name TEXT",
    "item_amount REAL DEFAULT 0",
    "purchase_date TEXT",
    "source TEXT",
    "raw_json TEXT",
    "created_at TEXT",
    "updated_at TEXT"
  ]) await addColumn(db, "customer_items", col);

  await db.prepare(`CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_items_item_key ON customer_items(item_key)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_customer_items_customer_date ON customer_items(customer_id, purchase_date)`).run();

  await db.prepare(`
    CREATE TABLE IF NOT EXISTS customer_timeline (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event_key TEXT,
      customer_id TEXT NOT NULL,
      event_type TEXT,
      event_title TEXT,
      event_date TEXT,
      amount REAL DEFAULT 0,
      detail_json TEXT,
      created_at TEXT
    )
  `).run();

  for (const col of [
    "event_key TEXT",
    "customer_id TEXT",
    "event_type TEXT",
    "event_title TEXT",
    "event_date TEXT",
    "amount REAL DEFAULT 0",
    "detail_json TEXT",
    "created_at TEXT"
  ]) await addColumn(db, "customer_timeline", col);

  await db.prepare(`CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_timeline_event_key ON customer_timeline(event_key)`).run();
  await db.prepare(`CREATE INDEX IF NOT EXISTS idx_customer_timeline_customer_date ON customer_timeline(customer_id, event_date)`).run();

  await db.prepare(`
    CREATE TABLE IF NOT EXISTS customer_tags (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      customer_id TEXT NOT NULL,
      tag TEXT NOT NULL,
      created_at TEXT
    )
  `).run();

  await db.prepare(`CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_tags_unique ON customer_tags(customer_id, tag)`).run();
}

function normalizeCustomer(item) {
  const customerId = nullableText(item.customer_id);
  const name = nullableText(item.name) || nullableText(item.customer_name) || nullableText(item.line_display_name) || "名称未設定";

  if (!customerId) throw new Error("customer_id is required");

  return {
    customer_id: customerId,
    name,
    furigana: nullableText(item.furigana),
    line_display_name: nullableText(item.line_display_name),
    phone: nullableText(item.phone),
    address: nullableText(item.address),
    email: nullableText(item.email),
    genre_history: nullableText(item.genre_history || item.genre),
    last_shoot_date: normalizeDate(item.last_shoot_date || item.shoot_date),
    repeat_count: toInt(item.repeat_count, 0),
    repeat_count_1y: toInt(item.repeat_count_1y, 0),
    total_revenue: toFloat(item.total_revenue || item.total_amount, 0),
    avg_order_value: toFloat(item.avg_order_value || item.total_amount, 0),
    acquisition_source: nullableText(item.acquisition_source),
    referrer: nullableText(item.referrer),
    child1_name: nullableText(item.child1_name || item.child_names),
    child1_birthdate: normalizeDate(item.child1_birthdate),
    child2_name: nullableText(item.child2_name),
    child2_birthdate: normalizeDate(item.child2_birthdate),
    child3_name: nullableText(item.child3_name),
    child3_birthdate: normalizeDate(item.child3_birthdate),
    anniversary: normalizeDate(item.anniversary),
    nps: item.nps === undefined || item.nps === null || item.nps === "" ? null : toInt(item.nps, 0),
    photo_public_ok: bool01(item.photo_public_ok),
    memo: nullableText(item.memo),
    genre_revenue_breakdown: nullableText(item.genre_revenue_breakdown),
    line_user_id: nullableText(item.line_user_id),
    created_at: nullableText(item.created_at) || nowIso(),
    updated_at: nowIso(),
    first_shoot_date: normalizeDate(item.first_shoot_date || item.shoot_date),
    repeat_count_90d: toInt(item.repeat_count_90d, 0),
    repeat_count_365d: toInt(item.repeat_count_365d, 0),
    repeat_count_730d: toInt(item.repeat_count_730d, 0),
    dormant_days: toInt(item.dormant_days, 0),
    square_avg_payment: toFloat(item.square_avg_payment, 0),
    square_last_payment_date: normalizeDate(item.square_last_payment_date)
  };
}

function makeReservationEventKey(customer, raw) {
  const customerId = text(customer.customer_id);
  const reservationId = text(raw.reservation_id);
  const date = text(raw.shoot_date || raw.last_shoot_date || customer.last_shoot_date);
  const genre = text(raw.genre || raw.genre_history || customer.genre_history);
  const amount = String(toFloat(raw.total_amount || raw.total_revenue || customer.total_revenue, 0));
  const source = text(raw.acquisition_source || "sync");
  if (reservationId) return `reservation_id:${reservationId}`;
  return `customer:${customerId}|date:${date}|genre:${genre}|amount:${amount}|source:${source}`;
}



function crmItemKey(customerId, reservationId, category, name, amount, date) {
  return ["item", text(customerId), text(reservationId), text(category), text(name), String(toFloat(amount, 0)), text(date)].join("|");
}

function collectPurchaseItems(customer, raw) {
  const customerId = text(customer.customer_id);
  const reservationId = text(raw.reservation_id);
  const date = normalizeDate(raw.shoot_date || raw.last_shoot_date || customer.last_shoot_date);
  const source = text(raw.acquisition_source || "sync");
  const items = [];

  const planLabel = text(raw.plan_label);
  const planAmount = toFloat(raw.plan_amount, 0);
  if (planLabel || planAmount) {
    items.push({ item_key: crmItemKey(customerId, reservationId, "plan", planLabel || "撮影プラン", planAmount, date), customer_id: customerId, reservation_id: reservationId || null, item_category: "plan", item_name: planLabel || "撮影プラン", item_amount: planAmount, purchase_date: date, source });
  }

  for (const n of [1, 2, 3]) {
    const name = text(raw[`option${n}_name`]);
    const amount = toFloat(raw[`option${n}_amount`], 0);
    if (name && name !== "なし") {
      items.push({ item_key: crmItemKey(customerId, reservationId, "option", name, amount, date), customer_id: customerId, reservation_id: reservationId || null, item_category: "option", item_name: name, item_amount: amount, purchase_date: date, source });
    }
  }

  const studioAmount = toFloat(raw.studio_amount, 0);
  if (studioAmount > 0) {
    items.push({ item_key: crmItemKey(customerId, reservationId, "studio", "スタジオ利用料", studioAmount, date), customer_id: customerId, reservation_id: reservationId || null, item_category: "studio", item_name: "スタジオ利用料", item_amount: studioAmount, purchase_date: date, source });
  }

  const trafficAmount = toFloat(raw.traffic_amount, 0);
  if (trafficAmount > 0) {
    items.push({ item_key: crmItemKey(customerId, reservationId, "traffic", "交通費", trafficAmount, date), customer_id: customerId, reservation_id: reservationId || null, item_category: "traffic", item_name: "交通費", item_amount: trafficAmount, purchase_date: date, source });
  }

  const rawItems = Array.isArray(raw.items) ? raw.items : [];
  for (const item of rawItems) {
    const name = text(item.name || item.item_name || item.label);
    const amount = toFloat(item.amount || item.item_amount || item.price, 0);
    const category = text(item.category || item.item_category || "item");
    if (!name && !amount) continue;
    items.push({ item_key: crmItemKey(customerId, reservationId, category, name || "購入アイテム", amount, date), customer_id: customerId, reservation_id: reservationId || null, item_category: category, item_name: name || "購入アイテム", item_amount: amount, purchase_date: date, source });
  }

  return items;
}

async function upsertCustomerItem(env, item, raw) {
  await env.DB.prepare(`
    INSERT INTO customer_items (item_key, customer_id, reservation_id, item_category, item_name, item_amount, purchase_date, source, raw_json, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(item_key) DO UPDATE SET
      item_category = excluded.item_category,
      item_name = excluded.item_name,
      item_amount = excluded.item_amount,
      purchase_date = excluded.purchase_date,
      source = excluded.source,
      raw_json = excluded.raw_json,
      updated_at = excluded.updated_at
  `).bind(item.item_key, item.customer_id, item.reservation_id, item.item_category, item.item_name, item.item_amount, item.purchase_date, item.source, JSON.stringify(raw), nowIso(), nowIso()).run();

  await env.DB.prepare(`
    INSERT INTO customer_timeline (event_key, customer_id, event_type, event_title, event_date, amount, detail_json, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(event_key) DO UPDATE SET event_title=excluded.event_title, event_date=excluded.event_date, amount=excluded.amount, detail_json=excluded.detail_json
  `).bind("timeline:" + item.item_key, item.customer_id, "item", `${item.item_name} ${item.item_amount ? " / " + Number(item.item_amount).toLocaleString("ja-JP") + "円" : ""}`, item.purchase_date, item.item_amount, JSON.stringify(item), nowIso()).run();
}

async function upsertCustomer(env, customer) {
  return env.DB.prepare(`
    INSERT INTO customers (
      customer_id,name,furigana,line_display_name,phone,address,email,genre_history,last_shoot_date,
      repeat_count,repeat_count_1y,total_revenue,avg_order_value,acquisition_source,referrer,
      child1_name,child1_birthdate,child2_name,child2_birthdate,child3_name,child3_birthdate,
      anniversary,nps,photo_public_ok,memo,genre_revenue_breakdown,line_user_id,
      created_at,updated_at,first_shoot_date,repeat_count_90d,repeat_count_365d,repeat_count_730d,
      dormant_days,square_avg_payment,square_last_payment_date
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(customer_id) DO UPDATE SET
      name=COALESCE(excluded.name,customers.name),
      furigana=COALESCE(excluded.furigana,customers.furigana),
      line_display_name=COALESCE(excluded.line_display_name,customers.line_display_name),
      phone=COALESCE(excluded.phone,customers.phone),
      address=COALESCE(excluded.address,customers.address),
      email=COALESCE(excluded.email,customers.email),
      genre_history=COALESCE(excluded.genre_history,customers.genre_history),
      last_shoot_date=COALESCE(excluded.last_shoot_date,customers.last_shoot_date),
      repeat_count=CASE WHEN excluded.repeat_count>0 THEN excluded.repeat_count ELSE customers.repeat_count END,
      repeat_count_1y=CASE WHEN excluded.repeat_count_1y>0 THEN excluded.repeat_count_1y ELSE customers.repeat_count_1y END,
      total_revenue=CASE WHEN excluded.total_revenue>0 THEN excluded.total_revenue ELSE customers.total_revenue END,
      avg_order_value=CASE WHEN excluded.avg_order_value>0 THEN excluded.avg_order_value ELSE customers.avg_order_value END,
      acquisition_source=COALESCE(excluded.acquisition_source,customers.acquisition_source),
      referrer=COALESCE(excluded.referrer,customers.referrer),
      child1_name=COALESCE(excluded.child1_name,customers.child1_name),
      child1_birthdate=COALESCE(excluded.child1_birthdate,customers.child1_birthdate),
      child2_name=COALESCE(excluded.child2_name,customers.child2_name),
      child2_birthdate=COALESCE(excluded.child2_birthdate,customers.child2_birthdate),
      child3_name=COALESCE(excluded.child3_name,customers.child3_name),
      child3_birthdate=COALESCE(excluded.child3_birthdate,customers.child3_birthdate),
      anniversary=COALESCE(excluded.anniversary,customers.anniversary),
      nps=COALESCE(excluded.nps,customers.nps),
      photo_public_ok=excluded.photo_public_ok,
      memo=COALESCE(excluded.memo,customers.memo),
      genre_revenue_breakdown=COALESCE(excluded.genre_revenue_breakdown,customers.genre_revenue_breakdown),
      line_user_id=COALESCE(excluded.line_user_id,customers.line_user_id),
      updated_at=excluded.updated_at,
      first_shoot_date=COALESCE(customers.first_shoot_date,excluded.first_shoot_date),
      repeat_count_90d=CASE WHEN excluded.repeat_count_90d>0 THEN excluded.repeat_count_90d ELSE customers.repeat_count_90d END,
      repeat_count_365d=CASE WHEN excluded.repeat_count_365d>0 THEN excluded.repeat_count_365d ELSE customers.repeat_count_365d END,
      repeat_count_730d=CASE WHEN excluded.repeat_count_730d>0 THEN excluded.repeat_count_730d ELSE customers.repeat_count_730d END,
      dormant_days=CASE WHEN excluded.dormant_days>0 THEN excluded.dormant_days ELSE customers.dormant_days END,
      square_avg_payment=CASE WHEN excluded.square_avg_payment>0 THEN excluded.square_avg_payment ELSE customers.square_avg_payment END,
      square_last_payment_date=COALESCE(excluded.square_last_payment_date,customers.square_last_payment_date)
  `).bind(
    customer.customer_id, customer.name, customer.furigana, customer.line_display_name, customer.phone,
    customer.address, customer.email, customer.genre_history, customer.last_shoot_date,
    customer.repeat_count, customer.repeat_count_1y, customer.total_revenue, customer.avg_order_value,
    customer.acquisition_source, customer.referrer,
    customer.child1_name, customer.child1_birthdate, customer.child2_name, customer.child2_birthdate,
    customer.child3_name, customer.child3_birthdate, customer.anniversary, customer.nps, customer.photo_public_ok,
    customer.memo, customer.genre_revenue_breakdown, customer.line_user_id,
    customer.created_at, customer.updated_at, customer.first_shoot_date,
    customer.repeat_count_90d, customer.repeat_count_365d, customer.repeat_count_730d,
    customer.dormant_days, customer.square_avg_payment, customer.square_last_payment_date
  ).run();
}

async function upsertReservationHistory(env, customer, raw) {
  const date = normalizeDate(raw.shoot_date || raw.last_shoot_date || customer.last_shoot_date);
  const genre = nullableText(raw.genre || raw.genre_history || customer.genre_history);
  const amount = toFloat(raw.total_amount || raw.total_revenue || customer.total_revenue, 0);
  if (!date && !amount && !genre) return { ok: true, skipped: true };

  const eventKey = makeReservationEventKey(customer, raw);
  const title = `${date || "日付未設定"} ${genre || "撮影"} ${amount ? " / " + amount.toLocaleString("ja-JP") + "円" : ""}`;

  await env.DB.prepare(`
    INSERT INTO customer_reservations (
      event_key,reservation_id,customer_id,customer_name,genre,shoot_date,start_time,end_time,plan_label,place,total_amount,status,source,raw_json,created_at,updated_at
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(event_key) DO UPDATE SET
      reservation_id=COALESCE(excluded.reservation_id,customer_reservations.reservation_id),
      customer_name=COALESCE(excluded.customer_name,customer_reservations.customer_name),
      genre=COALESCE(excluded.genre,customer_reservations.genre),
      shoot_date=COALESCE(excluded.shoot_date,customer_reservations.shoot_date),
      start_time=COALESCE(excluded.start_time,customer_reservations.start_time),
      end_time=COALESCE(excluded.end_time,customer_reservations.end_time),
      plan_label=COALESCE(excluded.plan_label,customer_reservations.plan_label),
      place=COALESCE(excluded.place,customer_reservations.place),
      total_amount=CASE WHEN excluded.total_amount>0 THEN excluded.total_amount ELSE customer_reservations.total_amount END,
      status=COALESCE(excluded.status,customer_reservations.status),
      source=COALESCE(excluded.source,customer_reservations.source),
      raw_json=excluded.raw_json,
      updated_at=excluded.updated_at
  `).bind(
    eventKey,
    nullableText(raw.reservation_id),
    customer.customer_id,
    nullableText(raw.customer_name || raw.name || customer.name),
    genre,
    date,
    nullableText(raw.start_time),
    nullableText(raw.end_time),
    nullableText(raw.plan_label),
    nullableText(raw.place || raw.shoot_location),
    amount,
    nullableText(raw.status),
    nullableText(raw.acquisition_source || "sync"),
    JSON.stringify(raw),
    nowIso(),
    nowIso()
  ).run();

  await env.DB.prepare(`
    INSERT INTO customer_timeline(event_key,customer_id,event_type,event_title,event_date,amount,detail_json,created_at)
    VALUES(?,?,?,?,?,?,?,?)
    ON CONFLICT(event_key) DO UPDATE SET
      event_title=excluded.event_title,
      event_date=excluded.event_date,
      amount=excluded.amount,
      detail_json=excluded.detail_json
  `).bind(
    "timeline:" + eventKey,
    customer.customer_id,
    "reservation",
    title,
    date,
    amount,
    JSON.stringify(raw),
    nowIso()
  ).run();

  const purchaseItems = collectPurchaseItems(customer, raw);
  for (const item of purchaseItems) {
    await upsertCustomerItem(env, item, raw);
  }

  return { ok: true, event_key: eventKey, item_count: purchaseItems.length };
}

async function refreshCustomerStats(env, customerId) {
  const row = await env.DB.prepare(`
    SELECT
      COUNT(*) AS repeat_count,
      COALESCE(SUM(total_amount),0) AS total_revenue,
      COALESCE(AVG(NULLIF(total_amount,0)),0) AS avg_order_value,
      MIN(shoot_date) AS first_shoot_date,
      MAX(shoot_date) AS last_shoot_date,
      SUM(CASE WHEN shoot_date >= date('now','-90 day') THEN 1 ELSE 0 END) AS repeat_count_90d,
      SUM(CASE WHEN shoot_date >= date('now','-365 day') THEN 1 ELSE 0 END) AS repeat_count_365d,
      SUM(CASE WHEN shoot_date >= date('now','-730 day') THEN 1 ELSE 0 END) AS repeat_count_730d,
      GROUP_CONCAT(DISTINCT genre) AS genre_history
    FROM customer_reservations
    WHERE customer_id=?
  `).bind(customerId).first();

  if (!row || Number(row.repeat_count || 0) <= 0) return { ok: true, skipped: true };

  const dormant = await env.DB.prepare(`
    SELECT CASE WHEN ? IS NULL OR ?='' THEN 0 ELSE CAST(julianday('now') - julianday(?) AS INTEGER) END AS dormant_days
  `).bind(row.last_shoot_date, row.last_shoot_date, row.last_shoot_date).first();

  await env.DB.prepare(`
    UPDATE customers SET
      repeat_count=?, repeat_count_1y=?, total_revenue=?, avg_order_value=?, first_shoot_date=?, last_shoot_date=?,
      repeat_count_90d=?, repeat_count_365d=?, repeat_count_730d=?, dormant_days=?, genre_history=COALESCE(?,genre_history), updated_at=?
    WHERE customer_id=?
  `).bind(
    Number(row.repeat_count || 0),
    Number(row.repeat_count_365d || 0),
    Number(row.total_revenue || 0),
    Number(row.avg_order_value || 0),
    row.first_shoot_date || null,
    row.last_shoot_date || null,
    Number(row.repeat_count_90d || 0),
    Number(row.repeat_count_365d || 0),
    Number(row.repeat_count_730d || 0),
    Number(dormant && dormant.dormant_days ? dormant.dormant_days : 0),
    row.genre_history || null,
    nowIso(),
    customerId
  ).run();

  return { ok: true };
}

async function upsertCustomersFromPayload(env, payload) {
  const rawItems = Array.isArray(payload) ? payload : Array.isArray(payload.items) ? payload.items : [payload];
  const items = rawItems.filter(Boolean);
  if (!items.length) return { ok: false, message: "No customer items found" };

  const results = [];
  for (const item of items) {
    const customer = normalizeCustomer(item);
    await upsertCustomer(env, customer);
    await upsertReservationHistory(env, customer, item);
    await refreshCustomerStats(env, customer.customer_id);
    results.push({ customer_id: customer.customer_id, name: customer.name });
  }
  return { ok: true, upserted: results.length, items: results };
}

function customerSelectSql() {
  return `
    SELECT customer_id,name,furigana,line_display_name,phone,email,address,genre_history,first_shoot_date,last_shoot_date,
      repeat_count,repeat_count_1y,repeat_count_90d,repeat_count_365d,repeat_count_730d,total_revenue,avg_order_value,
      acquisition_source,referrer,child1_name,child1_birthdate,child2_name,child2_birthdate,child3_name,child3_birthdate,
      anniversary,nps,photo_public_ok,memo,line_user_id,dormant_days,square_avg_payment,square_last_payment_date,created_at,updated_at
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

async function listCustomers(env, url) {
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

  const where = [];
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

  const sql = `${customerSelectSql()} ${where.length ? "WHERE " + where.join(" AND ") : ""} ORDER BY ${orderBy} LIMIT ?`;
  params.push(limit);

  const result = await env.DB.prepare(sql).bind(...params).all();
  const items = result.results || [];
  return { ok: true, count: items.length, items, filters: { keyword, segment, genre, source, min_revenue: minRevenue, max_revenue: maxRevenue, min_repeat: minRepeat, min_dormant: minDormant, photo_public_ok: photoPublicOk, has_child: hasChild, sort } };
}

async function getCustomerDetail(env, customerId) {
  const customer = await env.DB.prepare(`${customerSelectSql()} WHERE customer_id=? LIMIT 1`).bind(customerId).first();
  if (!customer) return { ok: false, message: "customer not found" };

  const reservations = await env.DB.prepare("SELECT * FROM customer_reservations WHERE customer_id=? ORDER BY shoot_date DESC, updated_at DESC LIMIT 100").bind(customerId).all();
  const items = await env.DB.prepare("SELECT * FROM customer_items WHERE customer_id=? ORDER BY purchase_date DESC, updated_at DESC LIMIT 200").bind(customerId).all();
  const timeline = await env.DB.prepare("SELECT * FROM customer_timeline WHERE customer_id=? ORDER BY COALESCE(event_date, created_at) DESC, id DESC LIMIT 100").bind(customerId).all();
  const tags = await env.DB.prepare("SELECT tag FROM customer_tags WHERE customer_id=? ORDER BY tag ASC").bind(customerId).all();

  return { ok: true, customer, reservations: reservations.results || [], items: items.results || [], timeline: timeline.results || [], tags: (tags.results || []).map(r => r.tag) };
}

async function getSegmentSummary(env) {
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
  `).first();

  const reservationRow = await env.DB.prepare("SELECT COUNT(*) AS reservations_total, COALESCE(SUM(total_amount),0) AS reservations_revenue, COALESCE(AVG(NULLIF(total_amount,0)),0) AS reservation_aov FROM customer_reservations").first();
  const itemRow = await env.DB.prepare("SELECT COUNT(*) AS item_count, COALESCE(SUM(item_amount),0) AS item_revenue, COALESCE(AVG(NULLIF(item_amount,0)),0) AS item_avg_amount FROM customer_items").first();
  const genreResult = await env.DB.prepare("SELECT COALESCE(genre,'未設定') AS genre, COUNT(*) AS count, COALESCE(SUM(total_amount),0) AS revenue FROM customer_reservations GROUP BY COALESCE(genre,'未設定') ORDER BY revenue DESC, count DESC LIMIT 30").all();
  const sourceResult = await env.DB.prepare("SELECT COALESCE(acquisition_source,'未設定') AS source, COUNT(*) AS count, COALESCE(SUM(total_revenue),0) AS revenue FROM customers GROUP BY COALESCE(acquisition_source,'未設定') ORDER BY count DESC LIMIT 30").all();
  const itemRanking = await env.DB.prepare("SELECT COALESCE(item_category,'item') AS item_category, COALESCE(item_name,'未設定') AS item_name, COUNT(*) AS count, COALESCE(SUM(item_amount),0) AS revenue FROM customer_items GROUP BY COALESCE(item_category,'item'), COALESCE(item_name,'未設定') ORDER BY revenue DESC, count DESC LIMIT 30").all();

  const summary = { ...(row || {}), ...(reservationRow || {}), ...(itemRow || {}) };
  const total = Number(summary.total || 0);
  summary.repeat_rate = total > 0 ? Number(summary.repeaters || 0) / total * 100 : 0;
  summary.line_rate = total > 0 ? Number(summary.line || 0) / total * 100 : 0;
  summary.photo_public_rate = total > 0 ? Number(summary.photo_public_ok || 0) / total * 100 : 0;

  return { ok: true, summary, genres: genreResult.results || [], sources: sourceResult.results || [], itemRanking: itemRanking.results || [] };
}

async function deleteCustomers(env, ids) {
  const cleanIds = (Array.isArray(ids) ? ids : [ids]).map(text).filter(Boolean);
  if (!cleanIds.length) return { ok: false, message: "customer_ids required" };
  for (const id of cleanIds) {
    await env.DB.prepare("DELETE FROM customers WHERE customer_id=?").bind(id).run();
    await env.DB.prepare("DELETE FROM customer_reservations WHERE customer_id=?").bind(id).run();
    await env.DB.prepare("DELETE FROM customer_items WHERE customer_id=?").bind(id).run();
    await env.DB.prepare("DELETE FROM customer_timeline WHERE customer_id=?").bind(id).run();
    await env.DB.prepare("DELETE FROM customer_tags WHERE customer_id=?").bind(id).run();
  }
  return { ok: true, deleted: cleanIds.length, customer_ids: cleanIds };
}

function adminPage() {
  return `<!doctype html><html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>顧客管理CRM</title><style>
:root{--bg:#f6f7fb;--surface:#fff;--surface2:#f1f5f9;--text:#111827;--muted:#6b7280;--primary:#028760;--border:#e5e7eb;--danger:#dc2626}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans JP",sans-serif;padding-bottom:32px}.app{max-width:1280px;margin:auto;padding:14px}.header{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;margin-bottom:12px}.title{font-size:30px;font-weight:950;letter-spacing:-.03em}.sub{color:var(--muted);font-size:.86rem;line-height:1.55}.card,.box,.stat{background:var(--surface);border:1px solid var(--border);border-radius:20px;padding:14px;box-shadow:0 8px 24px rgba(15,23,42,.06)}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:12px 0}.stat b{font-size:24px}.stat small{display:block;color:var(--muted);font-weight:800;margin-top:4px}.marketing{display:grid;grid-template-columns:1.25fr 1fr;gap:10px;margin:12px 0}.toolbar{display:grid;grid-template-columns:1fr auto auto auto;gap:8px;margin:12px 0}.input,select,textarea{width:100%;min-height:44px;border:1px solid var(--border);border-radius:14px;background:#fff;padding:10px 12px;font-size:16px}.btn{border:1px solid var(--border);border-radius:14px;background:#fff;padding:10px 13px;font-weight:900;cursor:pointer}.primary{background:var(--primary);color:#fff;border-color:var(--primary)}.danger{background:#fee2e2;color:#991b1b;border-color:#fecaca}.chips,.filter-summary{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0}.chip{border:1px solid var(--border);background:#fff;border-radius:999px;padding:8px 11px;font-weight:900;font-size:.82rem;cursor:pointer}.chip.active{background:var(--primary);border-color:var(--primary);color:#fff}.tablewrap{overflow:auto;background:#fff;border:1px solid var(--border);border-radius:20px}table{width:100%;border-collapse:collapse;min-width:1020px}th,td{border-bottom:1px solid var(--border);padding:10px;text-align:left;vertical-align:top}th{font-size:.78rem;color:var(--muted);background:#f8fafc;position:sticky;top:0;z-index:1}.badge{display:inline-block;border-radius:999px;background:var(--surface2);padding:4px 8px;font-size:.76rem;font-weight:900;margin:2px 2px 2px 0}.name{font-weight:950}.money{font-weight:950}.rank-row{display:grid;grid-template-columns:1fr auto auto;gap:8px;padding:8px;border:1px solid var(--border);border-radius:12px;background:#fff;margin-top:6px}.modal-bg{display:none;position:fixed;inset:0;background:rgba(15,23,42,.42);z-index:20}.modal{display:none;position:fixed;right:0;top:0;bottom:0;width:min(92vw,780px);background:#fff;z-index:21;overflow:auto;padding:18px;box-shadow:-20px 0 60px rgba(15,23,42,.2)}.filter-modal{display:none;position:fixed;left:50%;top:50%;transform:translate(-50%,-50%);width:min(94vw,860px);max-height:90vh;overflow:auto;background:#fff;border-radius:24px;z-index:22;padding:18px;box-shadow:0 24px 80px rgba(15,23,42,.25)}.modal.show,.modal-bg.show,.filter-modal.show{display:block}.detail-grid,.filter-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}.kv{background:#f8fafc;border:1px solid var(--border);border-radius:14px;padding:10px}.kv .k{font-size:.76rem;color:var(--muted);font-weight:900}.kv .v{font-weight:900;margin-top:4px;word-break:break-word}.timeline{display:grid;gap:8px}.timeline-item{border:1px solid var(--border);border-radius:14px;padding:10px;background:#fff}.actions{display:flex;justify-content:space-between;gap:8px;align-items:center;margin:12px 0;position:sticky;bottom:0;background:#fff;padding:10px 0}@media(max-width:820px){.app{padding:10px}.header{display:block}.title{font-size:24px}.grid{grid-template-columns:repeat(2,1fr)}.toolbar,.marketing,.detail-grid,.filter-grid{grid-template-columns:1fr}.modal{width:100%;border-radius:20px 20px 0 0;top:8vh}.filter-modal{left:0;right:0;bottom:0;top:auto;transform:none;width:100%;border-radius:24px 24px 0 0;max-height:86vh}}
</style></head><body><div class="app"><div class="header"><div><div class="title">顧客管理CRM</div><div class="sub">予約管理アプリと連携した顧客一覧・撮影履歴・購入履歴・マーケティング抽出</div></div><div><button class="btn" id="reloadBtn">更新</button><button class="btn danger" id="deleteTestBtn">テスト顧客削除</button></div></div><div class="box" id="status">読み込み中...</div><div class="grid" id="summary"></div><div class="marketing"><div class="box"><b>顧客マーケティング指標</b><div class="sub">平均顧客単価・リピート率・購入履歴から次の施策対象を見つけます。</div><div class="grid" id="marketingCards" style="grid-template-columns:repeat(3,1fr)"></div></div><div class="box"><b>購入アイテムランキング</b><div class="sub">プラン・オプション・スタジオ利用料・交通費を購入履歴として保存します。</div><div id="itemRanking"></div></div></div><div class="card"><div class="toolbar"><input class="input" id="keyword" placeholder="名前・ふりがな・電話・メール・LINE名・ジャンルで検索"><select id="sort"><option value="updated_at">更新順</option><option value="last_shoot">最終撮影日順</option><option value="revenue">売上順</option><option value="aov">平均顧客単価順</option><option value="repeat">リピート回数順</option><option value="dormant">休眠日数順</option></select><button class="btn" id="filterBtn">フィルター</button><button class="btn primary" id="searchBtn">検索</button></div><div class="filter-summary" id="filterSummary"></div><div class="chips" id="segments"></div><div class="tablewrap"><table><thead><tr><th>顧客</th><th>連絡先</th><th>撮影履歴</th><th>売上</th><th>流入</th><th>お子さま/記念日</th><th>操作</th></tr></thead><tbody id="tbody"></tbody></table></div></div></div><div class="modal-bg" id="modalBg"></div><div class="modal" id="modal"></div><div class="filter-modal" id="filterModal"></div><script>
(function(){const TOKEN=new URLSearchParams(location.search).get('token')||'';const $=(id)=>document.getElementById(id);let state={items:[],segment:'',summary:{},genres:[],sources:[],itemRanking:[],filters:{genre:'',source:'',min_revenue:'',max_revenue:'',min_repeat:'',min_dormant:'',photo_public_ok:'',has_child:''}};function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(m){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]})}function yen(v){return '¥'+Number(v||0).toLocaleString('ja-JP')}function pct(v){return Number(v||0).toLocaleString('ja-JP',{maximumFractionDigits:1})+'%'}function api(path,options){options=options||{};options.headers=options.headers||{};if(TOKEN)options.headers['x-admin-token']=TOKEN;if(options.body&&!options.headers['Content-Type'])options.headers['Content-Type']='application/json';const u=new URL(path,location.origin);if(TOKEN&&!u.searchParams.get('token'))u.searchParams.set('token',TOKEN);u.searchParams.set('_',String(Date.now()));return fetch(u.toString(),options).then(r=>r.text().then(t=>{let d={};try{d=t?JSON.parse(t):{}}catch(e){throw Error('JSONではない応答: '+t.slice(0,160))}if(!r.ok||d.ok===false)throw Error(d.message||d.error||('HTTP '+r.status));return d}))}function status(m,e){$('status').textContent=m;$('status').style.color=e?'var(--danger)':'var(--muted)'}function stat(l,v,s){return '<div class="stat"><div class="sub">'+esc(l)+'</div><b>'+esc(v)+'</b><small>'+esc(s||'')+'</small></div>'}function renderSummary(){const s=state.summary||{};$('summary').innerHTML=[stat('顧客数',Number(s.total||0).toLocaleString('ja-JP')+'人','CRM登録済み'),stat('平均顧客単価',yen(s.avg_ltv||0),'顧客LTVの平均'),stat('平均注文単価',yen(s.reservation_aov||s.avg_order_value||0),'撮影1回あたり'),stat('リピート率',pct(s.repeat_rate||0),'2回以上'),stat('休眠180日',Number(s.dormant_180||0).toLocaleString('ja-JP')+'人','再来店施策'),stat('休眠365日',Number(s.dormant_365||0).toLocaleString('ja-JP')+'人','強めの再接触'),stat('購入アイテム数',Number(s.item_count||0).toLocaleString('ja-JP')+'件','プラン/オプション'),stat('累計売上',yen(s.total_revenue||0),'CRM集計')].join('');$('marketingCards').innerHTML=[stat('リピーター',Number(s.repeaters||0).toLocaleString('ja-JP')+'人','再提案候補'),stat('高売上顧客',Number(s.high_value||0).toLocaleString('ja-JP')+'人','10万円以上'),stat('LINE連携率',pct(s.line_rate||0),'LINE接点あり'),stat('写真公開OK率',pct(s.photo_public_rate||0),'作例依頼候補'),stat('購入単価',yen(s.item_avg_amount||0),'アイテム平均'),stat('購入売上',yen(s.item_revenue||0),'アイテム合計')].join('');$('itemRanking').innerHTML=(state.itemRanking||[]).length?state.itemRanking.slice(0,8).map(x=>'<div class="rank-row"><div><b>'+esc(x.item_name)+'</b><div class="sub">'+esc(x.item_category)+'</div></div><div>'+Number(x.count||0)+'件</div><div class="money">'+yen(x.revenue||0)+'</div></div>').join(''):'<div class="sub">購入履歴はまだありません。今後の予約保存でプラン・オプションが蓄積されます。</div>'}function renderSegments(){const list=[['','すべて'],['repeaters','リピーター'],['first_time','初回のみ'],['dormant_90','休眠90日'],['dormant_180','休眠180日'],['dormant_365','休眠365日'],['high_value','高売上'],['omiyamairi','お宮参り'],['shichigosan','七五三'],['line','LINE連携'],['no_phone','電話なし'],['photo_public_ok','写真公開OK']];$('segments').innerHTML=list.map(x=>'<button class="chip '+(state.segment===x[0]?'active':'')+'" data-segment="'+esc(x[0])+'">'+esc(x[1])+'</button>').join('');document.querySelectorAll('[data-segment]').forEach(btn=>btn.onclick=()=>{state.segment=btn.dataset.segment||'';loadCustomers()})}function renderFilterSummary(){const labels=[];if(state.segment)labels.push('セグメント: '+state.segment);if(state.filters.genre)labels.push('ジャンル: '+state.filters.genre);if(state.filters.source)labels.push('流入元: '+state.filters.source);if(state.filters.min_revenue)labels.push('売上 '+Number(state.filters.min_revenue).toLocaleString('ja-JP')+'円以上');if(state.filters.max_revenue)labels.push('売上 '+Number(state.filters.max_revenue).toLocaleString('ja-JP')+'円以下');if(state.filters.min_repeat)labels.push('撮影 '+state.filters.min_repeat+'回以上');if(state.filters.min_dormant)labels.push('休眠 '+state.filters.min_dormant+'日以上');if(state.filters.photo_public_ok==='1')labels.push('写真公開OK');if(state.filters.has_child==='1')labels.push('お子さま情報あり');$('filterSummary').innerHTML=labels.length?labels.map(x=>'<span class="badge">'+esc(x)+'</span>').join('')+'<button class="chip" id="clearFiltersBtn">条件クリア</button>':'<span class="sub">フィルター条件なし</span>';const btn=$('clearFiltersBtn');if(btn)btn.onclick=()=>{state.segment='';state.filters={genre:'',source:'',min_revenue:'',max_revenue:'',min_repeat:'',min_dormant:'',photo_public_ok:'',has_child:''};loadCustomers()}}function renderCustomers(){renderFilterSummary();const items=state.items||[];if(!items.length){$('tbody').innerHTML='<tr><td colspan="7">顧客が見つかりません。</td></tr>';return}$('tbody').innerHTML=items.map(x=>{const child=[x.child1_name,x.child2_name,x.child3_name].filter(Boolean).join(' / ');const tags=[x.repeat_count>=2?'リピーター':'初回',x.dormant_days>=365?'休眠365日':'',x.photo_public_ok?'公開OK':''].filter(Boolean).map(t=>'<span class="badge">'+esc(t)+'</span>').join('');return '<tr><td><div class="name">'+esc(x.name)+'</div><div class="sub">'+esc(x.furigana||x.line_display_name||x.customer_id)+'</div>'+tags+'</td><td><div>'+esc(x.phone||'電話なし')+'</div><div class="sub">'+esc(x.email||'メールなし')+'</div></td><td><div>最終: <b>'+esc(x.last_shoot_date||'未設定')+'</b></div><div class="sub">初回: '+esc(x.first_shoot_date||'未設定')+' / 回数: '+Number(x.repeat_count||0)+'回</div><div class="sub">'+esc(x.genre_history||'')+'</div></td><td><div class="money">'+yen(x.total_revenue||0)+'</div><div class="sub">平均 '+yen(x.avg_order_value||0)+'</div></td><td><div>'+esc(x.acquisition_source||'未設定')+'</div><div class="sub">紹介: '+esc(x.referrer||'なし')+'</div></td><td><div>'+esc(child||'未設定')+'</div><div class="sub">記念日: '+esc(x.anniversary||'なし')+'</div></td><td><button class="btn" data-detail="'+esc(x.customer_id)+'">詳細</button></td></tr>'}).join('');document.querySelectorAll('[data-detail]').forEach(btn=>btn.onclick=()=>openDetail(btn.dataset.detail))}function kv(k,v){return '<div class="kv"><div class="k">'+esc(k)+'</div><div class="v">'+esc(v||'未設定')+'</div></div>'}function openModal(){$('modalBg').classList.add('show');$('modal').classList.add('show')}function closeModal(){$('modalBg').classList.remove('show');$('modal').classList.remove('show');$('filterModal').classList.remove('show')}function openFilter(){$('modalBg').classList.add('show');const genreOptions=['<option value="">すべて</option>'].concat((state.genres||[]).map(g=>'<option value="'+esc(g.genre)+'" '+(state.filters.genre===g.genre?'selected':'')+'>'+esc(g.genre)+'（'+Number(g.count||0)+'件）</option>')).join('');const sourceOptions=['<option value="">すべて</option>'].concat((state.sources||[]).map(s=>'<option value="'+esc(s.source)+'" '+(state.filters.source===s.source?'selected':'')+'>'+esc(s.source)+'（'+Number(s.count||0)+'件）</option>')).join('');$('filterModal').innerHTML='<h2>顧客フィルター</h2><div class="sub">ジャンル・流入元・売上・休眠日数などでマーケティング施策対象を抽出できます。</div><div class="filter-grid" style="margin-top:12px"><div><div class="sub">ジャンル</div><select id="fGenre">'+genreOptions+'</select></div><div><div class="sub">流入元</div><select id="fSource">'+sourceOptions+'</select></div><div><div class="sub">売上下限</div><input class="input" id="fMinRevenue" type="number" value="'+esc(state.filters.min_revenue)+'" placeholder="例: 50000"></div><div><div class="sub">売上上限</div><input class="input" id="fMaxRevenue" type="number" value="'+esc(state.filters.max_revenue)+'" placeholder="例: 150000"></div><div><div class="sub">撮影回数 下限</div><input class="input" id="fMinRepeat" type="number" value="'+esc(state.filters.min_repeat)+'" placeholder="例: 2"></div><div><div class="sub">休眠日数 下限</div><input class="input" id="fMinDormant" type="number" value="'+esc(state.filters.min_dormant)+'" placeholder="例: 180"></div><div><label><input type="checkbox" id="fPhotoOk" '+(state.filters.photo_public_ok==='1'?'checked':'')+'> 写真公開OKのみ</label></div><div><label><input type="checkbox" id="fHasChild" '+(state.filters.has_child==='1'?'checked':'')+'> お子さま情報あり</label></div></div><div class="actions"><button class="btn" id="filterClearBtn">クリア</button><div><button class="btn" id="filterCloseBtn">閉じる</button> <button class="btn primary" id="filterApplyBtn">この条件で抽出</button></div></div>';$('filterModal').classList.add('show');$('filterCloseBtn').onclick=closeModal;$('filterClearBtn').onclick=()=>{state.filters={genre:'',source:'',min_revenue:'',max_revenue:'',min_repeat:'',min_dormant:'',photo_public_ok:'',has_child:''};closeModal();loadCustomers()};$('filterApplyBtn').onclick=()=>{state.filters.genre=$('fGenre').value;state.filters.source=$('fSource').value;state.filters.min_revenue=$('fMinRevenue').value;state.filters.max_revenue=$('fMaxRevenue').value;state.filters.min_repeat=$('fMinRepeat').value;state.filters.min_dormant=$('fMinDormant').value;state.filters.photo_public_ok=$('fPhotoOk').checked?'1':'';state.filters.has_child=$('fHasChild').checked?'1':'';closeModal();loadCustomers()}}async function openDetail(id){openModal();$('modal').innerHTML='<h2>読み込み中...</h2>';try{const d=await api('/api/customers/'+encodeURIComponent(id));const c=d.customer;const rs=d.reservations||[];const items=d.items||[];const tl=d.timeline||[];$('modal').innerHTML='<div class="actions"><div><h2>'+esc(c.name)+'</h2><div class="sub">'+esc(c.customer_id)+'</div></div><button class="btn" id="closeModalBtn">閉じる</button></div><div class="detail-grid">'+kv('ふりがな',c.furigana)+kv('LINE表示名',c.line_display_name)+kv('電話',c.phone)+kv('メール',c.email)+kv('住所',c.address)+kv('流入元',c.acquisition_source)+kv('紹介者',c.referrer)+kv('最終撮影日',c.last_shoot_date)+kv('撮影回数',Number(c.repeat_count||0)+'回')+kv('累計売上',yen(c.total_revenue||0))+kv('平均顧客単価',yen(c.avg_order_value||0))+kv('休眠日数',Number(c.dormant_days||0)+'日')+kv('お子さま1',[c.child1_name,c.child1_birthdate].filter(Boolean).join(' / '))+kv('お子さま2',[c.child2_name,c.child2_birthdate].filter(Boolean).join(' / '))+kv('お子さま3',[c.child3_name,c.child3_birthdate].filter(Boolean).join(' / '))+kv('記念日',c.anniversary)+'</div><div class="box" style="margin-top:12px"><b>メモ</b><div class="sub" style="white-space:pre-wrap">'+esc(c.memo||'')+'</div></div><h3>撮影履歴</h3><div class="timeline">'+(rs.length?rs.map(r=>'<div class="timeline-item"><b>'+esc(r.shoot_date||'日付未設定')+' '+esc(r.genre||'')+'</b><div class="sub">'+esc(r.start_time||'')+' '+esc(r.place||'')+' / '+esc(r.plan_label||'')+'</div><div class="money">'+yen(r.total_amount||0)+'</div></div>').join(''):'<div class="sub">撮影履歴はまだありません。</div>')+'</div><h3>購入履歴</h3><div class="timeline">'+(items.length?items.map(item=>'<div class="timeline-item"><b>'+esc(item.item_name||'購入アイテム')+'</b><div class="sub">'+esc(item.purchase_date||'')+' / '+esc(item.item_category||'')+'</div><div class="money">'+yen(item.item_amount||0)+'</div></div>').join(''):'<div class="sub">購入履歴はまだありません。今後の予約保存でプラン・オプションが蓄積されます。</div>')+'</div><h3>タイムライン</h3><div class="timeline">'+(tl.length?tl.map(t=>'<div class="timeline-item"><b>'+esc(t.event_date||t.created_at||'')+' '+esc(t.event_title||'')+'</b><div class="sub">'+esc(t.event_type||'')+' / '+yen(t.amount||0)+'</div></div>').join(''):'<div class="sub">タイムラインはまだありません。</div>')+'</div>';$('closeModalBtn').onclick=closeModal}catch(e){$('modal').innerHTML='<h2>エラー</h2><p>'+esc(e.message)+'</p><button class="btn" id="closeModalBtn">閉じる</button>';$('closeModalBtn').onclick=closeModal}}async function loadSummary(){try{const d=await api('/api/segments/summary');state.summary=d.summary||{};state.genres=d.genres||[];state.sources=d.sources||[];state.itemRanking=d.itemRanking||[];renderSummary()}catch(e){console.warn(e)}}async function loadCustomers(){status('顧客一覧を読み込み中...');renderSegments();const p=new URLSearchParams();const kw=$('keyword').value.trim();if(kw)p.set('keyword',kw);if(state.segment)p.set('segment',state.segment);Object.keys(state.filters).forEach(k=>{if(state.filters[k])p.set(k,state.filters[k])});p.set('sort',$('sort').value||'updated_at');p.set('limit','200');try{const d=await api('/api/customers?'+p.toString());state.items=d.items||[];renderCustomers();status('読み込み完了: '+state.items.length+'件')}catch(e){status('読み込み失敗: '+e.message,true)}}async function deleteTest(){if(!confirm('テスト顧客 26000099 / CRM-LIVE-TEST-001 を削除しますか？'))return;try{await api('/api/customers/delete-test',{method:'POST',body:JSON.stringify({})});await loadSummary();await loadCustomers();status('テスト顧客を削除しました')}catch(e){alert('削除失敗: '+e.message)}}$('modalBg').onclick=closeModal;$('reloadBtn').onclick=()=>{loadSummary();loadCustomers()};$('searchBtn').onclick=loadCustomers;$('filterBtn').onclick=openFilter;$('keyword').addEventListener('keydown',e=>{if(e.key==='Enter')loadCustomers()});$('sort').onchange=loadCustomers;$('deleteTestBtn').onclick=deleteTest;renderSegments();loadSummary();loadCustomers();})();
</script></body></html>`;
}

async function handleApi(request, env) {
  await ensureSchema(env.DB);
  const url = new URL(request.url);
  const path = url.pathname;

  if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: corsHeaders });

  if (path === "/api/health" || path === "/health") {
    return json({ ok: true, service: "customer-crm-api", build: BUILD, time: nowIso(), hasDb: !!env.DB, admin_url: url.origin + "/admin?token=" + getAdminToken(env) });
  }

  if (path === "/api/debug-env") return json({ ok: true, hasDB: !!env.DB, hasSyncToken: !!env.SYNC_TOKEN, keys: Object.keys(env).sort() });

  if (path === "/api/customers" && request.method === "GET") return json(await listCustomers(env, url));

  if (path.startsWith("/api/customers/") && request.method === "GET") {
    const customerId = decodeURIComponent(path.replace("/api/customers/", ""));
    return json(await getCustomerDetail(env, customerId));
  }

  if (path === "/api/customers/upsert" && request.method === "POST") {
    if (!isAdmin(request, env)) return json({ ok: false, message: "Unauthorized" }, 401);
    return json(await upsertCustomersFromPayload(env, await readJson(request)));
  }

  if (path === "/api/customers/delete-test" && request.method === "POST") {
    if (!isAdmin(request, env)) return json({ ok: false, message: "Unauthorized" }, 401);
    return json(await deleteCustomers(env, ["26000099", "CRM-LIVE-TEST-001"]));
  }

  if (path === "/api/customers/delete" && request.method === "POST") {
    if (!isAdmin(request, env)) return json({ ok: false, message: "Unauthorized" }, 401);
    const body = await readJson(request);
    return json(await deleteCustomers(env, body.customer_ids || body.customer_id || body.ids || []));
  }

  if (path === "/api/segments/summary" && request.method === "GET") return json(await getSegmentSummary(env));

  if (path === "/api/sync/customers/upsert" && request.method === "POST") {
    const reqToken = getSyncTokenFromRequest(request);
    const workerToken = env.SYNC_TOKEN;
    if (!workerToken) return json({ ok: false, message: "SYNC_TOKEN is not configured" }, 500);
    if (!reqToken || reqToken !== workerToken) {
      return json({ ok: false, message: "Unauthorized", debug: { hasRequestToken: !!reqToken, requestTokenLength: reqToken ? reqToken.length : 0, hasWorkerToken: !!workerToken, workerTokenLength: workerToken ? workerToken.length : 0, tokensMatch: !!reqToken && !!workerToken && reqToken === workerToken } }, 401);
    }
    return json(await upsertCustomersFromPayload(env, await readJson(request)));
  }

  return json({ ok: false, message: "Not Found" }, 404);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    try {
      if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: corsHeaders });

      if (url.pathname === "/" || url.pathname === "/health") {
        await ensureSchema(env.DB);
        return json({ ok: true, service: "customer-crm-api", build: BUILD, time: nowIso(), hasDb: !!env.DB, admin_url: url.origin + "/admin?token=" + getAdminToken(env) });
      }

      if (url.pathname === "/admin") {
        await ensureSchema(env.DB);
        if (!isAdmin(request, env)) {
          return html(`<div style="font-family:sans-serif;padding:24px"><h1>Unauthorized</h1><p>管理画面を開くには token が必要です。</p><p><a href="/admin?token=${getAdminToken(env)}">/admin?token=${getAdminToken(env)}</a></p></div>`, 401);
        }
        return html(adminPage());
      }

      if (url.pathname.startsWith("/api/")) return await handleApi(request, env);

      return json({ ok: false, message: "Not Found" }, 404);
    } catch (error) {
      return json({ ok: false, error: error && error.stack ? error.stack : error && error.message ? error.message : String(error), build: BUILD }, 500);
    }
  }
};