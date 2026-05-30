// ======================================================
// CUSTOMER CRM API / COMPLETE WORKER
// build: customer-crm-api-complete-20260517-line-chat-api-fix-01
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

const BUILD = "customer-crm-api-complete-20260517-line-chat-api-fix-01";
const DEFAULT_ADMIN_TOKEN = "mizuno-admin-2026-secret-001";
const DEFAULT_INTERNAL_TOKEN = "mizuno-reservation-bridge-2026-secret-001";
const DEFAULT_LINE_WORKER_BASE = "https://line-webhook-worker.ohw3rz5578d277e.workers.dev";

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

function parseJson(value, fallback = {}) {
  try {
    if (value === undefined || value === null || value === "") return fallback;
    if (typeof value === "object") return value;
    return JSON.parse(String(value));
  } catch (_) {
    return fallback;
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

  await db.prepare(`
    CREATE TABLE IF NOT EXISTS customer_line_messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      message_key TEXT,
      customer_id TEXT,
      line_user_id TEXT,
      direction TEXT,
      message_type TEXT,
      message_text TEXT,
      sender_name TEXT,
      sent_at TEXT,
      raw_json TEXT,
      created_at TEXT
    )
  `).run();

  for (const col of [
    "message_key TEXT",
    "customer_id TEXT",
    "line_user_id TEXT",
    "direction TEXT",
    "message_type TEXT",
    "message_text TEXT",
    "sender_name TEXT",
    "sent_at TEXT",
    "raw_json TEXT",
    "created_at TEXT"
  ]) {
    await addColumn(db, "customer_line_messages", col);
  }

  await db.prepare(`
    CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_line_messages_key
    ON customer_line_messages(message_key)
  `).run();

  await db.prepare(`
    CREATE INDEX IF NOT EXISTS idx_customer_line_messages_customer
    ON customer_line_messages(customer_id, sent_at)
  `).run();

  await db.prepare(`
    CREATE INDEX IF NOT EXISTS idx_customer_line_messages_line_user
    ON customer_line_messages(line_user_id, sent_at)
  `).run();
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
    await upsertLineMessagesFromPayload(env, customer, item);
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


async function upsertLineMessagesFromPayload(env, customer, raw) {
  const messages = Array.isArray(raw.line_messages) ? raw.line_messages : [];
  if (!messages.length) return { ok: true, inserted: 0 };

  let count = 0;
  for (const msg of messages) {
    const sentAt = normalizeDate(msg.sent_at || msg.created_at || msg.event_timestamp || raw.created_at) || nowIso();
    const textBody = text(msg.text || msg.message_text || msg.body || "");
    const messageType = text(msg.message_type || msg.type || "text") || "text";
    const direction = text(msg.direction || "inbound") || "inbound";
    const lineUserId = text(msg.line_user_id || msg.user_id || customer.line_user_id);
    const key = text(msg.message_key || msg.webhook_event_id || msg.event_id || msg.message_id) ||
      ["line", customer.customer_id, lineUserId, direction, messageType, sentAt, textBody.slice(0, 80)].join("|");

    await env.DB.prepare(`
      INSERT INTO customer_line_messages (
        message_key,
        customer_id,
        line_user_id,
        direction,
        message_type,
        message_text,
        sender_name,
        sent_at,
        raw_json,
        created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(message_key) DO UPDATE SET
        customer_id = COALESCE(excluded.customer_id, customer_line_messages.customer_id),
        line_user_id = COALESCE(excluded.line_user_id, customer_line_messages.line_user_id),
        direction = excluded.direction,
        message_type = excluded.message_type,
        message_text = excluded.message_text,
        sender_name = COALESCE(excluded.sender_name, customer_line_messages.sender_name),
        sent_at = excluded.sent_at,
        raw_json = excluded.raw_json
    `).bind(
      key,
      customer.customer_id,
      lineUserId || null,
      direction,
      messageType,
      textBody,
      text(msg.sender_name || msg.display_name || customer.name) || null,
      sentAt,
      JSON.stringify(msg),
      nowIso()
    ).run();

    count++;
  }

  return { ok: true, inserted: count };
}

function normalizeLineHistoryMessages(items) {
  return (Array.isArray(items) ? items : []).map((m) => {
    const raw = parseJson(m.raw_json, {});

    const rawDirection = text(m.direction || m.sender || m.sender_type || raw.direction || "inbound").toLowerCase();
    const direction =
      ["outbound", "reply", "admin", "staff", "owner", "shop", "operator", "sent"].includes(rawDirection)
        ? "outbound"
        : "inbound";

    const messageType = text(m.message_type || m.type || raw.message_type || (raw.message && raw.message.type) || "text") || "text";
    const messageText =
      text(m.message_text || m.text || raw.message_text || raw.text || (raw.message && raw.message.text) || "") ||
      (messageType && messageType !== "text" ? "[" + messageType + "]" : "");

    const sentAt =
      text(m.event_time_jst) ||
      text(m.sent_at) ||
      text(m.created_at) ||
      text(m.timestamp) ||
      text(m.event_timestamp) ||
      text(raw.event_time_jst) ||
      text(raw.sent_at) ||
      text(raw.created_at) ||
      text(raw.event_timestamp);

    return {
      id: m.id || m.message_id || m.webhook_event_id || m.event_id || "",
      message_key: text(m.message_key || m.webhook_event_id || m.event_id || m.message_id),
      line_user_id: text(m.line_user_id || m.user_id || raw.line_user_id || raw.user_id),
      direction,
      message_type: messageType,
      message_text: messageText,
      sender_name: text(m.sender_name || m.display_name || raw.display_name || (direction === "outbound" ? "運営" : "お客様")),
      sent_at: sentAt,
      raw_json: raw
    };
  }).sort((a, b) => String(a.sent_at || "").localeCompare(String(b.sent_at || "")));
}

async function getLocalCustomerLineHistory(env, customer) {
  const params = [];
  let where = "customer_id = ?";
  params.push(customer.customer_id);

  if (text(customer.line_user_id)) {
    where = "(customer_id = ? OR line_user_id = ?)";
    params.push(text(customer.line_user_id));
  }

  const result = await env.DB.prepare(`
    SELECT *
    FROM customer_line_messages
    WHERE ${where}
    ORDER BY COALESCE(sent_at, created_at) ASC, id ASC
    LIMIT 300
  `).bind(...params).all();

  return normalizeLineHistoryMessages(result.results || []);
}

async function fetchRemoteLineHistory(env, customer) {
  const lineUserId = text(customer.line_user_id);
  if (!lineUserId) {
    return {
      ok: false,
      connected: false,
      message: "この顧客には line_user_id がまだありません。LINE連携後に履歴を表示できます。",
      messages: [],
      debug: [{ step: "missing_line_user_id" }]
    };
  }

  const adminToken = getAdminToken(env);
  const internalToken =
    text(env.LINE_INTERNAL_TOKEN) ||
    text(env.LINE_WORKER_INTERNAL_TOKEN) ||
    text(env.RESERVATION_INTERNAL_TOKEN) ||
    DEFAULT_INTERNAL_TOKEN;

  const baseCandidates = Array.from(new Set([
    text(env.LINE_HISTORY_API_BASE),
    text(env.LINE_WEBHOOK_WORKER_BASE),
    text(env.LINE_WORKER_BASE),
    DEFAULT_LINE_WORKER_BASE
  ].filter(Boolean).map((v) => v.replace(/\/+$/, ""))));

  const path = "/api/internal/customer-line-history";
  const debug = [];

  // 1) Service Binding があれば最優先
  if (env.LINE_SERVICE && typeof env.LINE_SERVICE.fetch === "function") {
    try {
      const url = new URL("https://line-service.internal" + path);
      url.searchParams.set("line_user_id", lineUserId);
      url.searchParams.set("user_id", lineUserId);

      const res = await env.LINE_SERVICE.fetch(new Request(url.toString(), {
        method: "GET",
        headers: {
          "x-internal-token": internalToken,
          "x-admin-token": adminToken,
          "authorization": "Bearer " + internalToken
        }
      }));

      const rawText = await res.text();
      let data = {};
      try { data = rawText ? JSON.parse(rawText) : {}; } catch (_) { data = { raw: rawText }; }

      debug.push({ source: "LINE_SERVICE", status: res.status, ok: res.ok, count: Array.isArray(data.items || data.messages) ? (data.items || data.messages).length : 0 });

      if (res.ok && data && data.ok !== false) {
        return {
          ok: true,
          connected: true,
          source: "LINE_SERVICE" + path,
          messages: normalizeLineHistoryMessages(data.messages || data.items || []),
          debug
        };
      }
    } catch (e) {
      debug.push({ source: "LINE_SERVICE", status: 0, message: e && e.message ? e.message : String(e) });
    }
  }

  // 2) public workers.dev URL
  for (const base of baseCandidates) {
    try {
      const url = new URL(base + path);
      url.searchParams.set("line_user_id", lineUserId);
      url.searchParams.set("user_id", lineUserId);
      // line-webhook-worker はこの token で直接テスト成功済みなので、まず固定管理トークンで通す
      url.searchParams.set("token", DEFAULT_ADMIN_TOKEN);

      const res = await fetch(url.toString(), {
        method: "GET",
        headers: {
          "x-internal-token": internalToken,
          "x-admin-token": DEFAULT_ADMIN_TOKEN,
          "authorization": "Bearer " + internalToken,
          "cache-control": "no-cache"
        }
      });

      const rawText = await res.text();
      let data = {};
      try { data = rawText ? JSON.parse(rawText) : {}; } catch (_) { data = { raw: rawText }; }

      const arr = data.messages || data.items || [];
      debug.push({
        source: base + path,
        status: res.status,
        ok: res.ok,
        data_ok: data && data.ok,
        count: Array.isArray(arr) ? arr.length : 0,
        message: data && (data.message || data.error) ? (data.message || data.error) : ""
      });

      if (res.ok && data && data.ok !== false) {
        return {
          ok: true,
          connected: true,
          source: base + path,
          messages: normalizeLineHistoryMessages(arr),
          debug
        };
      }
    } catch (e) {
      debug.push({ source: base + path, status: 0, message: e && e.message ? e.message : String(e) });
    }
  }

  return {
    ok: false,
    connected: false,
    message: "LINE履歴APIに接続できませんでした。LINEワーカー単体は成功しているため、CRM側からのfetchまたは認証で停止しています。",
    messages: [],
    debug
  };
}

async function getCustomerLineHistory(env, customer) {
  const localMessages = await getLocalCustomerLineHistory(env, customer);
  const remote = await fetchRemoteLineHistory(env, customer);

  const combined = normalizeLineHistoryMessages([
    ...localMessages,
    ...((remote && remote.messages) || [])
  ]);

  const seen = new Set();
  const messages = [];
  for (const msg of combined) {
    const key = msg.message_key || [msg.line_user_id, msg.direction, msg.message_type, msg.sent_at, msg.message_text].join("|");
    if (seen.has(key)) continue;
    seen.add(key);
    messages.push(msg);
  }

  return {
    ok: true,
    connected: !!(remote && remote.connected),
    source: remote && remote.source ? remote.source : "customer_crm_local",
    message: messages.length ? "" : ((remote && remote.message) || "LINE履歴はまだありません。"),
    line_user_id: text(customer.line_user_id),
    count: messages.length,
    messages
  };
}


async function getCustomerDetail(env, customerId) {
  const customer = await env.DB.prepare(`${customerSelectSql()} WHERE customer_id=? LIMIT 1`).bind(customerId).first();
  if (!customer) return { ok: false, message: "customer not found" };

  const reservations = await env.DB.prepare("SELECT * FROM customer_reservations WHERE customer_id=? ORDER BY shoot_date DESC, updated_at DESC LIMIT 100").bind(customerId).all();
  const items = await env.DB.prepare("SELECT * FROM customer_items WHERE customer_id=? ORDER BY purchase_date DESC, updated_at DESC LIMIT 200").bind(customerId).all();
  const timeline = await env.DB.prepare("SELECT * FROM customer_timeline WHERE customer_id=? ORDER BY COALESCE(event_date, created_at) DESC, id DESC LIMIT 100").bind(customerId).all();
  const tags = await env.DB.prepare("SELECT tag FROM customer_tags WHERE customer_id=? ORDER BY tag ASC").bind(customerId).all();
  const lineHistory = await getCustomerLineHistory(env, customer);

  return {
    ok: true,
    customer,
    reservations: reservations.results || [],
    items: items.results || [],
    timeline: timeline.results || [],
    line_history: lineHistory,
    tags: (tags.results || []).map(r => r.tag)
  };
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
    await env.DB.prepare("DELETE FROM customer_line_messages WHERE customer_id=?").bind(id).run();
    await env.DB.prepare("DELETE FROM customer_tags WHERE customer_id=?").bind(id).run();
  }
  return { ok: true, deleted: cleanIds.length, customer_ids: cleanIds };
}

function adminPage() {
  return `<!doctype html><html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><title>顧客管理CRM</title><style>
:root{--bg:#f6f7fb;--surface:#fff;--surface2:#f1f5f9;--text:#111827;--muted:#6b7280;--primary:#028760;--border:#e5e7eb;--danger:#dc2626}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","Noto Sans JP",sans-serif;padding-bottom:32px}.app{max-width:1280px;margin:auto;padding:14px}.header{display:flex;justify-content:space-between;gap:12px;align-items:flex-start;margin-bottom:12px}.title{font-size:30px;font-weight:950;letter-spacing:-.03em}.sub{color:var(--muted);font-size:.86rem;line-height:1.55}.card,.box,.stat{background:var(--surface);border:1px solid var(--border);border-radius:20px;padding:14px;box-shadow:0 8px 24px rgba(15,23,42,.06)}.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:12px 0}.stat b{font-size:24px}.stat small{display:block;color:var(--muted);font-weight:800;margin-top:4px}.marketing{display:grid;grid-template-columns:1.25fr 1fr;gap:10px;margin:12px 0}.toolbar{display:grid;grid-template-columns:1fr auto auto auto;gap:8px;margin:12px 0}.input,select,textarea{width:100%;min-height:44px;border:1px solid var(--border);border-radius:14px;background:#fff;padding:10px 12px;font-size:16px}.btn{border:1px solid var(--border);border-radius:14px;background:#fff;padding:10px 13px;font-weight:900;cursor:pointer}.primary{background:var(--primary);color:#fff;border-color:var(--primary)}.danger{background:#fee2e2;color:#991b1b;border-color:#fecaca}.chips,.filter-summary{display:flex;gap:8px;flex-wrap:wrap;margin:10px 0}.chip{border:1px solid var(--border);background:#fff;border-radius:999px;padding:8px 11px;font-weight:900;font-size:.82rem;cursor:pointer}.chip.active{background:var(--primary);border-color:var(--primary);color:#fff}.tablewrap{overflow:auto;background:#fff;border:1px solid var(--border);border-radius:20px}table{width:100%;border-collapse:collapse;min-width:760px}th,td{border-bottom:1px solid var(--border);padding:10px;text-align:left;vertical-align:top}th{font-size:.78rem;color:var(--muted);background:#f8fafc;position:sticky;top:0;z-index:1}.badge{display:inline-block;border-radius:999px;background:var(--surface2);padding:4px 8px;font-size:.76rem;font-weight:900;margin:2px 2px 2px 0}.name{font-weight:950}.money{font-weight:950}.rank-row{display:grid;grid-template-columns:1fr auto auto;gap:8px;padding:8px;border:1px solid var(--border);border-radius:12px;background:#fff;margin-top:6px}.modal-bg{display:none;position:fixed;inset:0;background:rgba(15,23,42,.42);z-index:20}.modal{display:none;position:fixed;right:0;top:0;bottom:0;width:min(92vw,780px);background:#fff;z-index:21;overflow:auto;padding:18px;box-shadow:-20px 0 60px rgba(15,23,42,.2)}.filter-modal{display:none;position:fixed;left:50%;top:50%;transform:translate(-50%,-50%);width:min(94vw,860px);max-height:90vh;overflow:auto;background:#fff;border-radius:24px;z-index:22;padding:18px;box-shadow:0 24px 80px rgba(15,23,42,.25)}.modal.show,.modal-bg.show,.filter-modal.show{display:block}.detail-grid,.filter-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}.kv{background:#f8fafc;border:1px solid var(--border);border-radius:14px;padding:10px}.kv .k{font-size:.76rem;color:var(--muted);font-weight:900}.kv .v{font-weight:900;margin-top:4px;word-break:break-word}.timeline{display:grid;gap:8px}.timeline-item{border:1px solid var(--border);border-radius:14px;padding:10px;background:#fff}.actions{display:flex;justify-content:space-between;gap:8px;align-items:center;margin:12px 0;position:sticky;bottom:0;background:#fff;padding:10px 0}/* ======================================================
MOBILE UIX REFINEMENT
====================================================== */
.kpi-scroll-hint{display:none}

.customer-line{
  display:flex;
  align-items:center;
  justify-content:space-between;
  gap:12px;
}
.customer-main-name{
  font-size:1.02rem;
  font-weight:950;
}
.customer-subline{
  color:var(--muted);
  font-size:.82rem;
  margin-top:3px;
}
.public-ok{
  background:#dcfce7;
  color:#166534;
}
.public-ng{
  background:#f1f5f9;
  color:#64748b;
}
.repeat-pill{
  display:inline-flex;
  align-items:center;
  justify-content:center;
  min-width:72px;
  border-radius:999px;
  background:#f8fafc;
  border:1px solid var(--border);
  padding:8px 12px;
  font-weight:950;
}
.chat-box{
  background:#eaf3ff;
  border:1px solid #dbeafe;
  border-radius:18px;
  padding:10px;
  max-height:430px;
  overflow:auto;
}
.chat-row{
  display:flex;
  margin:8px 0;
}
.chat-row.inbound{
  justify-content:flex-start;
}
.chat-row.outbound{
  justify-content:flex-end;
}
.chat-bubble{
  max-width:82%;
  border-radius:18px;
  padding:9px 11px;
  line-height:1.55;
  white-space:pre-wrap;
  word-break:break-word;
  box-shadow:0 2px 8px rgba(15,23,42,.06);
}
.chat-row.inbound .chat-bubble{
  background:#fff;
  border-bottom-left-radius:6px;
}
.chat-row.outbound .chat-bubble{
  background:#06c755;
  color:#fff;
  border-bottom-right-radius:6px;
}
.chat-meta{
  font-size:.68rem;
  opacity:.72;
  margin-top:4px;
}
.detail-section-title{
  display:flex;
  justify-content:space-between;
  gap:8px;
  align-items:center;
  margin-top:16px;
}

@media(max-width:820px){
body{background:#f4f6fb}
.app{padding:12px 10px 24px}
.header{display:block;background:linear-gradient(180deg,#fff 0%,#f8fafc 100%);border:1px solid var(--border);border-radius:22px;padding:14px;box-shadow:0 8px 24px rgba(15,23,42,.05);margin-bottom:10px}
.title{font-size:25px;line-height:1.08;letter-spacing:-.04em;margin-bottom:5px}
.header .sub{font-size:.82rem;line-height:1.45;max-width:95%}
.header>div:last-child{display:grid;grid-template-columns:1fr 1.25fr;gap:8px;margin-top:12px}
.header .btn{min-height:42px;padding:9px 10px;border-radius:16px;font-size:.9rem}
.box#status{padding:11px 13px;border-radius:18px;font-size:.88rem;margin-bottom:8px}

#summary.grid{display:flex;gap:8px;overflow-x:auto;-webkit-overflow-scrolling:touch;scroll-snap-type:x mandatory;padding:2px 2px 10px;margin:8px -2px 8px}
#summary.grid::-webkit-scrollbar{display:none}
#summary .stat{min-width:148px;max-width:148px;scroll-snap-align:start;border-radius:18px;padding:12px;min-height:112px}
#summary .stat .sub{font-size:.76rem;line-height:1.25;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
#summary .stat b{display:block;font-size:25px;line-height:1.05;margin:7px 0 4px;letter-spacing:-.03em}
#summary .stat small{font-size:.72rem;line-height:1.2;min-height:1.2em}
.kpi-scroll-hint{display:block;color:var(--muted);font-size:.74rem;margin:-2px 0 8px 4px}

.marketing{grid-template-columns:1fr;gap:8px;margin:8px 0}
.marketing .box{border-radius:20px;padding:12px}
.marketing .box>b{font-size:1rem}
#marketingCards.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr))!important;gap:8px;margin:10px 0 0}
#marketingCards .stat{padding:10px;border-radius:16px;min-height:94px}
#marketingCards .stat b{font-size:20px;line-height:1.1}
#marketingCards .stat .sub{font-size:.72rem}
#marketingCards .stat small{font-size:.7rem}
.rank-row{grid-template-columns:1fr auto;align-items:center}

.card{border-radius:22px;padding:12px}
.toolbar{grid-template-columns:1fr auto;gap:8px;margin:4px 0 10px;position:sticky;top:0;z-index:8;background:rgba(246,247,251,.96);backdrop-filter:blur(8px);padding:8px 0}
.toolbar #keyword{grid-column:1 / -1;min-height:46px;border-radius:16px;font-size:15px}
.toolbar #sort{min-height:44px;border-radius:16px;font-size:14px}
.toolbar .btn{min-height:44px;border-radius:16px;padding:9px 12px}
#filterBtn{background:#111827;color:#fff;border-color:#111827}
.chips{overflow-x:auto;flex-wrap:nowrap;-webkit-overflow-scrolling:touch;padding-bottom:4px;margin:8px -2px 10px}
.chips::-webkit-scrollbar{display:none}
.chip{flex:0 0 auto;padding:8px 10px;font-size:.78rem}
.filter-summary{margin:6px 0;overflow-x:auto;flex-wrap:nowrap;-webkit-overflow-scrolling:touch;padding-bottom:4px}
.filter-summary::-webkit-scrollbar{display:none}
.filter-summary .badge{flex:0 0 auto}

.tablewrap{border:0;background:transparent;overflow:visible;border-radius:0}
table{min-width:0;display:block}
thead{display:none}
tbody{display:grid;gap:10px}
tr{display:block;background:#fff;border:1px solid var(--border);border-radius:18px;padding:12px;box-shadow:0 8px 18px rgba(15,23,42,.05)}
td{display:grid;grid-template-columns:86px 1fr;gap:8px;border-bottom:0;padding:6px 0;font-size:.88rem}
td:before{color:var(--muted);font-size:.72rem;font-weight:900;padding-top:2px}
td:nth-child(1):before{content:"顧客"}
td:nth-child(2):before{content:"連絡先"}
td:nth-child(3):before{content:"撮影履歴"}
td:nth-child(4):before{content:"売上"}
td:nth-child(5):before{content:"流入"}
td:nth-child(6):before{content:"家族情報"}
td:nth-child(7):before{content:"操作"}
td:nth-child(7){grid-template-columns:1fr;padding-top:10px}
td:nth-child(7):before{display:none}
td:nth-child(7) .btn{width:100%;min-height:44px;border-radius:14px}
.name{font-size:1.02rem}
.money{font-size:1.02rem}
.badge{font-size:.7rem;padding:3px 7px}

.detail-grid,.filter-grid{grid-template-columns:1fr}
.modal{width:100%;border-radius:22px 22px 0 0;top:7vh;padding:14px}
.filter-modal{left:0;right:0;bottom:0;top:auto;transform:none;width:100%;border-radius:24px 24px 0 0;max-height:86vh;padding:14px}
.actions{gap:8px;padding:10px 0 calc(10px + env(safe-area-inset-bottom))}
.actions .btn{min-height:44px}
}
@media(max-width:380px){
#summary .stat{min-width:136px;max-width:136px;min-height:106px;padding:10px}
#summary .stat b{font-size:22px}
#marketingCards.grid{grid-template-columns:1fr!important}
td{grid-template-columns:76px 1fr;gap:6px}
}
/* LINE風チャット表示強化 */
.chat-box.line-like,.chat-box{
  background:#8fb4dc;
  background:linear-gradient(180deg,#8fb4dc 0%,#b7d2ec 100%);
  border:0;
  border-radius:18px;
  padding:14px 10px;
  max-height:520px;
  overflow:auto;
}
.chat-empty{
  background:rgba(255,255,255,.86);
  color:#334155;
  border-radius:16px;
  padding:14px;
  line-height:1.6;
}
.chat-date-divider{
  width:max-content;
  max-width:80%;
  margin:12px auto;
  padding:4px 10px;
  border-radius:999px;
  background:rgba(30,41,59,.28);
  color:#fff;
  font-size:.72rem;
  font-weight:900;
}
.chat-row{
  display:flex;
  margin:8px 0;
}
.chat-row.inbound{
  justify-content:flex-start;
}
.chat-row.outbound{
  justify-content:flex-end;
}
.chat-stack{
  max-width:84%;
  display:flex;
  flex-direction:column;
}
.chat-row.outbound .chat-stack{
  align-items:flex-end;
}
.chat-sender{
  font-size:.68rem;
  font-weight:900;
  color:rgba(255,255,255,.9);
  margin:0 8px 3px;
}
.chat-bubble{
  max-width:100%;
  border-radius:18px;
  padding:10px 12px 7px;
  line-height:1.58;
  white-space:pre-wrap;
  word-break:break-word;
  box-shadow:0 2px 8px rgba(15,23,42,.12);
  position:relative;
}
.chat-row.inbound .chat-bubble{
  background:#fff;
  color:#111827;
  border-bottom-left-radius:5px;
}
.chat-row.outbound .chat-bubble{
  background:#06c755;
  color:#111827;
  border-bottom-right-radius:5px;
}
.chat-meta{
  font-size:.66rem;
  opacity:.65;
  margin-top:5px;
  text-align:right;
}
@media(max-width:820px){
  .chat-box.line-like,.chat-box{
    max-height:460px;
    padding:12px 8px;
    border-radius:16px;
  }
  .chat-stack{
    max-width:88%;
  }
  .chat-bubble{
    font-size:.9rem;
  }
}
</style></head><body><div class="app"><div class="header"><div><div class="title">顧客管理CRM</div><div class="sub">予約管理アプリと連携した顧客一覧・撮影履歴・購入履歴・マーケティング抽出</div></div><div><button class="btn" id="reloadBtn">更新</button><button class="btn danger" id="deleteTestBtn">テスト顧客削除</button></div></div><div class="box" id="status">読み込み中...</div><div class="grid" id="summary"></div><div class="kpi-scroll-hint">横にスワイプすると他の指標も見られます</div><div class="marketing"><div class="box"><b>顧客マーケティング指標</b><div class="sub">平均顧客単価・リピート率・購入履歴から次の施策対象を見つけます。</div><div class="grid" id="marketingCards" style="grid-template-columns:repeat(3,1fr)"></div></div><div class="box"><b>購入アイテムランキング</b><div class="sub">プラン・オプション・スタジオ利用料・交通費を購入履歴として保存します。</div><div id="itemRanking"></div></div></div><div class="card"><div class="toolbar"><input class="input" id="keyword" placeholder="名前・ふりがな・電話・メール・LINE名・ジャンルで検索"><select id="sort"><option value="updated_at">更新順</option><option value="last_shoot">最終撮影日順</option><option value="revenue">売上順</option><option value="aov">平均顧客単価順</option><option value="repeat">リピート回数順</option><option value="dormant">休眠日数順</option></select><button class="btn" id="filterBtn">フィルター</button><button class="btn primary" id="searchBtn">検索</button></div><div class="filter-summary" id="filterSummary"></div><div class="chips" id="segments"></div><div class="tablewrap"><table><thead><tr><th>名前</th><th>直近の撮影日</th><th>リピート回数</th><th>写真公開OK</th><th>操作</th></tr></thead><tbody id="tbody"></tbody></table></div></div></div><div class="modal-bg" id="modalBg"></div><div class="modal" id="modal"></div><div class="filter-modal" id="filterModal"></div><script>
(function(){const TOKEN=new URLSearchParams(location.search).get('token')||'';const $=(id)=>document.getElementById(id);let state={items:[],segment:'',summary:{},genres:[],sources:[],itemRanking:[],filters:{genre:'',source:'',min_revenue:'',max_revenue:'',min_repeat:'',min_dormant:'',photo_public_ok:'',has_child:''}};function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(m){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m]})}function yen(v){return '¥'+Number(v||0).toLocaleString('ja-JP')}function pct(v){return Number(v||0).toLocaleString('ja-JP',{maximumFractionDigits:1})+'%'}function api(path,options){options=options||{};options.headers=options.headers||{};if(TOKEN)options.headers['x-admin-token']=TOKEN;if(options.body&&!options.headers['Content-Type'])options.headers['Content-Type']='application/json';const u=new URL(path,location.origin);if(TOKEN&&!u.searchParams.get('token'))u.searchParams.set('token',TOKEN);u.searchParams.set('_',String(Date.now()));return fetch(u.toString(),options).then(r=>r.text().then(t=>{let d={};try{d=t?JSON.parse(t):{}}catch(e){throw Error('JSONではない応答: '+t.slice(0,160))}if(!r.ok||d.ok===false)throw Error(d.message||d.error||('HTTP '+r.status));return d}))}function status(m,e){$('status').textContent=m;$('status').style.color=e?'var(--danger)':'var(--muted)'}function stat(l,v,s){return '<div class="stat"><div class="sub">'+esc(l)+'</div><b>'+esc(v)+'</b><small>'+esc(s||'')+'</small></div>'}function renderSummary(){const s=state.summary||{};$('summary').innerHTML=[stat('顧客数',Number(s.total||0).toLocaleString('ja-JP')+'人','CRM登録済み'),stat('平均顧客単価',yen(s.avg_ltv||0),'顧客LTVの平均'),stat('平均注文単価',yen(s.reservation_aov||s.avg_order_value||0),'撮影1回あたり'),stat('リピート率',pct(s.repeat_rate||0),'2回以上'),stat('休眠180日',Number(s.dormant_180||0).toLocaleString('ja-JP')+'人','再来店施策'),stat('休眠365日',Number(s.dormant_365||0).toLocaleString('ja-JP')+'人','強めの再接触'),stat('購入アイテム数',Number(s.item_count||0).toLocaleString('ja-JP')+'件','プラン/オプション'),stat('累計売上',yen(s.total_revenue||0),'CRM集計')].join('');$('marketingCards').innerHTML=[stat('リピーター',Number(s.repeaters||0).toLocaleString('ja-JP')+'人','再提案候補'),stat('高売上顧客',Number(s.high_value||0).toLocaleString('ja-JP')+'人','10万円以上'),stat('LINE連携率',pct(s.line_rate||0),'LINE接点あり'),stat('写真公開OK率',pct(s.photo_public_rate||0),'作例依頼候補'),stat('購入単価',yen(s.item_avg_amount||0),'アイテム平均'),stat('購入売上',yen(s.item_revenue||0),'アイテム合計')].join('');$('itemRanking').innerHTML=(state.itemRanking||[]).length?state.itemRanking.slice(0,8).map(x=>'<div class="rank-row"><div><b>'+esc(x.item_name)+'</b><div class="sub">'+esc(x.item_category)+'</div></div><div>'+Number(x.count||0)+'件</div><div class="money">'+yen(x.revenue||0)+'</div></div>').join(''):'<div class="sub">購入履歴はまだありません。今後の予約保存でプラン・オプションが蓄積されます。</div>'}function renderSegments(){const list=[['','すべて'],['repeaters','リピーター'],['first_time','初回のみ'],['dormant_90','休眠90日'],['dormant_180','休眠180日'],['dormant_365','休眠365日'],['high_value','高売上'],['omiyamairi','お宮参り'],['shichigosan','七五三'],['line','LINE連携'],['no_phone','電話なし'],['photo_public_ok','写真公開OK']];$('segments').innerHTML=list.map(x=>'<button class="chip '+(state.segment===x[0]?'active':'')+'" data-segment="'+esc(x[0])+'">'+esc(x[1])+'</button>').join('');document.querySelectorAll('[data-segment]').forEach(btn=>btn.onclick=()=>{state.segment=btn.dataset.segment||'';loadCustomers()})}function renderFilterSummary(){const labels=[];if(state.segment)labels.push('セグメント: '+state.segment);if(state.filters.genre)labels.push('ジャンル: '+state.filters.genre);if(state.filters.source)labels.push('流入元: '+state.filters.source);if(state.filters.min_revenue)labels.push('売上 '+Number(state.filters.min_revenue).toLocaleString('ja-JP')+'円以上');if(state.filters.max_revenue)labels.push('売上 '+Number(state.filters.max_revenue).toLocaleString('ja-JP')+'円以下');if(state.filters.min_repeat)labels.push('撮影 '+state.filters.min_repeat+'回以上');if(state.filters.min_dormant)labels.push('休眠 '+state.filters.min_dormant+'日以上');if(state.filters.photo_public_ok==='1')labels.push('写真公開OK');if(state.filters.has_child==='1')labels.push('お子さま情報あり');$('filterSummary').innerHTML=labels.length?labels.map(x=>'<span class="badge">'+esc(x)+'</span>').join('')+'<button class="chip" id="clearFiltersBtn">条件クリア</button>':'<span class="sub">フィルター条件なし</span>';const btn=$('clearFiltersBtn');if(btn)btn.onclick=()=>{state.segment='';state.filters={genre:'',source:'',min_revenue:'',max_revenue:'',min_repeat:'',min_dormant:'',photo_public_ok:'',has_child:''};loadCustomers()}}function renderCustomers(){
  renderFilterSummary();
  const items=state.items||[];
  if(!items.length){
    $('tbody').innerHTML='<tr><td colspan="5">顧客が見つかりません。</td></tr>';
    return;
  }
  $('tbody').innerHTML=items.map(x=>{
    const lineName=x.line_display_name?'<div class="customer-subline">LINE: '+esc(x.line_display_name)+'</div>':'';
    const furigana=x.furigana?'<div class="customer-subline">'+esc(x.furigana)+'</div>':'';
    const last=x.last_shoot_date||'未設定';
    const genre=x.genre_history?'<div class="customer-subline">'+esc(x.genre_history)+'</div>':'';
    const repeat=Number(x.repeat_count||0);
    const publicLabel=x.photo_public_ok?'<span class="badge public-ok">公開OK</span>':'<span class="badge public-ng">未確認</span>';
    return '<tr class="customer-list-row">'+
      '<td><div class="customer-main-name">'+esc(x.name||'名称未設定')+'</div>'+furigana+lineName+'</td>'+
      '<td><b>'+esc(last)+'</b>'+genre+'</td>'+
      '<td><span class="repeat-pill">'+repeat+'回</span><div class="customer-subline">1年以内 '+Number(x.repeat_count_1y||x.repeat_count_365d||0)+'回</div></td>'+
      '<td>'+publicLabel+'</td>'+
      '<td><button class="btn primary" data-detail="'+esc(x.customer_id)+'">詳細</button></td>'+
    '</tr>';
  }).join('');
  document.querySelectorAll('[data-detail]').forEach(btn=>btn.onclick=()=>openDetail(btn.dataset.detail));
}function kv(k,v){return '<div class="kv"><div class="k">'+esc(k)+'</div><div class="v">'+esc(v||'未設定')+'</div></div>'}function openModal(){$('modalBg').classList.add('show');$('modal').classList.add('show')}function closeModal(){$('modalBg').classList.remove('show');$('modal').classList.remove('show');$('filterModal').classList.remove('show')}function openFilter(){$('modalBg').classList.add('show');const genreOptions=['<option value="">すべて</option>'].concat((state.genres||[]).map(g=>'<option value="'+esc(g.genre)+'" '+(state.filters.genre===g.genre?'selected':'')+'>'+esc(g.genre)+'（'+Number(g.count||0)+'件）</option>')).join('');const sourceOptions=['<option value="">すべて</option>'].concat((state.sources||[]).map(s=>'<option value="'+esc(s.source)+'" '+(state.filters.source===s.source?'selected':'')+'>'+esc(s.source)+'（'+Number(s.count||0)+'件）</option>')).join('');$('filterModal').innerHTML='<h2>顧客フィルター</h2><div class="sub">ジャンル・流入元・売上・休眠日数などでマーケティング施策対象を抽出できます。</div><div class="filter-grid" style="margin-top:12px"><div><div class="sub">ジャンル</div><select id="fGenre">'+genreOptions+'</select></div><div><div class="sub">流入元</div><select id="fSource">'+sourceOptions+'</select></div><div><div class="sub">売上下限</div><input class="input" id="fMinRevenue" type="number" value="'+esc(state.filters.min_revenue)+'" placeholder="例: 50000"></div><div><div class="sub">売上上限</div><input class="input" id="fMaxRevenue" type="number" value="'+esc(state.filters.max_revenue)+'" placeholder="例: 150000"></div><div><div class="sub">撮影回数 下限</div><input class="input" id="fMinRepeat" type="number" value="'+esc(state.filters.min_repeat)+'" placeholder="例: 2"></div><div><div class="sub">休眠日数 下限</div><input class="input" id="fMinDormant" type="number" value="'+esc(state.filters.min_dormant)+'" placeholder="例: 180"></div><div><label><input type="checkbox" id="fPhotoOk" '+(state.filters.photo_public_ok==='1'?'checked':'')+'> 写真公開OKのみ</label></div><div><label><input type="checkbox" id="fHasChild" '+(state.filters.has_child==='1'?'checked':'')+'> お子さま情報あり</label></div></div><div class="actions"><button class="btn" id="filterClearBtn">クリア</button><div><button class="btn" id="filterCloseBtn">閉じる</button> <button class="btn primary" id="filterApplyBtn">この条件で抽出</button></div></div>';$('filterModal').classList.add('show');$('filterCloseBtn').onclick=closeModal;$('filterClearBtn').onclick=()=>{state.filters={genre:'',source:'',min_revenue:'',max_revenue:'',min_repeat:'',min_dormant:'',photo_public_ok:'',has_child:''};closeModal();loadCustomers()};$('filterApplyBtn').onclick=()=>{state.filters.genre=$('fGenre').value;state.filters.source=$('fSource').value;state.filters.min_revenue=$('fMinRevenue').value;state.filters.max_revenue=$('fMaxRevenue').value;state.filters.min_repeat=$('fMinRepeat').value;state.filters.min_dormant=$('fMinDormant').value;state.filters.photo_public_ok=$('fPhotoOk').checked?'1':'';state.filters.has_child=$('fHasChild').checked?'1':'';closeModal();loadCustomers()}}function renderChatMessages(lineHistory){
  const lh=lineHistory||{};
  const messages=lh.messages||[];
  if(!messages.length){
    return '<div class="detail-section-title"><h3>LINE履歴</h3><span class="badge">0件</span></div>' +
      '<div class="chat-box"><div class="chat-empty">'+esc(lh.message||'LINE履歴はまだありません。')+'</div></div>';
  }

  let lastDate='';
  function datePart(v){
    const s=String(v||'');
    const m=s.match(/^(\d{4}-\d{2}-\d{2})/);
    return m?m[1]:'';
  }

  return '<div class="detail-section-title"><h3>LINE履歴</h3><span class="badge">'+messages.length+'件</span></div>'+
    '<div class="chat-box line-like">'+messages.map(m=>{
      const dir=(m.direction==='outbound'||m.direction==='reply'||m.direction==='admin')?'outbound':'inbound';
      const body=m.message_text||((m.message_type&&m.message_type!=='text')?'['+m.message_type+']':'');
      const d=datePart(m.sent_at);
      const dateDivider=(d&&d!==lastDate)?('<div class="chat-date-divider">'+esc(d)+'</div>'):'';
      if(d)lastDate=d;
      return dateDivider + '<div class="chat-row '+dir+'">'+
        '<div class="chat-stack">'+
          '<div class="chat-sender">'+esc(dir==='outbound'?'運営側':(m.sender_name||'お客様'))+'</div>'+
          '<div class="chat-bubble">'+
            esc(body||'メッセージ本文なし')+
            '<div class="chat-meta">'+esc(m.sent_at||'')+'</div>'+
          '</div>'+
        '</div>'+
      '</div>';
    }).join('')+'</div>';
}

async function openDetail(id){
  openModal();
  $('modal').innerHTML='<h2>読み込み中...</h2>';
  try{
    const d=await api('/api/customers/'+encodeURIComponent(id));
    const c=d.customer;
    const rs=d.reservations||[];
    const items=d.items||[];
    const tl=d.timeline||[];
    const lh=d.line_history||{};
    $('modal').innerHTML=
      '<div class="actions"><div><h2>'+esc(c.name)+'</h2><div class="sub">'+esc(c.customer_id)+'</div></div><button class="btn" id="closeModalBtn">閉じる</button></div>'+
      '<div class="detail-grid">'+
        kv('ふりがな',c.furigana)+
        kv('LINE表示名',c.line_display_name)+
        kv('LINE user ID',c.line_user_id)+
        kv('電話',c.phone)+
        kv('メール',c.email)+
        kv('住所',c.address)+
        kv('流入元',c.acquisition_source)+
        kv('紹介者',c.referrer)+
        kv('最終撮影日',c.last_shoot_date)+
        kv('撮影回数',Number(c.repeat_count||0)+'回')+
        kv('写真公開',c.photo_public_ok?'OK':'未確認')+
        kv('累計売上',yen(c.total_revenue||0))+
        kv('平均顧客単価',yen(c.avg_order_value||0))+
        kv('休眠日数',Number(c.dormant_days||0)+'日')+
        kv('お子さま1',[c.child1_name,c.child1_birthdate].filter(Boolean).join(' / '))+
        kv('記念日',c.anniversary)+
      '</div>'+
      '<div class="box" style="margin-top:12px"><b>メモ</b><div class="sub" style="white-space:pre-wrap">'+esc(c.memo||'')+'</div></div>'+
      renderChatMessages(lh)+
      '<h3>撮影履歴</h3><div class="timeline">'+
        (rs.length?rs.map(r=>'<div class="timeline-item"><b>'+esc(r.shoot_date||'日付未設定')+' '+esc(r.genre||'')+'</b><div class="sub">'+esc(r.start_time||'')+' '+esc(r.place||'')+' / '+esc(r.plan_label||'')+'</div><div class="money">'+yen(r.total_amount||0)+'</div></div>').join(''):'<div class="sub">撮影履歴はまだありません。</div>')+
      '</div>'+
      '<h3>購入履歴</h3><div class="timeline">'+
        (items.length?items.map(item=>'<div class="timeline-item"><b>'+esc(item.item_name||'購入アイテム')+'</b><div class="sub">'+esc(item.purchase_date||'')+' / '+esc(item.item_category||'')+'</div><div class="money">'+yen(item.item_amount||0)+'</div></div>').join(''):'<div class="sub">購入履歴はまだありません。今後の予約保存でプラン・オプションが蓄積されます。</div>')+
      '</div>'+
      '<h3>タイムライン</h3><div class="timeline">'+
        (tl.length?tl.map(t=>'<div class="timeline-item"><b>'+esc(t.event_date||t.created_at||'')+' '+esc(t.event_title||'')+'</b><div class="sub">'+esc(t.event_type||'')+' / '+yen(t.amount||0)+'</div></div>').join(''):'<div class="sub">タイムラインはまだありません。</div>')+
      '</div>';
    $('closeModalBtn').onclick=closeModal;
  }catch(e){
    $('modal').innerHTML='<h2>エラー</h2><p>'+esc(e.message)+'</p><button class="btn" id="closeModalBtn">閉じる</button>';
    $('closeModalBtn').onclick=closeModal;
  }
}
async function loadSummary(){try{const d=await api('/api/segments/summary');state.summary=d.summary||{};state.genres=d.genres||[];state.sources=d.sources||[];state.itemRanking=d.itemRanking||[];renderSummary()}catch(e){console.warn(e)}}async function loadCustomers(){status('顧客一覧を読み込み中...');renderSegments();const p=new URLSearchParams();const kw=$('keyword').value.trim();if(kw)p.set('keyword',kw);if(state.segment)p.set('segment',state.segment);Object.keys(state.filters).forEach(k=>{if(state.filters[k])p.set(k,state.filters[k])});p.set('sort',$('sort').value||'updated_at');p.set('limit','200');try{const d=await api('/api/customers?'+p.toString());state.items=d.items||[];renderCustomers();status('読み込み完了: '+state.items.length+'件')}catch(e){status('読み込み失敗: '+e.message,true)}}async function deleteTest(){if(!confirm('テスト顧客 26000099 / CRM-LIVE-TEST-001 を削除しますか？'))return;try{await api('/api/customers/delete-test',{method:'POST',body:JSON.stringify({})});await loadSummary();await loadCustomers();status('テスト顧客を削除しました')}catch(e){alert('削除失敗: '+e.message)}}$('modalBg').onclick=closeModal;$('reloadBtn').onclick=()=>{loadSummary();loadCustomers()};$('searchBtn').onclick=loadCustomers;$('filterBtn').onclick=openFilter;$('keyword').addEventListener('keydown',e=>{if(e.key==='Enter')loadCustomers()});$('sort').onchange=loadCustomers;$('deleteTestBtn').onclick=deleteTest;renderSegments();loadSummary();loadCustomers();})();
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

  if (path.startsWith("/api/customers/") && path.endsWith("/line-history") && request.method === "GET") {
    const customerId = decodeURIComponent(path.replace("/api/customers/", "").replace("/line-history", ""));
    const customer = await env.DB.prepare(`${customerSelectSql()} WHERE customer_id=? LIMIT 1`).bind(customerId).first();
    if (!customer) return json({ ok: false, message: "customer not found" }, 404);
    return json(await getCustomerLineHistory(env, customer));
  }

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
          return html(`<div style="font-family:sans-serif;padding:24px"><h1>Unauthorized</h1><p>Googleログインまたは管理者認証が必要です。</p></div>`, 401);
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