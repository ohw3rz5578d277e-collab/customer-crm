export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/api/health") {
      return Response.json({
        ok: true,
        service: "customer-crm-api",
        time: new Date().toISOString()
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
          result = await env.DB.prepare(
            `
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
            `
          ).bind(like, limit).all();
        } else {
          result = await env.DB.prepare(
            `
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
            `
          ).bind(limit).all();
        }

        return Response.json({
          ok: true,
          count: result.results ? result.results.length : 0,
          items: result.results || []
        });
      } catch (error) {
        return Response.json(
          {
            ok: false,
            message: error && error.message ? error.message : "Unknown error"
          },
          { status: 500 }
        );
      }
    }

    return Response.json(
      {
        ok: false,
        message: "Not Found"
      },
      { status: 404 }
    );
  }
};

function nowIso() {
  return new Date().toISOString();
}

function appendLines() {
  const out = [];
  for (let i = 0; i < arguments.length; i++) {
    const v = String(arguments[i] || "").trim();
    if (v) out.push(v);
  }
  return out.join("\n");
}

function normalizeAiGenre(value) {
  const s = String(value || "").trim();
  if (!s) return null;
  if (/お宮参り/.test(s)) return "お宮参り";
  if (/七五三/.test(s)) return "七五三";
  if (/家族/.test(s)) return "家族";
  if (/マタニティ/.test(s)) return "マタニティ";
  if (/ニューボーン/.test(s)) return "ニューボーン";
  return s.replace(/撮影/g, "").trim() || s;
}

function normalizeAiPreferredDate(value) {
  const s = String(value || "").trim();
  if (!s) return null;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return null;
  return s;
}

function normalizeAiPreferredTimeToStartTime(value) {
  const s = String(value || "").trim();
  if (!s) return null;
  if (/^\d{2}:\d{2}$/.test(s)) return s;
  return null;
}

function tokyoYearSuffix() {
  const today = todayTokyo();
  return today.slice(2, 4);
}

function generateCustomerId() {
  const rand = String(Math.floor(Math.random() * 900000) + 100000);
  return tokyoYearSuffix() + rand;
}

function generateReservationId() {
  return "R-" + Date.now() + "-" + Math.floor(Math.random() * 100000);
}

async function getTableColumns(env, tableName) {
  const rs = await env.DB.prepare(`PRAGMA table_info(${tableName})`).all();
  const rows = rs.results || [];
  return rows.map(r => String(r.name));
}

function hasColumn(columns, name) {
  return columns.indexOf(name) >= 0;
}

async function insertRowDynamic(env, tableName, row) {
  const keys = Object.keys(row).filter(function(k) {
    return row[k] !== undefined;
  });
  if (!keys.length) return null;

  const sql = `INSERT INTO ${tableName} (${keys.join(", ")}) VALUES (${keys.map(function() { return "?"; }).join(", ")})`;
  const values = keys.map(function(k) { return row[k]; });
  return env.DB.prepare(sql).bind.apply(env.DB.prepare(sql), values).run();
}

async function updateRowDynamicByKey(env, tableName, row, keyName, keyValue) {
  const keys = Object.keys(row).filter(function(k) {
    return row[k] !== undefined;
  });
  if (!keys.length) return null;

  const setSql = keys.map(function(k) { return k + " = ?"; }).join(", ");
  const sql = `UPDATE ${tableName} SET ${setSql} WHERE ${keyName} = ?`;
  const stmt = env.DB.prepare(sql);
  const values = keys.map(function(k) { return row[k]; });
  values.push(keyValue);
  return stmt.bind.apply(stmt, values).run();
}

async function findExistingCustomerByName(env, columns, customerName) {
  const nameCol = hasColumn(columns, "customer_name")
    ? "customer_name"
    : (hasColumn(columns, "name") ? "name" : null);

  if (!nameCol || !customerName) return null;

  const row = await env.DB.prepare(`
    SELECT *
    FROM app_customers
    WHERE ${nameCol} = ?
    ORDER BY rowid DESC
    LIMIT 1
  `).bind(customerName).first();

  return row || null;
}

async function createOrFindCustomerFromDraft(env, mergedDraft) {
  const columns = await getTableColumns(env, "app_customers");
  const customerName = String(mergedDraft.customer_name || "").trim();

  if (!customerName) {
    return {
      customer_id: null,
      customer_row: null
    };
  }

  const existing = await findExistingCustomerByName(env, columns, customerName);
  if (existing) {
    return {
      customer_id: existing.customer_id || null,
      customer_row: existing
    };
  }

  const row = {};
  let newCustomerId = null;

  if (hasColumn(columns, "customer_id")) {
    newCustomerId = generateCustomerId();
    row.customer_id = newCustomerId;
  }

  if (hasColumn(columns, "customer_name")) row.customer_name = customerName;
  if (hasColumn(columns, "name")) row.name = customerName;
  if (hasColumn(columns, "memo")) {
    row.memo = appendLines(
      mergedDraft.memo,
      mergedDraft.child_names ? "お子様名: " + mergedDraft.child_names : "",
      mergedDraft.child_info ? "お子様情報: " + mergedDraft.child_info : ""
    ) || null;
  }
  if (hasColumn(columns, "created_at")) row.created_at = nowIso();
  if (hasColumn(columns, "updated_at")) row.updated_at = nowIso();

  await insertRowDynamic(env, "app_customers", row);

  return {
    customer_id: newCustomerId,
    customer_row: row
  };
}

function mergeDraftWithOverrides(draft, body) {
  return {
    customer_name: body.customer_name != null ? body.customer_name : draft.customer_name,
    genre: body.genre != null ? body.genre : draft.genre,
    preferred_date: body.preferred_date != null ? body.preferred_date : draft.preferred_date,
    preferred_time: body.preferred_time != null ? body.preferred_time : draft.preferred_time,
    plan_label: body.plan_label != null ? body.plan_label : draft.plan_label,
    plan_amount: body.plan_amount != null ? body.plan_amount : draft.plan_amount,
    traffic_amount: body.traffic_amount != null ? body.traffic_amount : draft.traffic_amount,
    total_amount: body.total_amount != null ? body.total_amount : draft.total_amount,
    attendee_count: body.attendee_count != null ? body.attendee_count : draft.attendee_count,
    attendee_people: body.attendee_people != null ? body.attendee_people : draft.attendee_people,
    child_names: body.child_names != null ? body.child_names : draft.child_names,
    child_info: body.child_info != null ? body.child_info : draft.child_info,
    shoot_location: body.shoot_location != null ? body.shoot_location : draft.shoot_location,
    shoot_address: body.shoot_address != null ? body.shoot_address : draft.shoot_address,
    request_notes: body.request_notes != null ? body.request_notes : draft.request_notes,
    memo: body.memo != null ? body.memo : draft.memo,
    missing_fields_json: body.missing_fields_json != null ? body.missing_fields_json : draft.missing_fields_json,
    confidence: body.confidence != null ? body.confidence : draft.confidence,
    raw_text: body.raw_text != null ? body.raw_text : draft.raw_text,
    customer_label: body.customer_label != null ? body.customer_label : draft.customer_label
  };
}

async function createReservationFromApprovedDraft(env, mergedDraft, customerId, sourceDraft) {
  const columns = await getTableColumns(env, "app_reservations");
  const reservationId = hasColumn(columns, "reservation_id") ? generateReservationId() : null;

  const normalizedGenre = normalizeAiGenre(mergedDraft.genre);
  const normalizedDate = normalizeAiPreferredDate(mergedDraft.preferred_date);
  const normalizedStartTime = normalizeAiPreferredTimeToStartTime(mergedDraft.preferred_time);

  const row = {};

  if (hasColumn(columns, "reservation_id")) row.reservation_id = reservationId;
  if (hasColumn(columns, "customer_id")) row.customer_id = customerId || null;
  if (hasColumn(columns, "customer_name")) row.customer_name = String(mergedDraft.customer_name || "").trim() || String(mergedDraft.customer_label || "").trim() || null;
  if (hasColumn(columns, "status")) row.status = "AI承認済み";
  if (hasColumn(columns, "genre")) row.genre = normalizedGenre || null;
  if (hasColumn(columns, "shoot_date")) row.shoot_date = normalizedDate || null;
  if (hasColumn(columns, "start_time")) row.start_time = normalizedStartTime || null;
  if (hasColumn(columns, "end_time")) row.end_time = null;
  if (hasColumn(columns, "place")) row.place = String(mergedDraft.shoot_location || "").trim() || null;
  if (hasColumn(columns, "plan_amount")) row.plan_amount = mergedDraft.plan_amount == null || mergedDraft.plan_amount === "" ? 0 : toNumber(mergedDraft.plan_amount);
  if (hasColumn(columns, "traffic_amount")) row.traffic_amount = mergedDraft.traffic_amount == null || mergedDraft.traffic_amount === "" ? 0 : toNumber(mergedDraft.traffic_amount);
  if (hasColumn(columns, "total_amount")) {
    const total = mergedDraft.total_amount == null || mergedDraft.total_amount === ""
      ? toNumber(mergedDraft.plan_amount) + toNumber(mergedDraft.traffic_amount)
      : toNumber(mergedDraft.total_amount);
    row.total_amount = total;
  }
  if (hasColumn(columns, "ai_summary")) {
    row.ai_summary = appendLines(
      "AI予約承認から登録",
      mergedDraft.plan_label ? "プラン: " + mergedDraft.plan_label : "",
      mergedDraft.preferred_time ? "希望時間: " + mergedDraft.preferred_time : "",
      mergedDraft.attendee_people ? "参加者: " + mergedDraft.attendee_people : "",
      mergedDraft.child_names ? "お子様名: " + mergedDraft.child_names : "",
      mergedDraft.child_info ? "お子様情報: " + mergedDraft.child_info : "",
      mergedDraft.request_notes ? "要望: " + mergedDraft.request_notes : "",
      mergedDraft.memo ? "メモ: " + mergedDraft.memo : "",
      sourceDraft && sourceDraft.raw_text ? "原文:\n" + sourceDraft.raw_text : ""
    ) || null;
  }
  if (hasColumn(columns, "updated_at")) row.updated_at = nowIso();

  await insertRowDynamic(env, "app_reservations", row);

  return {
    reservation_id: reservationId,
    normalized_genre: normalizedGenre,
    normalized_date: normalizedDate,
    normalized_start_time: normalizedStartTime
  };
}

async function upsertReservationDetailFromDraft(env, reservationId, mergedDraft) {
  await ensureAdminTables(env);

  const memoText = appendLines(
    mergedDraft.memo,
    mergedDraft.preferred_time ? "希望時間: " + mergedDraft.preferred_time : ""
  );

  await env.DB.prepare(`
    INSERT INTO reservation_detail_notes (
      reservation_id,
      shoot_location_name,
      shoot_address,
      request_notes,
      image_urls_json,
      attendee_count,
      attendee_people,
      child_names,
      child_info,
      plan_label,
      memo,
      customer_notes,
      created_at,
      updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
    ON CONFLICT(reservation_id) DO UPDATE SET
      shoot_location_name = excluded.shoot_location_name,
      shoot_address = excluded.shoot_address,
      request_notes = excluded.request_notes,
      image_urls_json = excluded.image_urls_json,
      attendee_count = excluded.attendee_count,
      attendee_people = excluded.attendee_people,
      child_names = excluded.child_names,
      child_info = excluded.child_info,
      plan_label = excluded.plan_label,
      memo = excluded.memo,
      customer_notes = excluded.customer_notes,
      updated_at = datetime('now')
  `).bind(
    reservationId,
    String(mergedDraft.shoot_location || "").trim() || null,
    String(mergedDraft.shoot_address || "").trim() || null,
    String(mergedDraft.request_notes || "").trim() || null,
    JSON.stringify([]),
    mergedDraft.attendee_count == null || mergedDraft.attendee_count === "" ? null : toNumber(mergedDraft.attendee_count),
    String(mergedDraft.attendee_people || "").trim() || null,
    String(mergedDraft.child_names || "").trim() || null,
    String(mergedDraft.child_info || "").trim() || null,
    String(mergedDraft.plan_label || "").trim() || null,
    memoText || null,
    null
  ).run();
}

async function approveAiReservationDraft(env, body) {
  await ensureAdminTables(env);

  const draftId = toNumber(body.draft_id);
  if (!draftId) return { ok: false, error: "draft_id is required" };

  const draft = await env.DB.prepare(`
    SELECT
      d.*,
      i.customer_label,
      i.raw_text,
      i.id AS intake_id_ref
    FROM ai_reservation_drafts d
    LEFT JOIN ai_reservation_intakes i
      ON i.id = d.intake_id
    WHERE d.id = ?
    LIMIT 1
  `).bind(draftId).first();

  if (!draft) return { ok: false, error: "draft not found" };
  if (draft.approval_status === "approved") {
    return {
      ok: false,
      error: "already approved",
      approved_customer_id: draft.approved_customer_id,
      approved_reservation_id: draft.approved_reservation_id
    };
  }

  const merged = mergeDraftWithOverrides(draft, body);
  const customerResult = await createOrFindCustomerFromDraft(env, merged);
  const reservationResult = await createReservationFromApprovedDraft(env, merged, customerResult.customer_id, draft);

  if (reservationResult.reservation_id) {
    await upsertReservationDetailFromDraft(env, reservationResult.reservation_id, merged);
  }

  const missingFields = [];
  if (!String(merged.customer_name || "").trim()) missingFields.push("customer_name");
  if (!normalizeAiPreferredDate(merged.preferred_date)) missingFields.push("preferred_date");
  if (!normalizeAiGenre(merged.genre)) missingFields.push("genre");

  await updateRowDynamicByKey(env, "ai_reservation_drafts", {
    customer_name: String(merged.customer_name || "").trim() || null,
    genre: normalizeAiGenre(merged.genre),
    preferred_date: normalizeAiPreferredDate(merged.preferred_date),
    preferred_time: String(merged.preferred_time || "").trim() || null,
    plan_label: String(merged.plan_label || "").trim() || null,
    plan_amount: merged.plan_amount == null || merged.plan_amount === "" ? null : toNumber(merged.plan_amount),
    traffic_amount: merged.traffic_amount == null || merged.traffic_amount === "" ? null : toNumber(merged.traffic_amount),
    total_amount: merged.total_amount == null || merged.total_amount === "" ? null : toNumber(merged.total_amount),
    attendee_count: merged.attendee_count == null || merged.attendee_count === "" ? null : toNumber(merged.attendee_count),
    attendee_people: String(merged.attendee_people || "").trim() || null,
    child_names: String(merged.child_names || "").trim() || null,
    child_info: String(merged.child_info || "").trim() || null,
    shoot_location: String(merged.shoot_location || "").trim() || null,
    shoot_address: String(merged.shoot_address || "").trim() || null,
    request_notes: String(merged.request_notes || "").trim() || null,
    memo: String(merged.memo || "").trim() || null,
    missing_fields_json: JSON.stringify(missingFields),
    confidence: merged.confidence == null || merged.confidence === "" ? null : Number(merged.confidence),
    approval_status: "approved",
    approved_customer_id: customerResult.customer_id || null,
    approved_reservation_id: reservationResult.reservation_id || null,
    raw_json: JSON.stringify({
      approved_at: nowIso(),
      approved_values: merged
    }),
    updated_at: nowIso()
  }, "id", draftId);

  if (draft.intake_id_ref) {
    await env.DB.prepare(`
      UPDATE ai_reservation_intakes
      SET
        status = 'approved',
        updated_at = datetime('now')
      WHERE id = ?
    `).bind(draft.intake_id_ref).run();
  }

  const rebuild = await rebuildAnalytics(env);

  return {
    ok: true,
    draft_id: draftId,
    approved_customer_id: customerResult.customer_id || null,
    approved_reservation_id: reservationResult.reservation_id || null,
    rebuild: rebuild
  };
}

async function rejectAiReservationDraft(env, body) {
  await ensureAdminTables(env);

  const draftId = toNumber(body.draft_id);
  const reason = String(body.reason || "").trim();
  if (!draftId) return { ok: false, error: "draft_id is required" };

  const draft = await env.DB.prepare(`
    SELECT *
    FROM ai_reservation_drafts
    WHERE id = ?
    LIMIT 1
  `).bind(draftId).first();

  if (!draft) return { ok: false, error: "draft not found" };

  await env.DB.prepare(`
    UPDATE ai_reservation_drafts
    SET
      approval_status = 'rejected',
      memo = ?,
      updated_at = datetime('now')
    WHERE id = ?
  `).bind(
    appendLines(draft.memo, reason ? "却下理由: " + reason : ""),
    draftId
  ).run();

  if (draft.intake_id) {
    await env.DB.prepare(`
      UPDATE ai_reservation_intakes
      SET
        status = 'rejected',
        error_text = ?,
        updated_at = datetime('now')
      WHERE id = ?
    `).bind(reason || null, draft.intake_id).run();
  }

  return {
    ok: true,
    draft_id: draftId,
    status: "rejected"
  };
}
