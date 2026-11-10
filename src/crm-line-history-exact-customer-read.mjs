// A LINE user ID is not a safe join key for a customer-scoped conversation.
// Historical messages without an exact formal customer_id require separate Owner review.
const FORMAL_CUSTOMER_ID = /^\d{8}$/;

export function exactCustomerLineHistoryQuery(customerId) {
  if (typeof customerId !== 'string' || !FORMAL_CUSTOMER_ID.test(customerId)) return null;
  return {
    sql: `SELECT *
FROM customer_line_messages
WHERE customer_id = ?
ORDER BY COALESCE(sent_at, created_at) ASC, id ASC
LIMIT 300`,
    params: [customerId]
  };
}

export async function readExactCustomerLocalLineMessages(db, customerId) {
  const query = exactCustomerLineHistoryQuery(customerId);
  if (!query) return [];
  const result = await db.prepare(query.sql).bind(...query.params).all();
  return Array.isArray(result?.results) ? result.results : [];
}

// Only a unique, formal CRM-to-LINE identity can request remote chat history.
// The LINE Worker is scoped by LINE user ID, not canonical Customer ID.
const CUSTOMER_ID_RE = /^\d{8}$/;
const LINE_USER_ID_RE = /^U[0-9a-fA-F]{20,}$/;
const OWNER_SQL = 'SELECT customer_id FROM customers WHERE line_user_id = ? LIMIT 2';

export async function verifyRemoteLineIdentityOwnership(db, customer) {
  const customerId = customer?.customer_id;
  const lineUserId = customer?.line_user_id;
  if (typeof customerId !== 'string' || !CUSTOMER_ID_RE.test(customerId) ||
      typeof lineUserId !== 'string' || !LINE_USER_ID_RE.test(lineUserId)) {
    return { allowed: false, reason: 'remote_line_identity_invalid' };
  }
  if (!db || typeof db.prepare !== 'function') {
    return { allowed: false, reason: 'remote_line_identity_db_unavailable' };
  }
  try {
    const result = await db.prepare(OWNER_SQL).bind(lineUserId).all();
    const rows = result?.results;
    if (!Array.isArray(rows)) return { allowed: false, reason: 'remote_line_identity_lookup_invalid' };
    if (rows.length !== 1 || rows[0]?.customer_id !== customerId) {
      return { allowed: false, reason: 'remote_line_identity_ambiguous' };
    }
    return { allowed: true, lineUserId };
  } catch {
    // A query failure must never grant permission to look up remote history.
    return { allowed: false, reason: 'remote_line_identity_lookup_failed' };
  }
}

function sameIdentityFields(record, expected) {
  if (!record || typeof record !== 'object' || Array.isArray(record)) return false;
  let present = false;
  for (const name of ['line_user_id', 'user_id']) {
    if (Object.hasOwn(record, name) && record[name] != null && record[name] !== '') {
      present = true;
      if (typeof record[name] !== 'string' || record[name] !== expected) return false;
    }
  }
  return present;
}

export function validateRemoteLineHistoryPayload(payload, expectedLineUserId) {
  if (typeof expectedLineUserId !== 'string' || !LINE_USER_ID_RE.test(expectedLineUserId)) {
    return { valid: false, reason: 'remote_line_expected_identity_invalid' };
  }
  if (!payload || typeof payload !== 'object' || Array.isArray(payload) || payload.ok !== true ||
      typeof payload.line_user_id !== 'string' || payload.line_user_id !== expectedLineUserId) {
    return { valid: false, reason: 'remote_line_envelope_identity_mismatch' };
  }
  if (payload.contact != null && !sameIdentityFields(payload.contact, expectedLineUserId)) {
    return { valid: false, reason: 'remote_line_contact_identity_mismatch' };
  }
  const candidates = [];
  for (const field of ['messages', 'items']) {
    if (!Object.hasOwn(payload, field)) continue;
    if (!Array.isArray(payload[field])) return { valid: false, reason: 'remote_line_messages_invalid' };
    candidates.push({ field, rows: payload[field] });
  }
  if (!candidates.length) return { valid: false, reason: 'remote_line_messages_missing' };
  for (const {rows} of candidates) {
    if (!rows.every(row => sameIdentityFields(row, expectedLineUserId))) {
      return { valid: false, reason: 'remote_line_message_identity_mismatch' };
    }
  }
  const items = candidates.find(candidate => candidate.field === 'messages')?.rows ?? candidates[0].rows;
  return { valid: true, items };
}
