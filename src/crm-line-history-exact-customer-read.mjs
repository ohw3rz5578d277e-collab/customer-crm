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
