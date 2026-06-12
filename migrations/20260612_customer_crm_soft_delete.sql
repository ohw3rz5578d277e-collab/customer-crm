-- ======================================================
-- customer-crm soft delete columns
-- Created: 2026-06-12
-- ======================================================

ALTER TABLE customers ADD COLUMN deleted_at TEXT;
ALTER TABLE customers ADD COLUMN deleted_by TEXT;
ALTER TABLE customers ADD COLUMN delete_reason TEXT;

ALTER TABLE customer_reservations ADD COLUMN deleted_at TEXT;
ALTER TABLE customer_reservations ADD COLUMN deleted_by TEXT;
ALTER TABLE customer_reservations ADD COLUMN delete_reason TEXT;

ALTER TABLE customer_items ADD COLUMN deleted_at TEXT;
ALTER TABLE customer_items ADD COLUMN deleted_by TEXT;
ALTER TABLE customer_items ADD COLUMN delete_reason TEXT;

ALTER TABLE customer_timeline ADD COLUMN deleted_at TEXT;
ALTER TABLE customer_timeline ADD COLUMN deleted_by TEXT;
ALTER TABLE customer_timeline ADD COLUMN delete_reason TEXT;

ALTER TABLE customer_line_messages ADD COLUMN deleted_at TEXT;
ALTER TABLE customer_line_messages ADD COLUMN deleted_by TEXT;
ALTER TABLE customer_line_messages ADD COLUMN delete_reason TEXT;

ALTER TABLE customer_tags ADD COLUMN deleted_at TEXT;
ALTER TABLE customer_tags ADD COLUMN deleted_by TEXT;
ALTER TABLE customer_tags ADD COLUMN delete_reason TEXT;
