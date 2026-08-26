CREATE TABLE IF NOT EXISTS reservation_memos (
  memo_id TEXT PRIMARY KEY,
  reservation_id TEXT NOT NULL,
  message_text TEXT NOT NULL DEFAULT '',
  sender TEXT NOT NULL DEFAULT 'internal',
  source TEXT NOT NULL DEFAULT 'reservation_detail',
  client_request_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(reservation_id, client_request_id)
);

CREATE INDEX IF NOT EXISTS idx_reservation_memos_reservation_created
  ON reservation_memos(reservation_id, created_at, memo_id);

CREATE TABLE IF NOT EXISTS reservation_memo_attachments (
  attachment_id TEXT PRIMARY KEY,
  memo_id TEXT NOT NULL UNIQUE,
  reservation_id TEXT NOT NULL,
  file_name TEXT NOT NULL DEFAULT '',
  mime_type TEXT NOT NULL,
  byte_size INTEGER NOT NULL,
  image_blob BLOB NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_reservation_memo_attachments_reservation
  ON reservation_memo_attachments(reservation_id, created_at, attachment_id);
