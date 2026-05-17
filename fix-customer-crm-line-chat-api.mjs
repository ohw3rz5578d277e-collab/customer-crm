a// ======================================================
// fix-customer-crm-line-chat-api.mjs
// customer-crm / src/index.js のLINE履歴取得・LINE風チャット表示修正
// build: fix-customer-crm-line-chat-api-20260517-01
// ======================================================
//
// 目的:
// 1) CRM側が line-webhook-worker の /api/internal/customer-line-history を取得できない問題を修正
// 2) parseJson 未定義で normalizeLineHistoryMessages が落ちる問題を修正
// 3) LINE履歴を「お客様=左」「運営側=右」の吹き出し表示に整える
// 4) 既存の顧客一覧・セグメント・購入履歴・撮影履歴は壊さない
//
// 使い方:
// customer-crm の Codespaces で
// node fix-customer-crm-line-chat-api.mjs
// ======================================================

import fs from "node:fs";
import { execFileSync } from "node:child_process";

const TARGET = "src/index.js";
const BUILD = "customer-crm-api-complete-20260517-line-chat-api-fix-01";

function fail(message) {
  console.error("ERROR:", message);
  process.exit(1);
}

function backupFile(file) {
  const stamp = new Date().toISOString().replace(/[-:T.Z]/g, "").slice(0, 14);
  const backup = `${file}.backup-line-chat-api-${stamp}`;
  fs.copyFileSync(file, backup);
  console.log("backup created:", backup);
  return backup;
}

function replaceBetween(src, startMarker, endMarker, replacement, label) {
  const start = src.indexOf(startMarker);
  if (start < 0) fail(`${label}: start marker not found: ${startMarker}`);
  const end = src.indexOf(endMarker, start);
  if (end < 0) fail(`${label}: end marker not found: ${endMarker}`);
  console.log(`${label} replaced`);
  return src.slice(0, start) + replacement + "\n\n" + src.slice(end);
}

function insertAfter(src, marker, insert, label) {
  if (src.includes(insert.trim().slice(0, 80))) {
    console.log(`${label} already exists`);
    return src;
  }
  const pos = src.indexOf(marker);
  if (pos < 0) fail(`${label}: marker not found: ${marker}`);
  console.log(`${label} inserted`);
  return src.slice(0, pos + marker.length) + insert + src.slice(pos + marker.length);
}

if (!fs.existsSync(TARGET)) {
  fail(`${TARGET} が見つかりません。customer-crm のルートで実行してください。`);
}

let src = fs.readFileSync(TARGET, "utf8");

if (!src.includes("CUSTOMER CRM API") || !src.includes("customer_line_messages")) {
  fail("この src/index.js は customer-crm の本体ではない可能性があります。");
}

const backup = backupFile(TARGET);

// build更新
src = src.replace(/const BUILD = "([^"]+)";/, `const BUILD = "${BUILD}";`);
src = src.replace(/\/\/ build: customer-crm-api-complete-[^\n]+/, `// build: ${BUILD}`);

// parseJson がなければ追加
if (!src.includes("function parseJson(")) {
  const marker = `async function readJson(request) {
  try {
    return await request.json();
  } catch (_) {
    return {};
  }
}
`;
  const insert = `
function parseJson(value, fallback = {}) {
  try {
    if (value === undefined || value === null || value === "") return fallback;
    if (typeof value === "object") return value;
    return JSON.parse(String(value));
  } catch (_) {
    return fallback;
  }
}
`;
  src = insertAfter(src, marker, insert, "parseJson helper");
} else {
  console.log("parseJson helper already exists");
}

// normalizeLineHistoryMessages を差し替え
const normalizeFn = `function normalizeLineHistoryMessages(items) {
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
}`;

src = replaceBetween(
  src,
  "function normalizeLineHistoryMessages(items) {",
  "async function getLocalCustomerLineHistory(env, customer) {",
  normalizeFn,
  "normalizeLineHistoryMessages"
);

// fetchRemoteLineHistory を差し替え
const fetchRemoteFn = `async function fetchRemoteLineHistory(env, customer) {
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
  ].filter(Boolean).map((v) => v.replace(/\\/+$/, ""))));

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
}`;

src = replaceBetween(
  src,
  "async function fetchRemoteLineHistory(env, customer) {",
  "async function getCustomerLineHistory(env, customer) {",
  fetchRemoteFn,
  "fetchRemoteLineHistory"
);

// renderChatMessages を差し替え
const renderFn = `function renderChatMessages(lineHistory){
  const lh=lineHistory||{};
  const messages=lh.messages||[];
  if(!messages.length){
    return '<div class="detail-section-title"><h3>LINE履歴</h3><span class="badge">0件</span></div>' +
      '<div class="chat-box"><div class="chat-empty">'+esc(lh.message||'LINE履歴はまだありません。')+'</div></div>';
  }

  let lastDate='';
  function datePart(v){
    const s=String(v||'');
    const m=s.match(/^(\\d{4}-\\d{2}-\\d{2})/);
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
}`;

src = replaceBetween(
  src,
  "function renderChatMessages(lineHistory){",
  "async function openDetail(id){",
  renderFn,
  "renderChatMessages"
);

// CSSを追加・上書き気味に最後の style 内へ挿入
const css = `
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
`;

if (!src.includes("LINE風チャット表示強化")) {
  src = src.replace("</style></head><body>", css + "</style></head><body>");
  console.log("LINE-like chat CSS inserted");
} else {
  console.log("LINE-like chat CSS already exists");
}

fs.writeFileSync(TARGET, src, "utf8");

try {
  execFileSync("node", ["--check", TARGET], { stdio: "pipe" });
  console.log("syntax check OK");
} catch (e) {
  console.error(String(e.stdout || ""));
  console.error(String(e.stderr || ""));
  fail("syntax check failed");
}

console.log("Done: customer-crm src/index.js fixed safely");
console.log("Backup:", backup);
console.log("");
console.log("Next commands:");
console.log("  git status");
console.log("  rm " + backup);
console.log("  git add src/index.js fix-customer-crm-line-chat-api.mjs");
console.log('  git commit -m "fix crm line chat history"');
console.log("  git push");
