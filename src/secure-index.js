// ======================================================
// CUSTOMER CRM API / SECURE ENTRYPOINT WRAPPER
// build: customer-crm-api-secure-entry-20260530-01
// ======================================================
// Security goals:
// - Do not use /admin?token=... URLs
// - Do not return admin URLs, tokens, or internal URLs from health endpoints
// - Require login + 5-minute one-time code before showing /admin
// - Use HttpOnly Secure SameSite=Strict cookies for sessions
// - Add noindex/robots/security headers
// - Keep the existing app in ./index.js intact and proxy authenticated admin/API calls internally
// ======================================================

import app from "./index.js";

const SECURE_BUILD = "customer-crm-api-secure-entry-20260530-01";
const SESSION_COOKIE = "crm_admin_session";
const CHALLENGE_COOKIE = "crm_otp_challenge";
const SESSION_TTL_SECONDS = 30 * 60;
const OTP_TTL_SECONDS = 5 * 60;
const MAX_OTP_ATTEMPTS = 5;

const enc = new TextEncoder();
const dec = new TextDecoder();

function t(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}

function json(data, status = 200, extraHeaders = {}) {
  return secureResponse(JSON.stringify(data, null, 2), status, {
    "content-type": "application/json; charset=utf-8",
    ...extraHeaders
  });
}

function html(body, status = 200, extraHeaders = {}) {
  return secureResponse(body, status, {
    "content-type": "text/html; charset=utf-8",
    ...extraHeaders
  });
}

function secureHeaders(base = {}) {
  const h = new Headers(base);
  h.set("cache-control", "no-store, no-cache, must-revalidate, max-age=0");
  h.set("pragma", "no-cache");
  h.set("expires", "0");
  h.set("x-robots-tag", "noindex, nofollow, noarchive");
  h.set("x-content-type-options", "nosniff");
  h.set("referrer-policy", "no-referrer");
  h.set("x-frame-options", "DENY");
  h.set("permissions-policy", "camera=(), microphone=(), geolocation=(), payment=(), usb=(), interest-cohort=()");
  h.set("content-security-policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data: blob: https:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'");
  return h;
}

function secureResponse(body, status = 200, headers = {}) {
  return new Response(body, { status, headers: secureHeaders(headers) });
}

function parseCookies(request) {
  const out = {};
  const raw = request.headers.get("cookie") || "";
  raw.split(";").forEach((part) => {
    const i = part.indexOf("=");
    if (i <= 0) return;
    const k = part.slice(0, i).trim();
    const v = part.slice(i + 1).trim();
    out[k] = v;
  });
  return out;
}

function cookie(name, value, maxAge) {
  return `${name}=${value}; Path=/; Max-Age=${maxAge}; HttpOnly; Secure; SameSite=Strict`;
}

function clearCookie(name) {
  return `${name}=; Path=/; Max-Age=0; HttpOnly; Secure; SameSite=Strict`;
}

function bytesToBase64Url(bytes) {
  let bin = "";
  for (const b of bytes) bin += String.fromCharCode(b);
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
}

function base64UrlToBytes(s) {
  const b64 = String(s || "").replace(/-/g, "+").replace(/_/g, "/") + "===".slice((String(s || "").length + 3) % 4);
  const bin = atob(b64);
  const bytes = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
  return bytes;
}

function encodePayload(obj) {
  return bytesToBase64Url(enc.encode(JSON.stringify(obj)));
}

function decodePayload(payload) {
  return JSON.parse(dec.decode(base64UrlToBytes(payload)));
}

async function hmac(secret, value) {
  const key = await crypto.subtle.importKey("raw", enc.encode(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  const sig = await crypto.subtle.sign("HMAC", key, enc.encode(value));
  return bytesToBase64Url(new Uint8Array(sig));
}

async function sha256Hex(value) {
  const buf = await crypto.subtle.digest("SHA-256", enc.encode(value));
  return Array.from(new Uint8Array(buf)).map((b) => b.toString(16).padStart(2, "0")).join("");
}

function constantTimeEqual(a, b) {
  a = String(a || "");
  b = String(b || "");
  let diff = a.length ^ b.length;
  const len = Math.max(a.length, b.length);
  for (let i = 0; i < len; i++) diff |= (a.charCodeAt(i) || 0) ^ (b.charCodeAt(i) || 0);
  return diff === 0;
}

async function signPayload(secret, payloadObj) {
  const payload = encodePayload(payloadObj);
  const sig = await hmac(secret, payload);
  return `${payload}.${sig}`;
}

async function verifySignedValue(secret, value, expectedType) {
  const raw = String(value || "");
  const [payload, sig] = raw.split(".");
  if (!payload || !sig) return null;
  const expected = await hmac(secret, payload);
  if (!constantTimeEqual(sig, expected)) return null;
  const data = decodePayload(payload);
  const now = Math.floor(Date.now() / 1000);
  if (!data || data.type !== expectedType || !data.exp || Number(data.exp) < now) return null;
  return data;
}

function requiredConfig(env) {
  const missing = [];
  if (!t(env.ADMIN_TOKEN)) missing.push("ADMIN_TOKEN");
  if (!t(env.CRM_LOGIN_ID)) missing.push("CRM_LOGIN_ID");
  if (!t(env.CRM_LOGIN_PASSWORD) && !t(env.CRM_LOGIN_PASSWORD_SHA256)) missing.push("CRM_LOGIN_PASSWORD or CRM_LOGIN_PASSWORD_SHA256");
  if (!t(env.CRM_SESSION_SECRET)) missing.push("CRM_SESSION_SECRET");
  if (!t(env.RESEND_API_KEY)) missing.push("RESEND_API_KEY");
  if (!t(env.CRM_2FA_EMAIL_FROM)) missing.push("CRM_2FA_EMAIL_FROM");
  if (!t(env.CRM_2FA_EMAIL_TO)) missing.push("CRM_2FA_EMAIL_TO");
  return missing;
}

async function readJson(request) {
  try {
    return await request.json();
  } catch (_) {
    return {};
  }
}

async function verifyPassword(env, loginId, password) {
  if (!constantTimeEqual(loginId, t(env.CRM_LOGIN_ID))) return false;
  if (t(env.CRM_LOGIN_PASSWORD_SHA256)) {
    return constantTimeEqual(await sha256Hex(password), t(env.CRM_LOGIN_PASSWORD_SHA256).toLowerCase());
  }
  return constantTimeEqual(password, t(env.CRM_LOGIN_PASSWORD));
}

function randomOtp() {
  const arr = new Uint32Array(1);
  crypto.getRandomValues(arr);
  return String(arr[0] % 1000000).padStart(6, "0");
}

async function sendOtpEmail(env, code) {
  const res = await fetch("https://api.resend.com/emails", {
    method: "POST",
    headers: {
      "authorization": `Bearer ${env.RESEND_API_KEY}`,
      "content-type": "application/json"
    },
    body: JSON.stringify({
      from: env.CRM_2FA_EMAIL_FROM,
      to: [env.CRM_2FA_EMAIL_TO],
      subject: "CRM 管理画面 認証コード",
      text: `CRM管理画面の認証コードです。\n\n${code}\n\nこのコードは5分間だけ有効です。心当たりがない場合は破棄してください。`
    })
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`2FA email failed: ${res.status} ${body.slice(0, 200)}`);
  }
}

async function getSession(request, env) {
  const secret = t(env.CRM_SESSION_SECRET);
  if (!secret) return null;
  const cookies = parseCookies(request);
  return await verifySignedValue(secret, cookies[SESSION_COOKIE], "session");
}

async function handleStart(request, env) {
  const missing = requiredConfig(env);
  if (missing.length) {
    return json({ ok: false, message: "Security setup required", missing }, 503);
  }
  const body = await readJson(request);
  const loginId = t(body.login_id);
  const password = String(body.password || "");
  const ok = await verifyPassword(env, loginId, password);
  if (!ok) return json({ ok: false, message: "ログイン情報が正しくありません。" }, 401);

  const code = randomOtp();
  const now = Math.floor(Date.now() / 1000);
  const exp = now + OTP_TTL_SECONDS;
  const codeHash = await hmac(env.CRM_SESSION_SECRET, `${loginId}:${code}:${exp}:otp`);
  const challenge = await signPayload(env.CRM_SESSION_SECRET, { type: "otp", sub: loginId, exp, codeHash, attempts: 0 });

  await sendOtpEmail(env, code);

  return json({ ok: true, message: "認証コードを送信しました。5分以内に入力してください。" }, 200, {
    "set-cookie": cookie(CHALLENGE_COOKIE, challenge, OTP_TTL_SECONDS)
  });
}

async function handleVerify(request, env) {
  const missing = requiredConfig(env);
  if (missing.length) return json({ ok: false, message: "Security setup required", missing }, 503);
  const cookies = parseCookies(request);
  const challenge = await verifySignedValue(env.CRM_SESSION_SECRET, cookies[CHALLENGE_COOKIE], "otp");
  if (!challenge) {
    return json({ ok: false, message: "認証コードの有効期限が切れています。もう一度ログインしてください。" }, 401, {
      "set-cookie": clearCookie(CHALLENGE_COOKIE)
    });
  }
  if (Number(challenge.attempts || 0) >= MAX_OTP_ATTEMPTS) {
    return json({ ok: false, message: "認証コードの入力回数が上限に達しました。もう一度ログインしてください。" }, 429, {
      "set-cookie": clearCookie(CHALLENGE_COOKIE)
    });
  }

  const body = await readJson(request);
  const code = String(body.code || "").replace(/\D/g, "").slice(0, 6);
  const expected = await hmac(env.CRM_SESSION_SECRET, `${challenge.sub}:${code}:${challenge.exp}:otp`);
  if (!constantTimeEqual(expected, challenge.codeHash)) {
    const next = await signPayload(env.CRM_SESSION_SECRET, { ...challenge, attempts: Number(challenge.attempts || 0) + 1 });
    return json({ ok: false, message: "認証コードが正しくありません。" }, 401, {
      "set-cookie": cookie(CHALLENGE_COOKIE, next, Math.max(1, Number(challenge.exp) - Math.floor(Date.now() / 1000)))
    });
  }

  const now = Math.floor(Date.now() / 1000);
  const session = await signPayload(env.CRM_SESSION_SECRET, { type: "session", sub: challenge.sub, iat: now, exp: now + SESSION_TTL_SECONDS, nonce: crypto.randomUUID() });
  const headers = new Headers();
  headers.append("set-cookie", clearCookie(CHALLENGE_COOKIE));
  headers.append("set-cookie", cookie(SESSION_COOKIE, session, SESSION_TTL_SECONDS));
  return json({ ok: true, message: "ログインしました。" }, 200, headers);
}

function escHtml(value) {
  return String(value || "").replace(/[&<>\"]/g, (m) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "\"": "&quot;" }[m]));
}

function loginPage(env) {
  const setupMissing = requiredConfig(env);
  const setupHtml = setupMissing.length ? `<div class="setup"><b>初期設定が必要です</b><br>Cloudflareの環境変数/Secretsに以下を設定してください。<br><code>${setupMissing.map(escHtml).join("</code><br><code>")}</code></div>` : "";
  return `<!doctype html><html lang="ja"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="robots" content="noindex,nofollow,noarchive"><title>CRM 管理ログイン</title><style>
  body{margin:0;min-height:100vh;display:grid;place-items:center;background:#f6f7fb;color:#111827;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans JP',sans-serif}.box{width:min(92vw,430px);background:#fff;border:1px solid #e5e7eb;border-radius:22px;padding:24px;box-shadow:0 18px 60px rgba(15,23,42,.12)}h1{font-size:24px;margin:0 0 8px}.muted{color:#64748b;font-size:13px;line-height:1.7}label{display:block;font-weight:900;font-size:13px;margin:16px 0 6px}input{width:100%;box-sizing:border-box;border:1px solid #d1d5db;border-radius:14px;padding:13px;font-size:16px;background:#f8fafc}button{width:100%;border:0;border-radius:14px;padding:13px;margin-top:16px;background:#111827;color:#fff;font-weight:900;font-size:15px}.msg{margin-top:14px;font-size:13px;line-height:1.6}.setup{background:#fff7ed;border:1px solid #fed7aa;color:#9a3412;border-radius:14px;padding:12px;margin:14px 0;font-size:13px;line-height:1.6}code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
  </style></head><body><main class="box"><h1>CRM 管理ログイン</h1><div class="muted">URLトークン方式は無効です。ログイン後、5分間だけ有効な認証コードを入力してください。</div>${setupHtml}<div id="step1"><label>ログインID</label><input id="loginId" autocomplete="username"><label>パスワード</label><input id="password" type="password" autocomplete="current-password"><button id="sendBtn">認証コードを送信</button></div><div id="step2" style="display:none"><label>6桁の認証コード</label><input id="code" inputmode="numeric" maxlength="6" autocomplete="one-time-code"><button id="verifyBtn">ログイン</button></div><div class="msg" id="msg"></div><script>
  const $=id=>document.getElementById(id);function msg(v){$('msg').textContent=v||''}async function post(url,data){const r=await fetch(url,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(data)});const j=await r.json().catch(()=>({ok:false,message:'JSON error'}));if(!r.ok||j.ok===false)throw new Error(j.message||('HTTP '+r.status));return j}
  $('sendBtn').onclick=async()=>{try{$('sendBtn').disabled=true;msg('送信中...');const j=await post('/auth/start',{login_id:$('loginId').value,password:$('password').value});msg(j.message||'送信しました');$('step1').style.display='none';$('step2').style.display='block';$('code').focus()}catch(e){msg(e.message)}finally{$('sendBtn').disabled=false}};
  $('verifyBtn').onclick=async()=>{try{$('verifyBtn').disabled=true;msg('確認中...');await post('/auth/verify',{code:$('code').value});location.href='/admin'}catch(e){msg(e.message)}finally{$('verifyBtn').disabled=false}};
  </script></main></body></html>`;
}

async function renderSecureAdmin(request, env, ctx) {
  if (!t(env.ADMIN_TOKEN)) {
    return html(loginPage(env), 503);
  }
  const url = new URL(request.url);
  url.pathname = "/admin";
  url.search = "";
  url.searchParams.set("token", env.ADMIN_TOKEN);
  const headers = new Headers(request.headers);
  headers.set("x-admin-token", env.ADMIN_TOKEN);
  headers.set("authorization", `Bearer ${env.ADMIN_TOKEN}`);
  const internalReq = new Request(url.toString(), { method: "GET", headers });
  const res = await app.fetch(internalReq, env, ctx);
  let body = await res.text();
  body = body.replaceAll(env.ADMIN_TOKEN, "");
  body = body.replace(/<head>/i, "<head><meta name=\"robots\" content=\"noindex,nofollow,noarchive\">");
  body = body.replace(/\/admin\?token=[^'\"\s<)]+/g, "/admin");
  return html(body, res.status);
}

async function proxyOriginalWithAdmin(request, env, ctx) {
  const adminToken = t(env.ADMIN_TOKEN);
  if (!adminToken) return json({ ok: false, message: "Security setup required" }, 503);
  const url = new URL(request.url);
  url.searchParams.delete("token");
  const headers = new Headers(request.headers);
  headers.set("x-admin-token", adminToken);
  headers.set("authorization", `Bearer ${adminToken}`);
  const init = { method: request.method, headers, redirect: "manual" };
  if (request.method !== "GET" && request.method !== "HEAD") init.body = request.body;
  const res = await app.fetch(new Request(url.toString(), init), env, ctx);
  return new Response(res.body, { status: res.status, statusText: res.statusText, headers: secureHeaders(res.headers) });
}

function hasUrlToken(url) {
  return url.searchParams.has("token") || /token=/i.test(url.search || "");
}

function isPublicPassThrough(path) {
  return path.startsWith("/api/sync/");
}

export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      if (request.method === "OPTIONS") return secureResponse(null, 204);

      if (path === "/robots.txt") {
        return secureResponse("User-agent: *\nDisallow: /\n", 200, { "content-type": "text/plain; charset=utf-8" });
      }

      if (path === "/" || path === "/health" || path === "/api/health") {
        return json({ ok: true, service: "customer-crm-api", build: SECURE_BUILD, hasDb: !!env.DB, secure: true });
      }

      if (hasUrlToken(url)) {
        if (path === "/admin") {
          url.search = "";
          return Response.redirect(url.toString(), 302);
        }
        return json({ ok: false, message: "URL token authentication is disabled. Please login from /admin." }, 401);
      }

      if (path === "/auth/start" && request.method === "POST") return await handleStart(request, env);
      if (path === "/auth/verify" && request.method === "POST") return await handleVerify(request, env);
      if (path === "/auth/logout") {
        const headers = new Headers();
        headers.append("set-cookie", clearCookie(SESSION_COOKIE));
        headers.append("set-cookie", clearCookie(CHALLENGE_COOKIE));
        return html("<!doctype html><meta name=\"robots\" content=\"noindex\"><p>ログアウトしました。<a href=\"/admin\">ログイン画面へ</a></p>", 200, headers);
      }

      if (isPublicPassThrough(path)) {
        const res = await app.fetch(request, env, ctx);
        return new Response(res.body, { status: res.status, statusText: res.statusText, headers: secureHeaders(res.headers) });
      }

      const session = await getSession(request, env);

      if (path === "/admin") {
        if (!session) return html(loginPage(env), 401);
        return await renderSecureAdmin(request, env, ctx);
      }

      if (path.startsWith("/api/")) {
        if (!session) return json({ ok: false, message: "Login required" }, 401);
        return await proxyOriginalWithAdmin(request, env, ctx);
      }

      return json({ ok: false, message: "Not Found" }, 404);
    } catch (e) {
      return json({ ok: false, message: "Internal error", build: SECURE_BUILD }, 500);
    }
  }
};
