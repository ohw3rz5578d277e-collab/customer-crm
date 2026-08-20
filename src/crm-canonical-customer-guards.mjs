// TASK-CRM-LINE-FOLLOW-CANONICAL-CUSTOMER-01
// Canonical LINE identity guards for Customer CRM.
// - LINE identity is only a formal LINE user ID: ^U[0-9a-fA-F]{20,}$
// - Customer ID is never accepted as line_user_id
// - Display name is metadata, not identity matching
// - Existing real customer names are not overwritten by placeholders

import { resolveOrCreateCustomerIdentity } from "./customer-identity-resolver.mjs";

const BUILD = "crm-canonical-customer-guards-20260820-01";
const LINE_ID_RE = /^U[0-9a-fA-F]{20,}$/;
const PLACEHOLDER_NAMES = new Set(["", "名称未設定", "未設定", "名前未取得", "ID確認中", "LINEプロフィール未取得"]);

function text(v){ return v == null ? "" : String(v).trim(); }
function nowIso(){ return new Date().toISOString(); }
function json(data,status=200){ return new Response(JSON.stringify(data,null,2),{status,headers:{"content-type":"application/json; charset=utf-8","cache-control":"no-store","x-crm-canonical-customer-guards-build":BUILD}}); }
function bearer(req){ const h=text(req.headers.get("authorization")); return /^Bearer\s+/i.test(h) ? h.replace(/^Bearer\s+/i,"").trim() : ""; }
function internalToken(req){ return text(req.headers.get("x-internal-token")) || bearer(req); }
export function isFormalLineUserId(v){ return LINE_ID_RE.test(text(v)); }
function isPlaceholderName(v){ return PLACEHOLDER_NAMES.has(text(v)); }
function displayNameFromPayload(v){ return text(v && (v.line_display_name || v.display_name || v.profile_display_name || v.customer_name || v.name)); }
function sanitizeNameForUpsert(incomingName, displayName){
  const name = text(incomingName);
  if(!isPlaceholderName(name)) return name;
  const display = text(displayName);
  return display && !isPlaceholderName(display) ? display : null;
}
function sanitizeCustomerItem(item){
  const next = {...item};
  const incomingLineId = text(next.line_user_id || next.lineUserId || next.line_id || next.line_uid || next.line_mid);
  const displayName = displayNameFromPayload(next);
  const sanitizedName = sanitizeNameForUpsert(next.name || next.customer_name, displayName);
  const ignored = [];
  if(incomingLineId){
    if(isFormalLineUserId(incomingLineId)) next.line_user_id = incomingLineId;
    else { delete next.line_user_id; delete next.lineUserId; delete next.line_id; delete next.line_uid; delete next.line_mid; ignored.push("invalid_line_user_id"); }
  }
  if(displayName) next.line_display_name = displayName;
  if(sanitizedName) next.name = sanitizedName;
  else if(isPlaceholderName(next.name || next.customer_name)){ delete next.name; delete next.customer_name; ignored.push("placeholder_name_not_promoted"); }
  if(ignored.length) next.identity_guard = { ...(next.identity_guard||{}), ignored_invalid_line_user_id: ignored.includes("invalid_line_user_id"), ignored_placeholder_name: ignored.includes("placeholder_name_not_promoted") };
  return next;
}
export async function handleCanonicalLineFollow(request, env){
  const u = new URL(request.url);
  if(u.pathname !== "/api/internal/line/follow-canonical-customer") return null;
  if(request.method !== "POST") return json({ok:false,error:"method_not_allowed"},405);
  const expected = text(env && env.CRM_INTERNAL_TOKEN);
  if(!expected) return json({ok:false,error:"identity_auth_not_configured"},503);
  if(internalToken(request) !== expected) return json({ok:false,error:"unauthorized"},401);
  let body = {}; try{ body = await request.json(); }catch(_){ return json({ok:false,error:"invalid_json"},400); }
  const lineUserId = text(body.line_user_id || body.user_id);
  if(!isFormalLineUserId(lineUserId)) return json({ok:false,error:"invalid_line_user_id",review_required:false},400);
  const eventId = text(body.webhook_event_id || body.event_id || body.follow_event_id || body.idempotency_key) || `follow:${lineUserId}`;
  const displayName = displayNameFromPayload(body);
  const customerName = isPlaceholderName(text(body.customer_name || body.name)) ? displayName : text(body.customer_name || body.name);
  const resolved = await resolveOrCreateCustomerIdentity(env, {
    line_user_id: lineUserId,
    idempotency_key: `line_follow:${eventId}`,
    source: text(body.source) || "line_follow",
    customer_name: customerName || displayName || ""
  });
  const status = resolved.statusCode || 200;
  delete resolved.statusCode;
  if(!resolved.ok) return json({...resolved, build:BUILD}, status);
  if(displayName && env && env.DB){
    const existing = await env.DB.prepare("SELECT customer_id,name,line_display_name FROM customers WHERE customer_id=? LIMIT 1").bind(resolved.customer_id).first();
    if(existing){
      const shouldPromoteName = isPlaceholderName(existing.name);
      await env.DB.prepare(`UPDATE customers SET line_display_name=?, name=CASE WHEN ? THEN ? ELSE name END, updated_at=? WHERE customer_id=? AND line_user_id=?`).bind(displayName, shouldPromoteName ? 1 : 0, displayName, nowIso(), resolved.customer_id, lineUserId).run();
    }
  }
  return json({ok:true, ...resolved, line_user_id: lineUserId, line_display_name_saved: !!displayName, identity_matching:"line_user_id_only", name_matching:false, build:BUILD});
}
export async function handleGuardedCustomerUpsert(request, env, app, ctx){
  const u = new URL(request.url);
  if(request.method !== "POST" || u.pathname !== "/api/sync/customers/upsert") return null;
  let body = {}; try{ body = await request.clone().json(); }catch(_){ return app.fetch(request, env, ctx); }
  const isArray = Array.isArray(body);
  const sourceItems = isArray ? body : Array.isArray(body.items) ? body.items : [body];
  const sanitizedItems = sourceItems.filter(Boolean).map(sanitizeCustomerItem);
  const payload = isArray ? sanitizedItems : (Array.isArray(body.items) ? {...body, items:sanitizedItems} : sanitizedItems[0]);
  const headers = new Headers(request.headers);
  headers.set("content-type","application/json");
  headers.set("x-crm-canonical-line-guard","1");
  const guarded = new Request(request.url,{method:"POST",headers,body:JSON.stringify(payload)});
  const res = await app.fetch(guarded, env, ctx);
  const ct = res.headers.get("content-type") || "";
  if(!ct.includes("application/json")) return res;
  const raw = await res.text();
  let data = {}; try{ data = raw ? JSON.parse(raw) : {}; }catch(_){ data = {raw}; }
  return json({...data, canonical_line_guard:true, invalid_line_user_id_not_persisted:true, placeholder_name_not_forced:true, build_guard:BUILD}, res.status);
}
export function canonicalCustomerGuardHealth(){
  return {
    canonical_customer_guard_enabled:true,
    canonical_line_id_regex:"^U[0-9a-fA-F]{20,}$",
    canonical_customer_id_owner:"customer-crm",
    canonical_line_id_name_matching:false,
    canonical_invalid_line_id_overwrite:false,
    canonical_placeholder_name_overwrite:false,
    canonical_line_follow_receiver:"/api/internal/line/follow-canonical-customer",
    canonical_customer_guard_build:BUILD
  };
}
