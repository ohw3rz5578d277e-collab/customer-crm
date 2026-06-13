// ======================================================
// CUSTOMER CRM / CUSTOMER SMART PANEL WRAPPER
// build: customer-crm-api-customer-smart-panel-20260614-01
// Adds customer detail smart summary, next actions, and one-tap customer workflows.
// ======================================================

import app from "./production-index-crm-mobile-usability.js";

const BUILD = "customer-crm-api-customer-smart-panel-20260614-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const ROLES = ["root_admin", "admin", "staff", "viewer"];
const WRITE_ROLES = ["root_admin", "admin", "staff"];

function text(v){ return v === undefined || v === null ? "" : String(v).trim(); }
function lower(v){ return text(v).toLowerCase(); }
function nowIso(){ return new Date().toISOString(); }
function securityHeaders(headers = {}){
  const h = new Headers(headers);
  h.set("cache-control", "no-store, no-cache, must-revalidate, max-age=0");
  h.set("pragma", "no-cache");
  h.set("expires", "0");
  h.set("x-content-type-options", "nosniff");
  h.set("referrer-policy", "no-referrer");
  h.set("x-frame-options", "DENY");
  return h;
}
function json(data, status = 200){
  return new Response(JSON.stringify(data, null, 2), { status, headers: securityHeaders({ "content-type":"application/json; charset=utf-8" }) });
}
function getEmail(request){ return lower(request.headers.get("cf-access-authenticated-user-email") || request.headers.get("Cf-Access-Authenticated-User-Email") || request.headers.get("x-user-email") || ""); }
async function readJson(request){ try { return await request.json(); } catch(_) { return {}; } }
async function safeAll(env, sql, binds = []){ try { const s = env.DB.prepare(sql); const r = binds.length ? await s.bind(...binds).all() : await s.all(); return r.results || []; } catch(_) { return []; } }
async function safeFirst(env, sql, binds = []){ try { const s = env.DB.prepare(sql); return binds.length ? await s.bind(...binds).first() : await s.first(); } catch(_) { return null; } }
async function safeRun(env, sql, binds = []){ try { const s = env.DB.prepare(sql); return binds.length ? await s.bind(...binds).run() : await s.run(); } catch(e) { return { error:String(e && e.message || e) }; } }
async function addColumn(db, table, def){ try { await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${def}`).run(); } catch(_) {} }

async function ensureSchema(env){
  if(!env.DB) throw new Error("D1 DB binding(DB) is missing");
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users(email TEXT PRIMARY KEY, role TEXT, status TEXT, created_by TEXT, created_at TEXT, updated_at TEXT)`).run();
  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email, role, status, created_by, created_at, updated_at) VALUES(?, 'admin', 'active', 'system', datetime('now'), datetime('now'))`).bind(ROOT_ADMIN_EMAIL).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_customer_smart_action_logs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT,
    action_type TEXT,
    action_label TEXT,
    related_id TEXT,
    before_status TEXT,
    after_status TEXT,
    payload_json TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();
  await addColumn(env.DB, "customers", "next_action_label TEXT");
  await addColumn(env.DB, "customers", "next_action_due_at TEXT");
  await addColumn(env.DB, "customers", "next_action_source TEXT");
  await addColumn(env.DB, "customers", "smart_panel_updated_at TEXT");
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_customer_smart_action_logs_customer ON crm_customer_smart_action_logs(customer_id, created_at)`).run();
}
async function requireUser(request, env, roles = ROLES){
  await ensureSchema(env);
  const email = getEmail(request);
  if(!email) return { ok:false, response:json({ ok:false, message:"Login required" }, 401) };
  const user = await env.DB.prepare(`SELECT email, role, status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`).bind(email).first();
  if(!user) return { ok:false, response:json({ ok:false, message:"User is not allowed" }, 403) };
  if(roles.length && !roles.includes(user.role || "")) return { ok:false, response:json({ ok:false, message:"Permission denied" }, 403) };
  return { ok:true, email, user };
}

function yen(n){ const v=Number(n||0); return v ? `${v.toLocaleString('ja-JP')}円` : "0円"; }
function daysSince(dateText){ if(!dateText) return null; const d = new Date(dateText); if(Number.isNaN(d.getTime())) return null; return Math.floor((Date.now() - d.getTime()) / 86400000); }
function inferNextOffer(customer, reservations){
  const genreText = `${customer.genre_history||""} ${reservations.map(r=>r.genre||r.shoot_genre||"").join(" ")}`;
  const lastDays = daysSince(customer.last_shoot_date);
  if(/お宮参り|宮参り/.test(genreText)) return "ハーフバースデー・1歳バースデーのご提案";
  if(/マタニティ/.test(genreText)) return "ニューボーン・お宮参りのご提案";
  if(/ハーフバースデー/.test(genreText)) return "1歳バースデーのご提案";
  if(/七五三/.test(genreText)) return "入学・卒業・家族写真のご提案";
  if(/バースデー/.test(genreText)) return "次回バースデー・季節の家族写真のご提案";
  if(lastDays !== null && lastDays >= 180) return "休眠掘り起こし・季節の家族写真";
  if(lastDays !== null && lastDays >= 90) return "リピート撮影・季節の家族写真";
  return customer.next_offer || customer.next_event_suggestion || "次回撮影のご提案";
}
function decideNextAction({ customer, linePending, followOpen, deliveryOpen, inquiries, reservationDrafts, reservations }){
  const lastDays = daysSince(customer.last_shoot_date);
  if(Number(inquiries||0) > 0) return { type:"inquiry", label:"問い合わせ対応", reason:"問い合わせ・返信待ちがあります", priority:"high" };
  if(Number(reservationDrafts||0) > 0) return { type:"reservation", label:"予約下書き確認", reason:"予約下書き・予約連携の確認が必要です", priority:"high" };
  if(Number(linePending||0) > 0) return { type:"line", label:"未送信LINEを送る", reason:"作成済みのLINE文面があります", priority:"high" };
  if(Number(followOpen||0) > 0) return { type:"follow", label:"フォロー予定を完了", reason:"未完了フォローがあります", priority:"medium" };
  if(Number(deliveryOpen||0) > 0) return { type:"delivery", label:"納品進捗を確認", reason:"納品が完了していない案件があります", priority:"medium" };
  if(lastDays !== null && lastDays >= 180) return { type:"marketing", label:"休眠掘り起こしLINE", reason:"最終撮影から180日以上経過しています", priority:"medium" };
  if(lastDays !== null && lastDays >= 90) return { type:"marketing", label:"リピート提案LINE", reason:"最終撮影から90日以上経過しています", priority:"medium" };
  if(Number(reservations||0) === 0) return { type:"inquiry", label:"初回提案LINE", reason:"まだ撮影履歴がありません", priority:"low" };
  return { type:"none", label:"現在大きな未対応なし", reason:"必要な対応は少なめです", priority:"ok" };
}
async function logAction(env, customerId, actionType, label, relatedId, actor, payload = {}, beforeStatus = "", afterStatus = ""){
  await safeRun(env, `INSERT INTO crm_customer_smart_action_logs(customer_id, action_type, action_label, related_id, before_status, after_status, payload_json, created_by, created_at) VALUES(?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)`, [String(customerId), actionType, label, String(relatedId||""), beforeStatus, afterStatus, JSON.stringify(payload), actor]);
}

async function smartPanelApi(request, env, customerId){
  const auth = await requireUser(request, env);
  if(!auth.ok) return auth.response;
  const c = await safeFirst(env, `SELECT * FROM customers WHERE id=? OR customer_id=? LIMIT 1`, [customerId, customerId]);
  if(!c) return json({ ok:false, message:"Customer not found" }, 404);
  const cid = String(c.id || c.customer_id || customerId);
  const reservations = await safeAll(env, `SELECT * FROM customer_reservations WHERE customer_id=? ORDER BY COALESCE(shoot_date, created_at) DESC LIMIT 8`, [cid]);
  const drafts = await safeAll(env, `SELECT * FROM crm_reservation_drafts WHERE customer_id=? AND COALESCE(deleted_at,'')='' ORDER BY created_at DESC LIMIT 8`, [cid]);
  const lines = await safeAll(env, `SELECT * FROM customer_line_draft_logs WHERE customer_id=? ORDER BY created_at DESC LIMIT 8`, [cid]);
  const follows = await safeAll(env, `SELECT * FROM crm_follow_tasks WHERE customer_id=? ORDER BY due_date ASC, created_at DESC LIMIT 8`, [cid]);
  const progress = await safeAll(env, `SELECT * FROM crm_reservation_progress WHERE customer_id=? ORDER BY COALESCE(shoot_date, updated_at) DESC LIMIT 8`, [cid]);
  const inquiries = await safeAll(env, `SELECT * FROM crm_inquiry_pipeline WHERE customer_id=? AND COALESCE(deleted_at,'')='' ORDER BY updated_at DESC, created_at DESC LIMIT 8`, [cid]);
  const actionLogs = await safeAll(env, `SELECT * FROM crm_customer_smart_action_logs WHERE customer_id=? ORDER BY created_at DESC LIMIT 12`, [cid]);
  const linePending = lines.filter(x => !["sent","done","deleted"].includes(String(x.status||"pending"))).length;
  const followOpen = follows.filter(x => !["done","completed","deleted"].includes(String(x.status||"open"))).length;
  const deliveryOpen = progress.filter(x => !["完了","キャンセル"].includes(String(x.progress_status||""))).length;
  const inquiryOpen = inquiries.filter(x => !["予約確定","失注","deleted"].includes(String(x.status||""))).length;
  const draftOpen = drafts.filter(x => !["created","cancelled","deleted"].includes(String(x.status||""))).length;
  const nextOffer = inferNextOffer(c, reservations);
  const nextAction = decideNextAction({ customer:c, linePending, followOpen, deliveryOpen, inquiries:inquiryOpen, reservationDrafts:draftOpen, reservations:reservations.length });
  await safeRun(env, `UPDATE customers SET next_event_suggestion=?, next_line_suggestion=?, next_action_label=?, next_action_source=?, smart_panel_updated_at=CURRENT_TIMESTAMP WHERE id=? OR customer_id=?`, [nextOffer, nextAction.label, nextAction.label, nextAction.type, cid, cid]);
  return json({ ok:true, build:BUILD, customer:c, summary:{ customer_id:cid, rank:c.customer_rank||"", total_revenue:Number(c.total_revenue||0), total_revenue_label:yen(c.total_revenue), repeat_count:Number(c.repeat_count||0), last_shoot_date:c.last_shoot_date||"", last_shoot_days:daysSince(c.last_shoot_date), next_offer:nextOffer, next_action:nextAction, counts:{ reservations:reservations.length, reservation_drafts:draftOpen, line_pending:linePending, follow_open:followOpen, delivery_open:deliveryOpen, inquiry_open:inquiryOpen } }, reservations, drafts, lines, follows, progress, inquiries, action_logs:actionLogs, generated_at:nowIso() });
}

async function createNextActionApi(request, env, customerId){
  const auth = await requireUser(request, env, WRITE_ROLES);
  if(!auth.ok) return auth.response;
  const b = await readJson(request);
  const type = text(b.type || "auto");
  const c = await safeFirst(env, `SELECT * FROM customers WHERE id=? OR customer_id=? LIMIT 1`, [customerId, customerId]);
  if(!c) return json({ ok:false, message:"Customer not found" }, 404);
  const cid = String(c.id || c.customer_id || customerId);
  let chosen = type;
  if(type === "auto"){
    const panelRes = await smartPanelApi(request, env, cid);
    const data = await panelRes.clone().json().catch(()=>null);
    chosen = data?.summary?.next_action?.type || "line";
  }
  if(chosen === "line" || chosen === "marketing"){
    const offer = inferNextOffer(c, []);
    const message = text(b.message) || `${c.name || c.customer_name || "お客様"}様\n\nいつもありがとうございます。\n前回の撮影から少しお日にちが経ちましたので、${offer}のご案内でご連絡いたしました。\nご希望の日程や気になることがあれば、いつでもお気軽にご相談ください。`;
    const r = await safeRun(env, `INSERT INTO customer_line_draft_logs(customer_id, customer_name, message_body, status, source_type, template_name, created_by, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)`, [cid, c.name || c.customer_name || "", message, "pending", "customer_smart_panel", offer, auth.email, nowIso()]);
    const id = r?.meta?.last_row_id || "";
    await logAction(env, cid, "line_draft", "スマートパネルからLINE作成", id, auth.email, { offer, message });
    return json({ ok:true, action:"line_draft", line_log_id:id, message_body:message });
  }
  if(chosen === "follow"){
    const due = text(b.due_date) || new Date(Date.now()+86400000).toISOString().slice(0,10);
    const title = text(b.title) || "顧客フォロー";
    const note = text(b.note) || "スマートパネルから作成";
    const r = await safeRun(env, `INSERT INTO crm_follow_tasks(customer_id, title, note, status, due_date, assigned_to, created_by, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)`, [cid, title, note, "open", due, auth.email, auth.email, nowIso()]);
    const id = r?.meta?.last_row_id || "";
    await logAction(env, cid, "follow_task", title, id, auth.email, { due_date:due, note });
    return json({ ok:true, action:"follow_task", follow_task_id:id, due_date:due });
  }
  if(chosen === "reservation"){
    const genre = text(b.genre || c.last_genre || "");
    const r = await safeRun(env, `INSERT INTO crm_reservation_drafts(customer_id, customer_name, genre, shoot_date, location, plan_name, amount, memo, status, source_type, created_by, created_at, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)`, [cid, c.name || c.customer_name || "", genre, text(b.shoot_date), text(b.location), text(b.plan_name), Number(b.amount||0), text(b.memo||"スマートパネルから作成"), "draft", "customer_smart_panel", auth.email, nowIso()]);
    const id = r?.meta?.last_row_id || "";
    await logAction(env, cid, "reservation_draft", "予約下書き作成", id, auth.email, b);
    return json({ ok:true, action:"reservation_draft", reservation_draft_id:id });
  }
  return json({ ok:false, message:`Unsupported action type: ${chosen}` }, 400);
}

function injectCustomerSmartPanel(html){
  if(!html || html.includes("crmCustomerSmartPanelScript")) return html;
  const style = `<style id="crmCustomerSmartPanelStyle">
#crmCustomerSmartFab{position:fixed;right:16px;bottom:132px;z-index:1000045;background:#028760;color:#fff;border:0;border-radius:999px;padding:11px 14px;font-weight:950;box-shadow:0 18px 46px rgba(2,135,96,.28);cursor:pointer}#crmCustomerSmartPanel{position:fixed;right:14px;bottom:184px;z-index:1000046;width:min(520px,calc(100vw - 28px));max-height:78vh;overflow:auto;background:#fff;border:1px solid #e5e7eb;border-radius:20px;padding:14px;box-shadow:0 28px 80px rgba(15,23,42,.26);display:none;color:#0f172a}#crmCustomerSmartPanel h3{margin:0 0 8px;font-size:17px;font-weight:950}.crmSmartTop{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin:10px 0}.crmSmartStat{background:#f8fafc;border:1px solid #e5e7eb;border-radius:14px;padding:9px}.crmSmartStat b{display:block;font-size:18px}.crmSmartAction{border:1px solid #bbf7d0;background:#f0fdf4;border-radius:16px;padding:12px;margin:10px 0}.crmSmartAction strong{display:block;font-size:16px}.crmSmartButtons{display:flex;flex-wrap:wrap;gap:7px;margin:10px 0}.crmSmartButtons button,#crmSmartLoadBtn{border:0;border-radius:12px;background:#111827;color:#fff;padding:8px 10px;font-size:12px;font-weight:900;cursor:pointer}.crmSmartButtons button.primary{background:#028760}.crmSmartSection{border-top:1px solid #e5e7eb;padding-top:9px;margin-top:10px}.crmSmartList{display:grid;gap:6px}.crmSmartItem{background:#fff;border:1px solid #e5e7eb;border-radius:12px;padding:8px;font-size:12px}.crmSmartInput{width:100%;border:1px solid #d1d5db;border-radius:12px;padding:9px;font-size:16px;margin:6px 0}@media(max-width:860px){#crmCustomerSmartFab{right:14px;bottom:186px}#crmCustomerSmartPanel{left:10px;right:10px;bottom:236px;width:auto}.crmSmartTop{grid-template-columns:1fr 1fr}.crmSmartButtons button{flex:1;min-width:44%}}
</style>`;
  const script = `<script id="crmCustomerSmartPanelScript">
(function(){
 if(window.__crmCustomerSmartPanelInstalled)return; window.__crmCustomerSmartPanelInstalled=true;
 function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
 function api(u,o){return fetch(u,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},o||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
 function toast(m){var d=document.createElement('div');d.textContent=m;d.style.cssText='position:fixed;right:16px;bottom:170px;z-index:1000060;background:#111827;color:#fff;padding:10px 12px;border-radius:12px;font-weight:900';document.body.appendChild(d);setTimeout(function(){d.remove()},2200)}
 function guessCustomerId(){var m=location.pathname.match(/customers?\/(\d+)/i)||location.search.match(/customer_id=(\d+)/i)||location.search.match(/[?&]id=(\d+)/i); if(m)return m[1]; var el=document.querySelector('[data-customer-id]'); if(el)return el.getAttribute('data-customer-id'); return localStorage.getItem('crmLastSmartCustomerId')||'';}
 function openPanel(){init();var p=document.getElementById('crmCustomerSmartPanel');p.style.display=p.style.display==='block'?'none':'block'; if(p.style.display==='block'){var id=guessCustomerId();document.getElementById('crmSmartCustomerId').value=id;if(id)load(id);}}
 function item(title,sub){return '<div class="crmSmartItem"><b>'+esc(title)+'</b><br><span>'+esc(sub||'')+'</span></div>'}
 function render(d){var s=d.summary||{}, c=d.customer||{};localStorage.setItem('crmLastSmartCustomerId',s.customer_id||c.id||'');document.getElementById('crmSmartBody').innerHTML='<h3>'+esc(c.name||c.customer_name||'顧客')+'</h3><small>ID: '+esc(s.customer_id||'')+'</small><div class="crmSmartTop"><div class="crmSmartStat">ランク<b>'+esc(s.rank||'-')+'</b></div><div class="crmSmartStat">累計<b>'+esc(s.total_revenue_label||'0円')+'</b></div><div class="crmSmartStat">撮影<b>'+esc(s.counts&&s.counts.reservations||0)+'件</b></div><div class="crmSmartStat">LINE<b>'+esc(s.counts&&s.counts.line_pending||0)+'</b></div><div class="crmSmartStat">フォロー<b>'+esc(s.counts&&s.counts.follow_open||0)+'</b></div><div class="crmSmartStat">納品<b>'+esc(s.counts&&s.counts.delivery_open||0)+'</b></div></div><div class="crmSmartAction"><strong>次にやること：'+esc(s.next_action&&s.next_action.label||'')+'</strong><small>'+esc(s.next_action&&s.next_action.reason||'')+'</small><br><small>次回提案：'+esc(s.next_offer||'')+'</small></div><div class="crmSmartButtons"><button class="primary" data-smart-act="auto">おすすめ実行</button><button data-smart-act="line">LINE作成</button><button data-smart-act="follow">フォロー作成</button><button data-smart-act="reservation">予約下書き</button><button data-smart-reload="1">再読込</button></div><div class="crmSmartSection"><b>最近の予約</b><div class="crmSmartList">'+(d.reservations||[]).slice(0,4).map(function(r){return item((r.genre||r.shoot_genre||'予約')+' '+(r.shoot_date||''),(r.amount||r.price||'')+' '+(r.status||''));}).join('')+'</div></div><div class="crmSmartSection"><b>未送信LINE・フォロー</b><div class="crmSmartList">'+(d.lines||[]).slice(0,3).map(function(x){return item(x.template_name||x.status||'LINE',x.message_body||x.body||'');}).join('')+(d.follows||[]).slice(0,3).map(function(x){return item(x.title||'フォロー',String(x.due_date||'')+' '+String(x.status||''));}).join('')+'</div></div><div class="crmSmartSection"><b>問い合わせ・操作履歴</b><div class="crmSmartList">'+(d.inquiries||[]).slice(0,3).map(function(x){return item(x.status||'問い合わせ',x.memo||x.message||x.note||'');}).join('')+(d.action_logs||[]).slice(0,5).map(function(x){return item(x.action_label||x.action_type,x.created_at||'');}).join('')+'</div></div>';}
 function load(id){if(!id){toast('顧客IDを入力してください');return;}api('/api/customers/'+encodeURIComponent(id)+'/smart-panel').then(function(d){if(!d.ok){toast(d.message||'読込できません');return;}render(d);});}
 function runAction(type){var id=document.getElementById('crmSmartCustomerId').value.trim();if(!id){toast('顧客IDを入力してください');return;}api('/api/customers/'+encodeURIComponent(id)+'/smart-action',{method:'POST',body:JSON.stringify({type:type})}).then(function(d){if(!d.ok){toast(d.message||'作成できません');return;}toast('作成しました');load(id);});}
 function init(){if(document.getElementById('crmCustomerSmartFab'))return;document.body.insertAdjacentHTML('beforeend','<button id="crmCustomerSmartFab">顧客スマート</button><div id="crmCustomerSmartPanel"><h3>顧客スマートパネル</h3><input id="crmSmartCustomerId" class="crmSmartInput" placeholder="顧客ID"><button id="crmSmartLoadBtn">読み込む</button><div id="crmSmartBody" style="margin-top:10px;color:#475569">顧客IDを入力して読み込んでください。</div></div>');}
 document.addEventListener('click',function(e){var t=e.target;if(!t)return;if(t.id==='crmCustomerSmartFab')openPanel();if(t.id==='crmSmartLoadBtn')load(document.getElementById('crmSmartCustomerId').value.trim());var a=t.getAttribute&&t.getAttribute('data-smart-act');if(a)runAction(a);if(t.getAttribute&&t.getAttribute('data-smart-reload'))load(document.getElementById('crmSmartCustomerId').value.trim());});
 document.addEventListener('DOMContentLoaded',init);setTimeout(init,800);
})();
</script>`;
  return html.replace("</head>", `${style}</head>`).replace("</body>", `${script}</body>`);
}

export default {
  async fetch(request, env, ctx){
    const url = new URL(request.url);
    const path = url.pathname;
    if((path === "/" || path === "/health" || path === "/api/health") && request.method === "GET") return json({ ok:true, service:"customer-crm-api", build:BUILD, time:nowIso() });
    const smartMatch = path.match(/^\/api\/customers\/([^/]+)\/smart-panel$/);
    if(smartMatch && request.method === "GET") return smartPanelApi(request, env, decodeURIComponent(smartMatch[1]));
    const actionMatch = path.match(/^\/api\/customers\/([^/]+)\/smart-action$/);
    if(actionMatch && request.method === "POST") return createNextActionApi(request, env, decodeURIComponent(actionMatch[1]));
    const res = await app.fetch(request, env, ctx);
    const type = res.headers.get("content-type") || "";
    if(type.includes("text/html")){
      const body = injectCustomerSmartPanel(await res.text());
      return new Response(body, { status:res.status, statusText:res.statusText, headers:securityHeaders(res.headers) });
    }
    return new Response(res.body, { status:res.status, statusText:res.statusText, headers:securityHeaders(res.headers) });
  }
};
