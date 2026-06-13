// ======================================================
// CUSTOMER CRM / ALL ROADMAP SUITE WRAPPER
// build: customer-crm-api-all-roadmap-suite-20260613-01
// Adds: rank badges, customer summary, repeat/dormant leads, event prediction,
// template manager, sales dashboard, checklists, operation logs, inquiry pipeline.
// ======================================================

import app from "./production-index-crm-delivery-dashboard.js";

const BUILD = "customer-crm-api-all-roadmap-suite-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const READ_ROLES = ["root_admin", "admin", "staff", "viewer"];
const WRITE_ROLES = ["root_admin", "admin", "staff"];

function text(v){ return v === undefined || v === null ? "" : String(v).trim(); }
function lower(v){ return text(v).toLowerCase(); }
function num(v){ const n = Number(v || 0); return Number.isFinite(n) ? n : 0; }
function todayJst(){ return new Date(Date.now() + 9 * 60 * 60 * 1000).toISOString().slice(0,10); }
function isoNow(){ return new Date().toISOString(); }
function daysBetween(dateText){ if(!text(dateText)) return null; return Math.floor((new Date(`${todayJst()}T00:00:00+09:00`) - new Date(`${text(dateText).slice(0,10)}T00:00:00+09:00`)) / 86400000); }
function addDays(dateText, days){ const d = new Date(`${(text(dateText)||todayJst()).slice(0,10)}T00:00:00+09:00`); d.setDate(d.getDate() + Number(days || 0)); return new Date(d.getTime() + 9*60*60*1000).toISOString().slice(0,10); }
function ym(dateText){ return text(dateText || todayJst()).slice(0,7); }
function securityHeaders(headers={}){ const h = new Headers(headers); h.set("cache-control","no-store, no-cache, must-revalidate, max-age=0"); h.set("pragma","no-cache"); h.set("expires","0"); h.set("x-content-type-options","nosniff"); h.set("referrer-policy","no-referrer"); h.set("x-frame-options","DENY"); return h; }
function json(data,status=200,headers={}){ return new Response(JSON.stringify(data,null,2),{status,headers:securityHeaders({"content-type":"application/json; charset=utf-8",...headers})}); }
async function readJson(request){ try{return await request.json();}catch(_){return {};} }
function getAccessEmail(request){ return lower(request.headers.get("cf-access-authenticated-user-email") || request.headers.get("Cf-Access-Authenticated-User-Email") || request.headers.get("x-user-email") || ""); }
async function addColumn(db, table, definition){ try{ await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run(); }catch(_){} }
async function safeAll(env, sql, bindings=[]){ try{ const s=env.DB.prepare(sql); const r=bindings.length?await s.bind(...bindings).all():await s.all(); return r.results || []; }catch(_){ return []; } }
async function safeFirst(env, sql, bindings=[]){ try{ const s=env.DB.prepare(sql); return bindings.length?await s.bind(...bindings).first():await s.first(); }catch(_){ return null; } }

async function ensureSchema(env){
  if(!env.DB) throw new Error("D1 DB binding(DB) is missing");
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users(email TEXT PRIMARY KEY,role TEXT,status TEXT,created_by TEXT,created_at TEXT,updated_at TEXT)`).run();
  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email,role,status,created_by,created_at,updated_at) VALUES(?,'admin','active','system',datetime('now'),datetime('now'))`).bind(ROOT_ADMIN_EMAIL).run();

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_checklist_items(
    id TEXT PRIMARY KEY,
    checklist_type TEXT NOT NULL,
    target_type TEXT,
    target_id TEXT,
    customer_id TEXT,
    customer_name TEXT,
    title TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    due_date TEXT,
    priority TEXT DEFAULT 'medium',
    completed_at TEXT,
    completed_by TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    deleted_at TEXT,
    deleted_by TEXT
  )`).run();

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_inquiry_pipeline(
    id TEXT PRIMARY KEY,
    customer_id TEXT,
    customer_name TEXT,
    source TEXT,
    inquiry_text TEXT,
    inquiry_status TEXT NOT NULL DEFAULT '問い合わせ',
    expected_genre TEXT,
    expected_date TEXT,
    expected_amount REAL DEFAULT 0,
    next_action TEXT,
    next_due_date TEXT,
    lost_reason TEXT,
    assigned_to TEXT,
    created_by TEXT,
    updated_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    deleted_at TEXT,
    deleted_by TEXT
  )`).run();

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_operation_logs_unified(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    log_type TEXT,
    action_label TEXT,
    target_type TEXT,
    target_id TEXT,
    customer_id TEXT,
    customer_name TEXT,
    before_json TEXT,
    after_json TEXT,
    result TEXT,
    actor_email TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
  )`).run();

  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_auto_tags(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id TEXT,
    tag_name TEXT,
    tag_reason TEXT,
    source TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(customer_id, tag_name)
  )`).run();

  for(const col of ["customer_rank TEXT","customer_rank_reason TEXT","customer_rank_updated_at TEXT","next_event_suggestion TEXT","next_event_reason TEXT","next_line_suggestion TEXT","next_line_reason TEXT","inquiry_status TEXT"]) await addColumn(env.DB,"customers",col);
  for(const col of ["checklist_ready_at TEXT","shooting_day_notes TEXT","delivery_checklist_status TEXT"]) await addColumn(env.DB,"crm_reservation_drafts",col);
  for(const col of ["checklist_status TEXT","shooting_day_notes TEXT"]) await addColumn(env.DB,"customer_reservations",col);
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_checklists_lookup ON crm_checklist_items(checklist_type,status,due_date,customer_id)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_pipeline_status ON crm_inquiry_pipeline(inquiry_status,next_due_date,customer_id)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_unified_logs ON crm_operation_logs_unified(log_type,created_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_auto_tags_customer ON crm_auto_tags(customer_id, tag_name)`).run();
}

async function requireUser(request, env, roles=READ_ROLES){
  await ensureSchema(env);
  const email=getAccessEmail(request);
  if(!email) return {ok:false,response:json({ok:false,message:"Login required"},401)};
  const user=await env.DB.prepare(`SELECT email,role,status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`).bind(email).first();
  if(!user) return {ok:false,response:json({ok:false,message:"User is not allowed"},403)};
  if(roles.length && !roles.includes(user.role || "")) return {ok:false,response:json({ok:false,message:"Permission denied"},403)};
  return {ok:true,email,user};
}
async function logOp(env, data){ try{ await env.DB.prepare(`INSERT INTO crm_operation_logs_unified(log_type,action_label,target_type,target_id,customer_id,customer_name,before_json,after_json,result,actor_email,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP)`).bind(text(data.log_type),text(data.action_label),text(data.target_type),text(data.target_id),text(data.customer_id),text(data.customer_name),text(data.before_json),text(data.after_json),text(data.result||"ok"),text(data.actor_email||"system")).run(); }catch(_){} }

function rankCustomer(row){
  const revenue=num(row.total_revenue); const repeat=num(row.repeat_count); const last=text(row.last_shoot_date); const since=daysBetween(last);
  if(revenue>=100000) return {rank:"VIP",reason:"累計売上10万円以上"};
  if(repeat>=2) return {rank:"リピーター",reason:"撮影2回以上"};
  if(since!==null && since>=180) return {rank:"休眠",reason:"最終撮影から180日以上"};
  if(revenue>0) return {rank:"新規",reason:"初回予約あり"};
  return {rank:"要フォロー",reason:"未予約・未提案、または情報不足"};
}
async function refreshCustomerRank(env, customerId){
  const stats=await safeFirst(env,`SELECT COUNT(*) AS repeat_count, COALESCE(SUM(COALESCE(total_amount,0)),0) AS total_revenue, MAX(shoot_date) AS last_shoot_date, GROUP_CONCAT(DISTINCT genre) AS genre_history FROM customer_reservations WHERE customer_id=? AND COALESCE(deleted_at,'')='' AND COALESCE(status,'') NOT IN ('cancelled','キャンセル')`,[customerId])||{};
  const rank=rankCustomer(stats);
  await env.DB.prepare(`UPDATE customers SET repeat_count=?,total_revenue=?,last_shoot_date=?,genre_history=?,customer_rank=?,customer_rank_reason=?,customer_rank_updated_at=CURRENT_TIMESTAMP WHERE id=?`).bind(num(stats.repeat_count),num(stats.total_revenue),text(stats.last_shoot_date),text(stats.genre_history),rank.rank,rank.reason,customerId).run();
  return {...stats,...rank};
}
function predictEvent(row){
  const genres=lower(row.genre_history || row.genre || ""); const last=text(row.last_shoot_date); const since=daysBetween(last);
  if(genres.includes("お宮参り")) return {event:"ハーフバースデー・1歳バースデー",reason:"お宮参り撮影後の成長イベント"};
  if(genres.includes("七五三")) return {event:"入学・卒業 / 家族写真",reason:"七五三後の次回提案"};
  if(genres.includes("バースデー")) return {event:"次回バースデー・家族写真",reason:"バースデー撮影履歴あり"};
  if(since!==null && since>=90) return {event:"季節の家族写真",reason:"前回撮影から90日以上"};
  return {event:"家族写真・季節撮影",reason:"汎用リピート提案"};
}
function nextLineSuggestion(row){
  const rank=text(row.customer_rank); const since=daysBetween(row.last_shoot_date);
  if(rank==="VIP" || rank==="リピーター") return {line:"リピーター向け次回提案",reason:"優良顧客の継続提案"};
  if(rank==="休眠" || (since!==null && since>=180)) return {line:"休眠掘り起こしLINE",reason:"最終撮影から期間経過"};
  if(text(row.next_event_suggestion)) return {line:`${row.next_event_suggestion}のご提案`,reason:"イベント予測に基づく提案"};
  return {line:"相談・次回撮影提案",reason:"要フォロー"};
}
async function ensureCustomerInsights(env, customer){
  const rank=text(customer.customer_rank) ? {rank:customer.customer_rank,reason:customer.customer_rank_reason} : await refreshCustomerRank(env, customer.id);
  const merged={...customer,...rank};
  const ev=predictEvent(merged); const line=nextLineSuggestion({...merged,next_event_suggestion:ev.event});
  await env.DB.prepare(`UPDATE customers SET next_event_suggestion=?,next_event_reason=?,next_line_suggestion=?,next_line_reason=? WHERE id=?`).bind(ev.event,ev.reason,line.line,line.reason,customer.id).run();
  return {...merged,next_event_suggestion:ev.event,next_event_reason:ev.reason,next_line_suggestion:line.line,next_line_reason:line.reason};
}

async function customerBadgesApi(request, env){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok) return auth.response;
  const rows=await safeAll(env,`SELECT id,name,kana,phone,email,repeat_count,total_revenue,last_shoot_date,customer_rank,customer_rank_reason,next_event_suggestion,next_line_suggestion FROM customers WHERE COALESCE(deleted_at,'')='' ORDER BY COALESCE(total_revenue,0) DESC, COALESCE(repeat_count,0) DESC LIMIT 1000`);
  return json({ok:true,build:BUILD,items:rows,counts:rows.reduce((a,r)=>{const k=text(r.customer_rank||"未判定");a[k]=(a[k]||0)+1;return a;},{})});
}
async function customerSummaryApi(request, env, customerId){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok) return auth.response;
  const customer=await safeFirst(env,`SELECT * FROM customers WHERE id=? AND COALESCE(deleted_at,'')='' LIMIT 1`,[customerId]);
  if(!customer) return json({ok:false,message:"customer not found"},404);
  const insight=await ensureCustomerInsights(env, customer);
  const line=await safeAll(env,`SELECT id,action_label,status,created_at FROM customer_line_draft_logs WHERE customer_id=? AND COALESCE(status,'') NOT IN ('sent','送信済み') ORDER BY datetime(created_at) DESC LIMIT 10`,[customerId]);
  const follow=await safeAll(env,`SELECT id,title,due_date,status,priority FROM crm_follow_tasks WHERE customer_id=? AND COALESCE(status,'') NOT IN ('completed','done','完了') ORDER BY date(COALESCE(due_date,'2999-12-31')) LIMIT 10`,[customerId]);
  const reservations=await safeAll(env,`SELECT reservation_id,genre,shoot_date,status,total_amount,delivery_progress_status FROM customer_reservations WHERE customer_id=? AND COALESCE(deleted_at,'')='' ORDER BY date(COALESCE(shoot_date,'1970-01-01')) DESC LIMIT 10`,[customerId]);
  const progress=await safeAll(env,`SELECT reservation_key,progress_status,next_action,next_due_date FROM crm_reservation_progress WHERE customer_id=? AND COALESCE(deleted_at,'')='' AND progress_status!='完了' ORDER BY date(COALESCE(next_due_date,'2999-12-31')) LIMIT 10`,[customerId]);
  return json({ok:true,build:BUILD,customer:insight,summary:{rank:insight.customer_rank,next_event:insight.next_event_suggestion,next_line:insight.next_line_suggestion,unread_line_count:line.length,open_follow_count:follow.length,open_progress_count:progress.length},line,follow,reservations,progress});
}

async function repeatCandidatesApi(request, env){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok) return auth.response;
  const rows=await safeAll(env,`SELECT id,name,kana,phone,email,repeat_count,total_revenue,last_shoot_date,genre_history,customer_rank,next_event_suggestion,next_line_suggestion FROM customers WHERE COALESCE(deleted_at,'')='' AND COALESCE(total_revenue,0)>0 ORDER BY date(COALESCE(last_shoot_date,'1970-01-01')) ASC LIMIT 1000`);
  const items=[];
  for(const row of rows){
    const since=daysBetween(row.last_shoot_date); if(since===null || since<90) continue;
    const x=await ensureCustomerInsights(env,row);
    items.push({...x,days_since_last:since,proposal:x.next_event_suggestion,reason:x.next_event_reason});
  }
  return json({ok:true,build:BUILD,items:items.slice(0,300),counts:{total:items.length,high:items.filter(x=>x.days_since_last>=180).length}});
}
async function dormantCustomersApi(request, env){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok) return auth.response;
  const rows=await safeAll(env,`SELECT id,name,kana,phone,email,total_revenue,repeat_count,last_shoot_date,genre_history,customer_rank FROM customers WHERE COALESCE(deleted_at,'')='' AND COALESCE(total_revenue,0)>0 ORDER BY date(COALESCE(last_shoot_date,'1970-01-01')) ASC LIMIT 1000`);
  const items=[]; for(const row of rows){const since=daysBetween(row.last_shoot_date); if(since!==null && since>=180){const x=await ensureCustomerInsights(env,row); items.push({...x,days_since_last:since,revival_line:"休眠掘り起こしLINE"});}}
  return json({ok:true,build:BUILD,items:items.slice(0,300),counts:{total:items.length}});
}
async function eventPredictionsApi(request, env){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok) return auth.response;
  const rows=await safeAll(env,`SELECT id,name,total_revenue,repeat_count,last_shoot_date,genre_history,next_event_suggestion,next_event_reason FROM customers WHERE COALESCE(deleted_at,'')='' ORDER BY date(COALESCE(last_shoot_date,'1970-01-01')) DESC LIMIT 500`);
  const items=[]; for(const row of rows){items.push(await ensureCustomerInsights(env,row));}
  return json({ok:true,build:BUILD,items});
}
async function autoTagsApi(request, env){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok) return auth.response;
  const rows=await safeAll(env,`SELECT id,name,customer_rank,total_revenue,repeat_count,last_shoot_date,genre_history,next_event_suggestion FROM customers WHERE COALESCE(deleted_at,'')='' LIMIT 1000`);
  let inserted=0;
  for(const r0 of rows){const r=await ensureCustomerInsights(env,r0); const tags=[]; if(r.customer_rank) tags.push([r.customer_rank,r.customer_rank_reason]); if(daysBetween(r.last_shoot_date)>=180) tags.push(["休眠", "最終撮影から180日以上"]); if(lower(r.genre_history).includes("七五三")) tags.push(["七五三撮影済み","撮影履歴から自動付与"]); if(lower(r.genre_history).includes("お宮参り")) tags.push(["お宮参り済み","撮影履歴から自動付与"]); if(text(r.next_event_suggestion)) tags.push([`提案:${r.next_event_suggestion}`,"次回イベント予測"]);
    for(const [tag,reason] of tags){try{await env.DB.prepare(`INSERT OR IGNORE INTO crm_auto_tags(customer_id,tag_name,tag_reason,source,created_at) VALUES(?,?,?,?,CURRENT_TIMESTAMP)`).bind(r.id,tag,reason,"auto").run(); inserted++;}catch(_){}}
  }
  return json({ok:true,inserted});
}

const PRE_ITEMS=["神社・場所確認","集合場所確認","撮影時間確認","産着・服装案内","雨天時対応","支払い案内","前日LINE送信"];
const SHOOT_ITEMS=["お名前・兄弟構成確認","祖父母参加確認","希望カット確認","注意事項確認","駐車場・集合場所確認"];
const DELIVERY_ITEMS=["セレクト完了","色味調整完了","レタッチ完了","先行15枚納品","全データ納品","URL送信","保存期限案内","口コミ依頼"];
async function createChecklist(env, type, targetType, targetId, customerId, customerName, items, actor){
  const out=[]; let i=0; for(const title of items){i++; const id=`ck-${type}-${targetId}-${i}`.replace(/[^a-zA-Z0-9_-]/g,"-"); await env.DB.prepare(`INSERT OR IGNORE INTO crm_checklist_items(id,checklist_type,target_type,target_id,customer_id,customer_name,title,status,due_date,priority,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`).bind(id,type,targetType,targetId,customerId,customerName,title,"open","","medium",actor||"system").run(); out.push(id);} return out;
}
async function checklistApi(request, env){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok) return auth.response;
  const url=new URL(request.url); const type=text(url.searchParams.get("type")); const status=text(url.searchParams.get("status")); const customer=text(url.searchParams.get("customer_id")); const b=[]; let w=`WHERE COALESCE(deleted_at,'')=''`; if(type){w+=` AND checklist_type=?`;b.push(type);} if(status){w+=` AND status=?`;b.push(status);} if(customer){w+=` AND customer_id=?`;b.push(customer);} const rows=await safeAll(env,`SELECT * FROM crm_checklist_items ${w} ORDER BY checklist_type, customer_name, id LIMIT 1000`,b); return json({ok:true,items:rows,counts:rows.reduce((a,r)=>{a[r.status]=(a[r.status]||0)+1;return a;},{})});
}
async function checklistSeedApi(request, env){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok) return auth.response;
  const body=await readJson(request); const customerId=text(body.customer_id); const targetId=text(body.target_id||customerId||Date.now()); const customerName=text(body.customer_name); const type=text(body.type||"pre_shoot"); const items=type==="delivery"?DELIVERY_ITEMS:type==="shoot_day"?SHOOT_ITEMS:PRE_ITEMS; const ids=await createChecklist(env,type,text(body.target_type||"customer"),targetId,customerId,customerName,items,auth.email); return json({ok:true,type,ids});
}
async function checklistCompleteApi(request, env, id){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok) return auth.response;
  const before=await safeFirst(env,`SELECT * FROM crm_checklist_items WHERE id=? LIMIT 1`,[id]); if(!before) return json({ok:false,message:"checklist not found"},404);
  await env.DB.prepare(`UPDATE crm_checklist_items SET status='completed',completed_at=CURRENT_TIMESTAMP,completed_by=?,updated_at=CURRENT_TIMESTAMP WHERE id=?`).bind(auth.email,id).run();
  await logOp(env,{log_type:"checklist",action_label:"チェック完了",target_type:"checklist",target_id:id,customer_id:before.customer_id,customer_name:before.customer_name,before_json:JSON.stringify(before),after_json:JSON.stringify({status:"completed"}),actor_email:auth.email});
  return json({ok:true,id});
}

async function salesDashboardApi(request, env){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok) return auth.response;
  const month=text(new URL(request.url).searchParams.get("month")||ym(todayJst()));
  const all=await safeFirst(env,`SELECT COUNT(*) AS count, COALESCE(SUM(COALESCE(total_amount,0)),0) AS sales, AVG(COALESCE(total_amount,0)) AS avg_amount FROM customer_reservations WHERE COALESCE(deleted_at,'')='' AND COALESCE(status,'') NOT IN ('cancelled','キャンセル') AND substr(COALESCE(shoot_date,''),1,7)=?`,[month])||{};
  const genre=await safeAll(env,`SELECT genre, COUNT(*) AS count, COALESCE(SUM(COALESCE(total_amount,0)),0) AS sales FROM customer_reservations WHERE COALESCE(deleted_at,'')='' AND COALESCE(status,'') NOT IN ('cancelled','キャンセル') AND substr(COALESCE(shoot_date,''),1,7)=? GROUP BY genre ORDER BY sales DESC`,[month]);
  const repeat=await safeFirst(env,`SELECT COUNT(*) AS count, COALESCE(SUM(COALESCE(total_amount,0)),0) AS sales FROM customer_reservations WHERE COALESCE(deleted_at,'')='' AND COALESCE(status,'') NOT IN ('cancelled','キャンセル') AND substr(COALESCE(shoot_date,''),1,7)=? AND customer_id IN (SELECT id FROM customers WHERE COALESCE(repeat_count,0)>=2)`,[month])||{};
  const cancelled=await safeFirst(env,`SELECT COUNT(*) AS count FROM customer_reservations WHERE substr(COALESCE(shoot_date,''),1,7)=? AND COALESCE(status,'') IN ('cancelled','キャンセル')`,[month])||{};
  const undelivered=await safeFirst(env,`SELECT COUNT(*) AS count FROM crm_reservation_progress WHERE COALESCE(deleted_at,'')='' AND progress_status!='完了'`,[])||{};
  return json({ok:true,month,summary:{sales:num(all.sales),count:num(all.count),avg_amount:num(all.avg_amount),repeat_sales:num(repeat.sales),repeat_count:num(repeat.count),cancelled_count:num(cancelled.count),undelivered_count:num(undelivered.count)},genre});
}
async function operationLogsApi(request, env){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok) return auth.response;
  const url=new URL(request.url); const type=text(url.searchParams.get("type")); const b=[]; let w=""; if(type){w="WHERE log_type=?";b.push(type);} const rows=await safeAll(env,`SELECT * FROM crm_operation_logs_unified ${w} ORDER BY datetime(created_at) DESC LIMIT 200`,b); return json({ok:true,items:rows});
}
async function pipelineListApi(request, env){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok) return auth.response;
  const url=new URL(request.url); const status=text(url.searchParams.get("status")); const b=[]; let w=`WHERE COALESCE(deleted_at,'')=''`; if(status){w+=` AND inquiry_status=?`;b.push(status);} const rows=await safeAll(env,`SELECT * FROM crm_inquiry_pipeline ${w} ORDER BY date(COALESCE(next_due_date,'2999-12-31')) ASC, datetime(updated_at) DESC LIMIT 500`,b); return json({ok:true,items:rows,counts:rows.reduce((a,r)=>{a[r.inquiry_status]=(a[r.inquiry_status]||0)+1;return a;},{})});
}
async function pipelineSaveApi(request, env){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok) return auth.response;
  const b=await readJson(request); const id=text(b.id)||`inq-${Date.now()}-${Math.random().toString(36).slice(2,8)}`;
  await env.DB.prepare(`INSERT INTO crm_inquiry_pipeline(id,customer_id,customer_name,source,inquiry_text,inquiry_status,expected_genre,expected_date,expected_amount,next_action,next_due_date,lost_reason,assigned_to,created_by,updated_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP) ON CONFLICT(id) DO UPDATE SET customer_id=excluded.customer_id,customer_name=excluded.customer_name,source=excluded.source,inquiry_text=excluded.inquiry_text,inquiry_status=excluded.inquiry_status,expected_genre=excluded.expected_genre,expected_date=excluded.expected_date,expected_amount=excluded.expected_amount,next_action=excluded.next_action,next_due_date=excluded.next_due_date,lost_reason=excluded.lost_reason,assigned_to=excluded.assigned_to,updated_by=excluded.updated_by,updated_at=CURRENT_TIMESTAMP`).bind(id,text(b.customer_id),text(b.customer_name),text(b.source||"LINE"),text(b.inquiry_text),text(b.inquiry_status||"問い合わせ"),text(b.expected_genre),text(b.expected_date),num(b.expected_amount),text(b.next_action),text(b.next_due_date),text(b.lost_reason),text(b.assigned_to),auth.email,auth.email).run();
  await logOp(env,{log_type:"pipeline",action_label:"問い合わせステータス保存",target_type:"inquiry",target_id:id,customer_id:b.customer_id,customer_name:b.customer_name,after_json:JSON.stringify(b),actor_email:auth.email});
  return json({ok:true,id});
}

async function roadmapOverviewApi(request, env){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok) return auth.response;
  const ranks=await safeAll(env,`SELECT customer_rank,COUNT(*) AS n FROM customers WHERE COALESCE(deleted_at,'')='' GROUP BY customer_rank`);
  const repeats=await repeatCandidatesApi(new Request(request.url,{headers:request.headers}),env).then(r=>r.json()).catch(()=>({items:[]}));
  const dormant=await dormantCustomersApi(new Request(request.url,{headers:request.headers}),env).then(r=>r.json()).catch(()=>({items:[]}));
  const check=await safeFirst(env,`SELECT COUNT(*) AS n FROM crm_checklist_items WHERE COALESCE(status,'open')!='completed' AND COALESCE(deleted_at,'')=''`);
  const pipeline=await safeFirst(env,`SELECT COUNT(*) AS n FROM crm_inquiry_pipeline WHERE COALESCE(deleted_at,'')='' AND inquiry_status NOT IN ('予約確定','失注')`);
  const sales=await salesDashboardApi(new Request(request.url,{headers:request.headers}),env).then(r=>r.json()).catch(()=>({summary:{}}));
  return json({ok:true,build:BUILD,counts:{ranks:Object.fromEntries(ranks.map(r=>[text(r.customer_rank||"未判定"),num(r.n)])),repeat_candidates:repeats.items.length,dormant:dormant.items.length,open_checklists:num(check&&check.n),open_inquiries:num(pipeline&&pipeline.n),month_sales:sales.summary&&sales.summary.sales||0},top:{repeat_candidates:repeats.items.slice(0,5),dormant:dormant.items.slice(0,5)}});
}

function injectRoadmapUi(html){
  if(!html || html.includes("crmRoadmapSuiteScript")) return html;
  const style=`<style id="crmRoadmapSuiteStyle">.crm-roadmap-panel{margin:10px auto 18px;max-width:1180px;border:1px solid #c7d2fe;background:linear-gradient(135deg,#eef2ff,#fff);border-radius:18px;padding:12px;box-shadow:0 12px 30px rgba(79,70,229,.08);font-family:inherit;color:#111827}.crm-roadmap-head{display:flex;justify-content:space-between;gap:10px;flex-wrap:wrap}.crm-roadmap-title{font-size:18px;font-weight:950;margin:0}.crm-roadmap-sub{font-size:12px;color:#4f46e5;margin:4px 0 0}.crm-roadmap-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:10px}.crm-roadmap-actions button{border:0;background:#4f46e5;color:#fff;border-radius:10px;padding:8px 10px;font-size:12px;font-weight:950;cursor:pointer}.crm-roadmap-grid{display:grid;grid-template-columns:repeat(6,minmax(110px,1fr));gap:8px;margin-top:10px}.crm-roadmap-card{background:#fff;border:1px solid #e0e7ff;border-radius:14px;padding:10px}.crm-roadmap-card b{display:block;font-size:20px}.crm-roadmap-card span{font-size:11px;color:#4f46e5;font-weight:900}.crm-roadmap-list{display:grid;grid-template-columns:repeat(2,1fr);gap:8px;margin-top:10px}.crm-roadmap-row{background:#fff;border:1px solid #e0e7ff;border-radius:14px;padding:10px;font-size:12px}.crm-roadmap-row b{font-weight:950}.crm-roadmap-row small{display:block;color:#64748b;line-height:1.45;margin-top:4px}@media(max-width:860px){.crm-roadmap-panel{margin:10px}.crm-roadmap-grid{grid-template-columns:repeat(2,1fr)}.crm-roadmap-list{grid-template-columns:1fr}}</style>`;
  const script=`<script id="crmRoadmapSuiteScript">(function(){if(window.__crmRoadmapSuiteInstalled)return;window.__crmRoadmapSuiteInstalled=true;function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}function api(u,o){return fetch(u,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},o||{})).then(function(r){return r.json().catch(function(){return{ok:false,status:r.status}})})}function panel(){return '<section id="crmRoadmapPanel" class="crm-roadmap-panel"><div class="crm-roadmap-head"><div><h2 class="crm-roadmap-title">CRM総合強化ダッシュボード</h2><p class="crm-roadmap-sub">ランク・リピーター候補・休眠掘り起こし・チェックリスト・売上・問い合わせ状況をまとめて確認します。</p></div><div class="crm-roadmap-actions"><button id="crmRoadmapReload">更新</button><button data-roadmap-open="/api/repeat-candidates">リピーター候補</button><button data-roadmap-open="/api/dormant-customers">休眠顧客</button><button data-roadmap-open="/api/sales-dashboard">売上</button><button data-roadmap-open="/api/inquiry-pipeline">問い合わせ</button></div></div><div id="crmRoadmapKpis" class="crm-roadmap-grid"></div><div id="crmRoadmapList" class="crm-roadmap-list"></div></section>'}function kpi(d){var c=d.counts||{};var r=c.ranks||{};var a=[['vip',r.VIP||0,'VIP'],['repeat',r['リピーター']||0,'リピーター'],['repeat_candidates',c.repeat_candidates||0,'候補'],['dormant',c.dormant||0,'休眠'],['check',c.open_checklists||0,'チェック'],['sales',c.month_sales||0,'今月売上']];return a.map(function(x){return '<div class="crm-roadmap-card"><b>'+esc(x[1])+'</b><span>'+esc(x[2])+'</span></div>'}).join('')}function rows(d){var a=[].concat((d.top&&d.top.repeat_candidates||[]).map(function(x){return{t:'リピーター候補: '+(x.name||'-'),m:(x.proposal||'-')+' / '+(x.reason||'')}}),(d.top&&d.top.dormant||[]).map(function(x){return{t:'休眠: '+(x.name||'-'),m:(x.next_line_suggestion||'掘り起こしLINE')+' / '+(x.days_since_last||'-')+'日経過'}}));return a.length?a.map(function(x){return '<div class="crm-roadmap-row"><b>'+esc(x.t)+'</b><small>'+esc(x.m)+'</small></div>'}).join(''):'<div class="crm-roadmap-row"><b>今すぐ表示する候補はありません</b></div>'}async function load(){var d=await api('/api/roadmap-suite/overview');var k=document.getElementById('crmRoadmapKpis'),l=document.getElementById('crmRoadmapList');if(!d.ok){if(l)l.innerHTML='<div class="crm-roadmap-row">読み込み失敗</div>';return;}if(k)k.innerHTML=kpi(d);if(l)l.innerHTML=rows(d)}function install(){if(document.getElementById('crmRoadmapPanel'))return;var base=document.getElementById('crmDeliveryPanel')||document.getElementById('crmGrowthPanel')||document.getElementById('crmTodayFilterPanel')||document.querySelector('main')||document.body;if(base.insertAdjacentHTML)base.insertAdjacentHTML(base.tagName==='MAIN'?'afterbegin':'afterend',panel());load()}document.addEventListener('click',function(e){var t=e.target;if(!t)return;if(t.id==='crmRoadmapReload')load();var u=t.getAttribute&&t.getAttribute('data-roadmap-open');if(u)api(u).then(function(d){alert(JSON.stringify(d,null,2).slice(0,5000))})});document.addEventListener('DOMContentLoaded',install);setTimeout(install,1500);setInterval(load,180000);})();</script>`;
  return html.replace("</head>",`${style}</head>`).replace("</body>",`${script}</body>`);
}

function idFrom(path, re){ const m=path.match(re); return m?decodeURIComponent(m[1]):""; }

export default { async fetch(request,env,ctx){
  const url=new URL(request.url); const path=url.pathname;
  if((path==="/"||path==="/health"||path==="/api/health")&&request.method==="GET") return json({ok:true,service:"customer-crm-api",build:BUILD,time:isoNow()});
  if(path==="/api/roadmap-suite/overview"&&request.method==="GET") return roadmapOverviewApi(request,env);
  if(path==="/api/customer-badges"&&request.method==="GET") return customerBadgesApi(request,env);
  const summaryId=idFrom(path,/^\/api\/customers\/([^/]+)\/summary-plus$/); if(summaryId&&request.method==="GET") return customerSummaryApi(request,env,summaryId);
  if(path==="/api/repeat-candidates"&&request.method==="GET") return repeatCandidatesApi(request,env);
  if(path==="/api/dormant-customers"&&request.method==="GET") return dormantCustomersApi(request,env);
  if(path==="/api/event-predictions"&&request.method==="GET") return eventPredictionsApi(request,env);
  if(path==="/api/auto-tags/rebuild"&&request.method==="POST") return autoTagsApi(request,env);
  if(path==="/api/checklists"&&request.method==="GET") return checklistApi(request,env);
  if(path==="/api/checklists/seed"&&request.method==="POST") return checklistSeedApi(request,env);
  const ckId=idFrom(path,/^\/api\/checklists\/([^/]+)\/complete$/); if(ckId&&request.method==="POST") return checklistCompleteApi(request,env,ckId);
  if(path==="/api/sales-dashboard"&&request.method==="GET") return salesDashboardApi(request,env);
  if(path==="/api/operation-logs"&&request.method==="GET") return operationLogsApi(request,env);
  if(path==="/api/inquiry-pipeline"&&request.method==="GET") return pipelineListApi(request,env);
  if(path==="/api/inquiry-pipeline"&&request.method==="POST") return pipelineSaveApi(request,env);

  const res=await app.fetch(request,env,ctx);
  const type=res.headers.get("content-type")||"";
  if(type.includes("text/html")){ const body=injectRoadmapUi(await res.text()); return new Response(body,{status:res.status,statusText:res.statusText,headers:securityHeaders(res.headers)}); }
  return new Response(res.body,{status:res.status,statusText:res.statusText,headers:securityHeaders(res.headers)});
}};
