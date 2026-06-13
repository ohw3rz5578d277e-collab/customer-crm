// ======================================================
// CUSTOMER CRM / GROWTH SUITE ACTIONS WRAPPER
// build: customer-crm-api-growth-suite-actions-20260613-01
// Adds one-click progress advance for the growth suite panel.
// ======================================================

import app from "./production-index-crm-growth-suite.js";

const BUILD = "customer-crm-api-growth-suite-actions-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const WRITE_ROLES = ["root_admin", "admin", "staff"];

function text(v){return v===undefined||v===null?"":String(v).trim();}
function lower(v){return text(v).toLowerCase();}
function securityHeaders(headers={}){const h=new Headers(headers);h.set("cache-control","no-store, no-cache, must-revalidate, max-age=0");h.set("pragma","no-cache");h.set("expires","0");h.set("x-content-type-options","nosniff");h.set("referrer-policy","no-referrer");return h;}
function json(data,status=200){return new Response(JSON.stringify(data,null,2),{status,headers:securityHeaders({"content-type":"application/json; charset=utf-8"})});}
async function readJson(request){try{return await request.json();}catch(_){return {};}}
function getAccessEmail(request){return lower(request.headers.get("cf-access-authenticated-user-email")||request.headers.get("Cf-Access-Authenticated-User-Email")||request.headers.get("x-user-email")||"");}
async function addColumn(db,table,definition){try{await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run();}catch(_){}}
async function ensureSchema(env){
  if(!env.DB)throw new Error("D1 DB binding(DB) is missing");
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users(email TEXT PRIMARY KEY,role TEXT,status TEXT,created_by TEXT,created_at TEXT,updated_at TEXT)`).run();
  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email,role,status,created_by,created_at,updated_at) VALUES(?,'admin','active','system',datetime('now'),datetime('now'))`).bind(ROOT_ADMIN_EMAIL).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_reservation_progress(reservation_key TEXT PRIMARY KEY,crm_draft_id TEXT,reservation_app_reservation_id TEXT,customer_id TEXT,customer_name TEXT,genre TEXT,shoot_date TEXT,progress_status TEXT,progress_step INTEGER,next_action TEXT,next_due_date TEXT,completed_at TEXT,created_by TEXT,updated_by TEXT,created_at TEXT,updated_at TEXT,deleted_at TEXT,deleted_by TEXT)`).run();
  for(const col of ["delivery_progress_status TEXT","progress_step INTEGER","progress_updated_at TEXT"]) await addColumn(env.DB,"customer_reservations",col);
  await addColumn(env.DB,"crm_reservation_drafts","delivery_progress_status TEXT");
}
async function requireWriter(request,env){
  await ensureSchema(env);
  const email=getAccessEmail(request);
  if(!email)return {ok:false,response:json({ok:false,message:"Login required"},401)};
  const user=await env.DB.prepare(`SELECT email,role,status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`).bind(email).first();
  if(!user)return {ok:false,response:json({ok:false,message:"User is not allowed"},403)};
  if(!WRITE_ROLES.includes(user.role||""))return {ok:false,response:json({ok:false,message:"Permission denied"},403)};
  return {ok:true,email,user};
}
const STEPS=["予約確定","撮影前連絡済み","撮影完了","先行納品済み","本納品準備中","本納品済み","口コミ依頼済み","完了"];
function nextStatus(current){const i=STEPS.indexOf(text(current));return STEPS[Math.min(i<0?1:i+1,STEPS.length-1)]||"撮影前連絡済み";}
function addDays(dateText,days){const base=text(dateText)||new Date(Date.now()+9*60*60*1000).toISOString().slice(0,10);const d=new Date(`${base.slice(0,10)}T00:00:00+09:00`);d.setDate(d.getDate()+Number(days||0));return new Date(d.getTime()+9*60*60*1000).toISOString().slice(0,10);}
function info(status,shootDate){const s=text(status)||"予約確定";const map={"予約確定":[1,"撮影前連絡",-2],"撮影前連絡済み":[2,"撮影完了登録",0],"撮影完了":[3,"先行納品",7],"先行納品済み":[4,"本納品",60],"本納品準備中":[5,"本納品",60],"本納品済み":[6,"口コミ依頼",7],"口コミ依頼済み":[7,"完了",0],"完了":[8,"",0]};const m=map[s]||map["予約確定"];return {status:s,step:m[0],next_action:m[1],next_due_date:m[1]?addDays(shootDate,m[2]):""};}
async function advanceProgressApi(request,env,key,body){
  const auth=await requireWriter(request,env); if(!auth.ok)return auth.response;
  const current=await env.DB.prepare(`SELECT * FROM crm_reservation_progress WHERE reservation_key=? LIMIT 1`).bind(key).first();
  if(!current)return json({ok:false,message:"progress not found"},404);
  const target=text(body.status||body.progress_status)||nextStatus(current.progress_status);
  const p=info(target,current.shoot_date);
  await env.DB.prepare(`UPDATE crm_reservation_progress SET progress_status=?,progress_step=?,next_action=?,next_due_date=?,completed_at=?,updated_by=?,updated_at=CURRENT_TIMESTAMP WHERE reservation_key=?`).bind(p.status,p.step,p.next_action,p.next_due_date,p.status==="完了"?new Date().toISOString():text(current.completed_at),auth.email,key).run();
  await env.DB.prepare(`UPDATE customer_reservations SET delivery_progress_status=?,progress_step=?,progress_updated_at=CURRENT_TIMESTAMP WHERE customer_id=? AND (reservation_id=? OR event_key=?)`).bind(p.status,p.step,text(current.customer_id),text(current.reservation_app_reservation_id),key).run();
  await env.DB.prepare(`UPDATE crm_reservation_drafts SET delivery_progress_status=?,updated_at=CURRENT_TIMESTAMP WHERE id=?`).bind(p.status,text(current.crm_draft_id)).run();
  return json({ok:true,build:BUILD,reservation_key:key,progress:p});
}

export default {
  async fetch(request,env,ctx){
    const url=new URL(request.url);
    if((url.pathname==="/"||url.pathname==="/health"||url.pathname==="/api/health")&&request.method==="GET")return json({ok:true,service:"customer-crm-api",build:BUILD,time:new Date().toISOString()});
    const m=url.pathname.match(/^\/api\/reservation-progress\/([^/]+)$/);
    if(m&&request.method==="POST"){
      const clone=request.clone();
      const body=await readJson(request);
      if(text(body.status||body.progress_status)) return app.fetch(clone,env,ctx);
      return advanceProgressApi(clone,env,decodeURIComponent(m[1]),body);
    }
    return app.fetch(request,env,ctx);
  }
};
