// ======================================================
// CUSTOMER CRM / OPS SCREENS WRAPPER
// build: customer-crm-api-ops-screens-20260613-01
// Adds full-screen style management panels for LINE templates and inquiry pipeline.
// ======================================================

import app from "./production-index-crm-all-roadmap-suite.js";

const BUILD = "customer-crm-api-ops-screens-20260613-01";
const ROOT_ADMIN_EMAIL = "ohw3rz5578d277e@gmail.com";
const READ_ROLES = ["root_admin", "admin", "staff", "viewer"];
const WRITE_ROLES = ["root_admin", "admin", "staff"];

function text(v){ return v === undefined || v === null ? "" : String(v).trim(); }
function lower(v){ return text(v).toLowerCase(); }
function json(data,status=200,headers={}){ return new Response(JSON.stringify(data,null,2),{status,headers:secure({"content-type":"application/json; charset=utf-8",...headers})}); }
function secure(headers={}){ const h = new Headers(headers); h.set("cache-control","no-store, no-cache, must-revalidate, max-age=0"); h.set("pragma","no-cache"); h.set("expires","0"); h.set("x-content-type-options","nosniff"); h.set("referrer-policy","no-referrer"); h.set("x-frame-options","DENY"); return h; }
async function readJson(request){ try { return await request.json(); } catch(_) { return {}; } }
function emailFrom(request){ return lower(request.headers.get("cf-access-authenticated-user-email") || request.headers.get("Cf-Access-Authenticated-User-Email") || request.headers.get("x-user-email") || ""); }
async function addColumn(db,table,definition){ try { await db.prepare(`ALTER TABLE ${table} ADD COLUMN ${definition}`).run(); } catch(_){} }
async function ensureSchema(env){
  if(!env.DB) throw new Error("D1 DB binding(DB) is missing");
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_admin_users(email TEXT PRIMARY KEY, role TEXT, status TEXT, created_by TEXT, created_at TEXT, updated_at TEXT)`).run();
  await env.DB.prepare(`INSERT OR IGNORE INTO crm_admin_users(email,role,status,created_by,created_at,updated_at) VALUES(?,'admin','active','system',datetime('now'),datetime('now'))`).bind(ROOT_ADMIN_EMAIL).run();
  await env.DB.prepare(`CREATE TABLE IF NOT EXISTS crm_line_templates(
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT,
    genre TEXT,
    body TEXT NOT NULL,
    variables_json TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    usage_count INTEGER NOT NULL DEFAULT 0,
    created_by TEXT,
    updated_by TEXT,
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
    status TEXT,
    inquiry_text TEXT,
    next_action TEXT,
    next_due_date TEXT,
    expected_amount INTEGER DEFAULT 0,
    lost_reason TEXT,
    memo TEXT,
    created_by TEXT,
    updated_by TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    deleted_at TEXT,
    deleted_by TEXT
  )`).run();
  for(const c of ["source TEXT","next_action TEXT","next_due_date TEXT","expected_amount INTEGER DEFAULT 0","lost_reason TEXT","memo TEXT","updated_by TEXT","deleted_at TEXT","deleted_by TEXT"]) await addColumn(env.DB,"crm_inquiry_pipeline",c);
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_tpl_ops ON crm_line_templates(status, category, genre, deleted_at)`).run();
  await env.DB.prepare(`CREATE INDEX IF NOT EXISTS idx_crm_pipe_ops ON crm_inquiry_pipeline(status, next_due_date, updated_at, deleted_at)`).run();
}
async function requireUser(request,env,roles=READ_ROLES){
  await ensureSchema(env);
  const email=emailFrom(request);
  if(!email) return {ok:false,response:json({ok:false,message:"Login required"},401)};
  const user=await env.DB.prepare(`SELECT email,role,status FROM crm_admin_users WHERE lower(email)=lower(?) AND status='active' LIMIT 1`).bind(email).first();
  if(!user) return {ok:false,response:json({ok:false,message:"User is not allowed"},403)};
  if(roles.length && !roles.includes(user.role||"")) return {ok:false,response:json({ok:false,message:"Permission denied"},403)};
  return {ok:true,email,user};
}
async function all(env,sql,args=[]){ try{ const s=env.DB.prepare(sql); const r=args.length?await s.bind(...args).all():await s.all(); return r.results||[]; } catch(_){ return []; } }
async function first(env,sql,args=[]){ try{ const s=env.DB.prepare(sql); return args.length?await s.bind(...args).first():await s.first(); } catch(_){ return null; } }
function id(prefix){ return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2,8)}`; }

async function templatesApi(request,env){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok)return auth.response;
  const url=new URL(request.url);
  const q=lower(url.searchParams.get("q")); const category=text(url.searchParams.get("category")); const genre=text(url.searchParams.get("genre"));
  const args=[]; let where=`WHERE COALESCE(deleted_at,'')=''`;
  if(category){ where += ` AND category=?`; args.push(category); }
  if(genre){ where += ` AND (genre=? OR COALESCE(genre,'')='')`; args.push(genre); }
  if(q){ where += ` AND (lower(name) LIKE ? OR lower(body) LIKE ? OR lower(category) LIKE ?)`; args.push(`%${q}%`,`%${q}%`,`%${q}%`); }
  const rows=await all(env,`SELECT * FROM crm_line_templates ${where} ORDER BY CASE status WHEN 'active' THEN 0 ELSE 1 END, category, name LIMIT 500`,args);
  const cats=[...new Set(rows.map(r=>text(r.category||"未分類")).filter(Boolean))];
  return json({ok:true,build:BUILD,items:rows,categories:cats});
}
async function saveTemplateApi(request,env,templateId=""){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok)return auth.response;
  const b=await readJson(request); const tid=templateId||text(b.id)||id("tpl");
  if(!text(b.name)||!text(b.body)) return json({ok:false,message:"name and body are required"},400);
  await env.DB.prepare(`INSERT INTO crm_line_templates(id,name,category,genre,body,variables_json,status,created_by,updated_by,created_at,updated_at)
    VALUES(?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
    ON CONFLICT(id) DO UPDATE SET name=excluded.name, category=excluded.category, genre=excluded.genre, body=excluded.body, variables_json=excluded.variables_json, status=excluded.status, updated_by=excluded.updated_by, updated_at=CURRENT_TIMESTAMP`)
    .bind(tid,text(b.name),text(b.category||"汎用"),text(b.genre||""),text(b.body),text(b.variables_json||""),text(b.status||"active"),auth.email,auth.email).run();
  return json({ok:true,id:tid});
}
async function deleteTemplateApi(request,env,templateId){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok)return auth.response;
  await env.DB.prepare(`UPDATE crm_line_templates SET deleted_at=CURRENT_TIMESTAMP, deleted_by=?, status='deleted', updated_at=CURRENT_TIMESTAMP WHERE id=?`).bind(auth.email,templateId).run();
  return json({ok:true,id:templateId});
}
function render(body,data){ return text(body).replace(/\{\{\s*([a-zA-Z0-9_\-.]+)\s*\}\}/g,(_,k)=>text((data||{})[k]||"")); }
async function previewTemplateApi(request,env,templateId){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok)return auth.response;
  const b=await readJson(request); const tpl=templateId?await first(env,`SELECT * FROM crm_line_templates WHERE id=? AND COALESCE(deleted_at,'')='' LIMIT 1`,[templateId]):{body:b.body||""};
  if(!tpl) return json({ok:false,message:"template not found"},404);
  return json({ok:true,rendered:render(tpl.body,b.data||{customer_name:"山田様",genre:"七五三",shoot_date:"2026-11-15",customer_rank:"リピーター"})});
}
async function seedTemplatesApi(request,env){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok)return auth.response;
  const seeds=[
    ["tpl-before","撮影前案内","撮影前","","{{customer_name}}\n撮影日が近づいてきました。集合場所・服装・持ち物についてご不明点があればお気軽にご相談ください。"],
    ["tpl-thanks","撮影翌日：お礼","撮影後","","{{customer_name}}\n昨日は撮影ありがとうございました。ご家族の大切な時間をご一緒できて嬉しかったです。"],
    ["tpl-preview","先行納品案内","納品","","{{customer_name}}\n先行カットの準備ができました。まずは数枚お送りしますので、ご確認ください。"],
    ["tpl-delivery","本納品案内","納品","","{{customer_name}}\n本納品データの準備ができました。保存期限内にダウンロードをお願いいたします。"],
    ["tpl-review","口コミ依頼","口コミ","","{{customer_name}}\n撮影のご感想をいただけると励みになります。よろしければ一言だけでもお願いいたします。"],
    ["tpl-repeat","リピーター提案","リピート","","{{customer_name}}\n前回の撮影から少し経ちましたので、季節の記念やご家族写真もおすすめです。"],
    ["tpl-dormant","休眠掘り起こし","リピート","","{{customer_name}}\nお久しぶりです。季節の家族写真や記念日の撮影など、また気軽にご相談ください。"]
  ];
  for(const s of seeds){ await env.DB.prepare(`INSERT OR IGNORE INTO crm_line_templates(id,name,category,genre,body,status,created_by,updated_by,created_at,updated_at) VALUES(?,?,?,?,?,'active',?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)`).bind(s[0],s[1],s[2],s[3],s[4],auth.email,auth.email).run(); }
  return json({ok:true,inserted:seeds.length});
}

async function pipelineApi(request,env){
  const auth=await requireUser(request,env,READ_ROLES); if(!auth.ok)return auth.response;
  const url=new URL(request.url); const status=text(url.searchParams.get("status")); const q=lower(url.searchParams.get("q"));
  const args=[]; let where=`WHERE COALESCE(deleted_at,'')=''`;
  if(status){ where+=` AND status=?`; args.push(status); }
  if(q){ where+=` AND (lower(customer_name) LIKE ? OR lower(inquiry_text) LIKE ? OR lower(memo) LIKE ?)`; args.push(`%${q}%`,`%${q}%`,`%${q}%`); }
  const rows=await all(env,`SELECT * FROM crm_inquiry_pipeline ${where} ORDER BY date(COALESCE(next_due_date,'2999-12-31')) ASC, datetime(updated_at) DESC LIMIT 500`,args);
  const counts=rows.reduce((a,r)=>{const k=text(r.status||"未設定");a[k]=(a[k]||0)+1;return a;},{});
  return json({ok:true,build:BUILD,items:rows,counts,statuses:["問い合わせ","日程調整中","料金案内済み","返信待ち","仮予約","予約確定","失注"]});
}
async function savePipelineApi(request,env,pipelineId=""){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok)return auth.response;
  const b=await readJson(request); const pid=pipelineId||text(b.id)||id("inq");
  await env.DB.prepare(`INSERT INTO crm_inquiry_pipeline(id,customer_id,customer_name,source,status,inquiry_text,next_action,next_due_date,expected_amount,lost_reason,memo,created_by,updated_by,created_at,updated_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
    ON CONFLICT(id) DO UPDATE SET customer_id=excluded.customer_id, customer_name=excluded.customer_name, source=excluded.source, status=excluded.status, inquiry_text=excluded.inquiry_text, next_action=excluded.next_action, next_due_date=excluded.next_due_date, expected_amount=excluded.expected_amount, lost_reason=excluded.lost_reason, memo=excluded.memo, updated_by=excluded.updated_by, updated_at=CURRENT_TIMESTAMP`)
    .bind(pid,text(b.customer_id),text(b.customer_name),text(b.source||"LINE"),text(b.status||"問い合わせ"),text(b.inquiry_text),text(b.next_action),text(b.next_due_date),Number(b.expected_amount||0),text(b.lost_reason),text(b.memo),auth.email,auth.email).run();
  return json({ok:true,id:pid});
}
async function deletePipelineApi(request,env,pipelineId){
  const auth=await requireUser(request,env,WRITE_ROLES); if(!auth.ok)return auth.response;
  await env.DB.prepare(`UPDATE crm_inquiry_pipeline SET deleted_at=CURRENT_TIMESTAMP, deleted_by=?, updated_at=CURRENT_TIMESTAMP WHERE id=?`).bind(auth.email,pipelineId).run();
  return json({ok:true,id:pipelineId});
}

function injectOpsScreens(html){
  if(!html || html.includes("crmOpsScreensScript")) return html;
  const style=`<style id="crmOpsScreensStyle">
.crm-ops-btns{position:fixed;left:14px;bottom:82px;z-index:1000003;display:flex;flex-direction:column;gap:8px}.crm-ops-btns button{border:0;border-radius:999px;background:#111827;color:#fff;padding:10px 12px;font-weight:950;font-size:12px;box-shadow:0 10px 28px rgba(0,0,0,.18);cursor:pointer}.crm-ops-modal{position:fixed;inset:0;z-index:1000008;background:rgba(15,23,42,.56);display:flex;align-items:center;justify-content:center;padding:18px}.crm-ops-box{background:#fff;color:#0f172a;border-radius:18px;max-width:1100px;width:min(1100px,96vw);max-height:88vh;overflow:auto;padding:16px;box-shadow:0 24px 80px rgba(0,0,0,.28)}.crm-ops-head{display:flex;justify-content:space-between;gap:10px;align-items:flex-start}.crm-ops-head h2{margin:0;font-size:20px}.crm-ops-close{border:0;background:#e5e7eb;border-radius:10px;padding:7px 10px;font-weight:900;cursor:pointer}.crm-ops-tools{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}.crm-ops-tools input,.crm-ops-tools select,.crm-ops-tools textarea{border:1px solid #cbd5e1;border-radius:10px;padding:8px;font-size:12px}.crm-ops-tools button,.crm-ops-row button{border:0;background:#2563eb;color:#fff;border-radius:10px;padding:8px 10px;font-weight:900;font-size:12px;cursor:pointer}.crm-ops-grid{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}.crm-ops-row{border:1px solid #e5e7eb;border-radius:14px;padding:10px;background:#f8fafc}.crm-ops-row b{display:block}.crm-ops-row small{display:block;color:#64748b;white-space:pre-wrap;margin:5px 0}.crm-ops-form{display:grid;grid-template-columns:1fr 1fr;gap:8px;margin:10px 0}.crm-ops-form textarea{grid-column:1/-1;min-height:90px}.crm-ops-form input,.crm-ops-form select,.crm-ops-form textarea{border:1px solid #cbd5e1;border-radius:10px;padding:9px}.crm-ops-kpis{display:flex;gap:8px;flex-wrap:wrap}.crm-ops-kpi{background:#eff6ff;border:1px solid #bfdbfe;border-radius:12px;padding:8px 10px;font-size:12px;font-weight:900}@media(max-width:760px){.crm-ops-grid,.crm-ops-form{grid-template-columns:1fr}.crm-ops-btns{left:10px;bottom:70px}}
</style>`;
  const script=`<script id="crmOpsScreensScript">
(function(){
 if(window.__crmOpsScreensInstalled)return; window.__crmOpsScreensInstalled=true;
 function esc(v){return String(v==null?'':v).replace(/[&<>"']/g,function(c){return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];});}
 function api(url,opt){return fetch(url,Object.assign({credentials:'same-origin',cache:'no-store',headers:{'content-type':'application/json'}},opt||{})).then(function(r){return r.json().catch(function(){return {ok:false,status:r.status}})})}
 function toast(msg){var d=document.createElement('div');d.textContent=msg;d.style.cssText='position:fixed;right:16px;bottom:16px;z-index:1000010;background:#111827;color:#fff;padding:10px 13px;border-radius:12px;font-weight:900';document.body.appendChild(d);setTimeout(function(){d.remove()},2600)}
 function modal(title,body){var old=document.getElementById('crmOpsModal');if(old)old.remove();var d=document.createElement('div');d.id='crmOpsModal';d.className='crm-ops-modal';d.innerHTML='<div class="crm-ops-box"><div class="crm-ops-head"><h2>'+esc(title)+'</h2><button class="crm-ops-close" id="crmOpsClose">閉じる</button></div><div id="crmOpsBody">'+body+'</div></div>';document.body.appendChild(d)}
 function buttons(){if(document.getElementById('crmOpsBtns'))return;var d=document.createElement('div');d.id='crmOpsBtns';d.className='crm-ops-btns';d.innerHTML='<button id="crmOpenTpl">LINEテンプレ管理</button><button id="crmOpenPipe">問い合わせ管理</button>';document.body.appendChild(d)}
 function tplForm(t){t=t||{};return '<div class="crm-ops-form"><input id="tplName" placeholder="テンプレ名" value="'+esc(t.name||'')+'"><input id="tplCat" placeholder="カテゴリ" value="'+esc(t.category||'')+'"><input id="tplGenre" placeholder="ジャンル" value="'+esc(t.genre||'')+'"><select id="tplStatus"><option value="active">active</option><option value="draft">draft</option></select><textarea id="tplBody" placeholder="本文。{{customer_name}} など利用可">'+esc(t.body||'')+'</textarea><button id="tplSave" data-id="'+esc(t.id||'')+'">保存</button><button id="tplPreview">プレビュー</button></div>'}
 async function openTpl(){modal('LINEテンプレ管理','<div class="crm-ops-tools"><input id="tplQ" placeholder="検索"><button id="tplReload">検索</button><button id="tplNew">新規</button><button id="tplSeed">初期テンプレ作成</button></div><div id="tplForm">'+tplForm({})+'</div><div id="tplList" class="crm-ops-grid">読み込み中...</div>');loadTpl()}
 async function loadTpl(){var q=document.getElementById('tplQ');var d=await api('/api/ops/line-templates?q='+(q?encodeURIComponent(q.value):''));var list=document.getElementById('tplList');if(!d.ok){list.innerHTML='取得失敗';return}list.innerHTML=(d.items||[]).map(function(t){return '<div class="crm-ops-row"><b>'+esc(t.name)+'</b><small>['+esc(t.category||'-')+'] '+esc(t.genre||'')+' / '+esc(t.status||'')+' / 使用 '+esc(t.usage_count||0)+'回\n'+esc(t.body||'')+'</small><button data-tpl-edit="'+esc(t.id)+'">編集</button> <button data-tpl-del="'+esc(t.id)+'">削除</button></div>'}).join('')||'テンプレなし'}
 async function saveTpl(){var id=document.getElementById('tplSave').getAttribute('data-id');var body={name:tplName.value,category:tplCat.value,genre:tplGenre.value,status:tplStatus.value,body:tplBody.value};var d=await api(id?'/api/ops/line-templates/'+encodeURIComponent(id):'/api/ops/line-templates',{method:id?'PUT':'POST',body:JSON.stringify(body)});if(!d.ok){toast('保存失敗');return}toast('保存しました');document.getElementById('tplSave').setAttribute('data-id',d.id);loadTpl()}
 function pipeForm(p){p=p||{};var statuses=['問い合わせ','日程調整中','料金案内済み','返信待ち','仮予約','予約確定','失注'];return '<div class="crm-ops-form"><input id="pipeCustomer" placeholder="顧客名" value="'+esc(p.customer_name||'')+'"><input id="pipeCustomerId" placeholder="顧客ID" value="'+esc(p.customer_id||'')+'"><select id="pipeStatus">'+statuses.map(function(s){return '<option '+(s===(p.status||'問い合わせ')?'selected':'')+'>'+s+'</option>'}).join('')+'</select><input id="pipeSource" placeholder="流入元" value="'+esc(p.source||'LINE')+'"><input id="pipeDue" type="date" value="'+esc((p.next_due_date||'').slice(0,10))+'"><input id="pipeAmount" type="number" placeholder="見込金額" value="'+esc(p.expected_amount||'')+'"><textarea id="pipeText" placeholder="問い合わせ内容">'+esc(p.inquiry_text||'')+'</textarea><textarea id="pipeMemo" placeholder="メモ・次アクション">'+esc(p.memo||p.next_action||'')+'</textarea><button id="pipeSave" data-id="'+esc(p.id||'')+'">保存</button></div>'}
 async function openPipe(){modal('問い合わせ〜予約化 管理','<div class="crm-ops-tools"><select id="pipeFilter"><option value="">全ステータス</option><option>問い合わせ</option><option>日程調整中</option><option>料金案内済み</option><option>返信待ち</option><option>仮予約</option><option>予約確定</option><option>失注</option></select><input id="pipeQ" placeholder="検索"><button id="pipeReload">検索</button><button id="pipeNew">新規</button></div><div id="pipeForm">'+pipeForm({})+'</div><div id="pipeKpis" class="crm-ops-kpis"></div><div id="pipeList" class="crm-ops-grid">読み込み中...</div>');loadPipe()}
 async function loadPipe(){var st=document.getElementById('pipeFilter'),q=document.getElementById('pipeQ');var d=await api('/api/ops/inquiry-pipeline?status='+(st?encodeURIComponent(st.value):'')+'&q='+(q?encodeURIComponent(q.value):''));var list=document.getElementById('pipeList'),kp=document.getElementById('pipeKpis');if(!d.ok){list.innerHTML='取得失敗';return}kp.innerHTML=Object.keys(d.counts||{}).map(function(k){return '<span class="crm-ops-kpi">'+esc(k)+' '+esc(d.counts[k])+'</span>'}).join('');list.innerHTML=(d.items||[]).map(function(p){return '<div class="crm-ops-row"><b>'+esc(p.customer_name||'-')+'</b><small>'+esc(p.status||'-')+' / 次: '+esc(p.next_action||p.memo||'-')+' '+esc(p.next_due_date||'')+'\n'+esc(p.inquiry_text||'')+'</small><button data-pipe-edit="'+esc(p.id)+'">編集</button> <button data-pipe-del="'+esc(p.id)+'">削除</button></div>'}).join('')||'案件なし'}
 async function savePipe(){var id=document.getElementById('pipeSave').getAttribute('data-id');var b={customer_name:pipeCustomer.value,customer_id:pipeCustomerId.value,status:pipeStatus.value,source:pipeSource.value,next_due_date:pipeDue.value,expected_amount:pipeAmount.value,inquiry_text:pipeText.value,memo:pipeMemo.value,next_action:pipeMemo.value};var d=await api(id?'/api/ops/inquiry-pipeline/'+encodeURIComponent(id):'/api/ops/inquiry-pipeline',{method:id?'PUT':'POST',body:JSON.stringify(b)});if(!d.ok){toast('保存失敗');return}toast('保存しました');document.getElementById('pipeSave').setAttribute('data-id',d.id);loadPipe()}
 document.addEventListener('click',async function(e){var t=e.target;if(!t)return;if(t.id==='crmOpsClose'||t.id==='crmOpsModal')document.getElementById('crmOpsModal')?.remove();if(t.id==='crmOpenTpl')openTpl();if(t.id==='crmOpenPipe')openPipe();if(t.id==='tplReload')loadTpl();if(t.id==='tplNew')document.getElementById('tplForm').innerHTML=tplForm({});if(t.id==='tplSave')saveTpl();if(t.id==='tplSeed'){await api('/api/ops/line-templates/seed',{method:'POST',body:'{}'});toast('作成しました');loadTpl()}if(t.id==='tplPreview'){var d=await api('/api/ops/line-templates/preview',{method:'POST',body:JSON.stringify({body:tplBody.value})});alert(d.rendered||'')};var te=t.getAttribute&&t.getAttribute('data-tpl-edit');if(te){var d=await api('/api/ops/line-templates');var row=(d.items||[]).find(function(x){return x.id===te});document.getElementById('tplForm').innerHTML=tplForm(row||{})}var td=t.getAttribute&&t.getAttribute('data-tpl-del');if(td&&confirm('削除しますか？')){await api('/api/ops/line-templates/'+encodeURIComponent(td),{method:'DELETE'});loadTpl()}if(t.id==='pipeReload')loadPipe();if(t.id==='pipeNew')document.getElementById('pipeForm').innerHTML=pipeForm({});if(t.id==='pipeSave')savePipe();var pe=t.getAttribute&&t.getAttribute('data-pipe-edit');if(pe){var d=await api('/api/ops/inquiry-pipeline');var row=(d.items||[]).find(function(x){return x.id===pe});document.getElementById('pipeForm').innerHTML=pipeForm(row||{})}var pd=t.getAttribute&&t.getAttribute('data-pipe-del');if(pd&&confirm('削除しますか？')){await api('/api/ops/inquiry-pipeline/'+encodeURIComponent(pd),{method:'DELETE'});loadPipe()}});
 document.addEventListener('DOMContentLoaded',buttons);setTimeout(buttons,1000);
})();
</script>`;
  return html.replace("</head>",style+"</head>").replace("</body>",script+"</body>");
}

export default {
  async fetch(request,env,ctx){
    const url=new URL(request.url); const p=url.pathname;
    if((p==="/"||p==="/health"||p==="/api/health")&&request.method==="GET") return json({ok:true,service:"customer-crm-api",build:BUILD,time:new Date().toISOString()});
    if(p==="/api/ops/line-templates"&&request.method==="GET") return templatesApi(request,env);
    if(p==="/api/ops/line-templates"&&request.method==="POST") return saveTemplateApi(request,env);
    if(p==="/api/ops/line-templates/seed"&&request.method==="POST") return seedTemplatesApi(request,env);
    if(p==="/api/ops/line-templates/preview"&&request.method==="POST") return previewTemplateApi(request,env,"");
    const tm=p.match(/^\/api\/ops\/line-templates\/([^/]+)$/);
    if(tm&&request.method==="PUT") return saveTemplateApi(request,env,decodeURIComponent(tm[1]));
    if(tm&&request.method==="DELETE") return deleteTemplateApi(request,env,decodeURIComponent(tm[1]));
    if(tm&&request.method==="POST") return previewTemplateApi(request,env,decodeURIComponent(tm[1]));
    if(p==="/api/ops/inquiry-pipeline"&&request.method==="GET") return pipelineApi(request,env);
    if(p==="/api/ops/inquiry-pipeline"&&request.method==="POST") return savePipelineApi(request,env);
    const pm=p.match(/^\/api\/ops\/inquiry-pipeline\/([^/]+)$/);
    if(pm&&request.method==="PUT") return savePipelineApi(request,env,decodeURIComponent(pm[1]));
    if(pm&&request.method==="DELETE") return deletePipelineApi(request,env,decodeURIComponent(pm[1]));
    const res=await app.fetch(request,env,ctx);
    const type=res.headers.get("content-type")||"";
    if(type.includes("text/html")){ const body=injectOpsScreens(await res.text()); return new Response(body,{status:res.status,statusText:res.statusText,headers:secure(res.headers)}); }
    return new Response(res.body,{status:res.status,statusText:res.statusText,headers:secure(res.headers)});
  }
};
