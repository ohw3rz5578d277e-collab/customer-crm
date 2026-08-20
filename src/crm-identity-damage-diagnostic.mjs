// TASK-CRM-LINE-FOLLOW-CANONICAL-CUSTOMER-01
// READ ONLY identity damage diagnostics. No repair, merge, DDL, UPDATE, or DELETE.

import { isFormalLineUserId } from './customer-identity-resolver.mjs';
const BUILD='crm-identity-damage-diagnostic-20260820-02';
const SAMPLE_LIMIT=20;
function text(v){return v==null?'':String(v).trim();}
function json(data,status=200){return new Response(JSON.stringify(data,null,2),{status,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store','x-crm-identity-damage-diagnostic-build':BUILD}});}
function bearer(req){const h=text(req.headers.get('authorization'));return /^Bearer\s+/i.test(h)?h.replace(/^Bearer\s+/i,'').trim():'';}
function internalToken(req){return text(req.headers.get('x-internal-token'))||bearer(req);}
async function all(db,sql,...params){let s=db.prepare(sql);if(params.length)s=s.bind(...params);const r=await s.all();return r.results||[];}
async function first(db,sql,...params){let s=db.prepare(sql);if(params.length)s=s.bind(...params);return await s.first();}
async function tableExists(db,name){return !!(await first(db,"SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",name));}
async function columns(db,table){const rows=await all(db,`PRAGMA table_info(${table})`);return new Set(rows.map(x=>text(x.name)));}

export async function runIdentityDamageDiagnostic(env){
  if(!env||!env.DB)return{ok:false,statusCode:500,error:'db_binding_missing',read_only:true};
  const db=env.DB;
  if(!(await tableExists(db,'customers')))return{ok:false,statusCode:503,error:'customers_table_missing',read_only:true};
  const customerCols=await columns(db,'customers');
  const hasDisplay=customerCols.has('line_display_name');
  const select=`SELECT customer_id,name,${hasDisplay?'line_display_name':'NULL AS line_display_name'},line_user_id FROM customers`;
  const customers=await all(db,select);
  const registryPresent=await tableExists(db,'customer_identity_registry');
  const sequencePresent=await tableExists(db,'customer_identity_sequence');
  const registry=registryPresent?await all(db,'SELECT customer_id,line_user_id,idempotency_key,status FROM customer_identity_registry'):[];
  const formalCustomers=customers.filter(x=>isFormalLineUserId(x.line_user_id));
  const registryByLine=new Map(registry.map(r=>[text(r.line_user_id),r]));
  const duplicateLineGroups=[];
  const byLine=new Map();
  for(const c of formalCustomers){const id=text(c.line_user_id);if(!byLine.has(id))byLine.set(id,[]);byLine.get(id).push(c);}
  for(const [lineId,rows] of byLine)if(rows.length>1)duplicateLineGroups.push({line_user_id:lineId,customer_ids:rows.map(r=>text(r.customer_id)).filter(Boolean)});
  const registryMissing=formalCustomers.filter(c=>!registryByLine.has(text(c.line_user_id)));
  const registryMismatch=formalCustomers.filter(c=>{const r=registryByLine.get(text(c.line_user_id));return !!r&&text(r.customer_id)!==text(c.customer_id);});
  const linksByCustomerId=new Map();
  function addLink(cid,lid){cid=text(cid);lid=text(lid);if(!cid||!isFormalLineUserId(lid))return;if(!linksByCustomerId.has(cid))linksByCustomerId.set(cid,new Set());linksByCustomerId.get(cid).add(lid);}
  customers.forEach(c=>addLink(c.customer_id,c.line_user_id));registry.forEach(r=>addLink(r.customer_id,r.line_user_id));
  const conflictingCustomerGroups=[];for(const [cid,ids] of linksByCustomerId)if(ids.size>1)conflictingCustomerGroups.push({customer_id:cid,line_user_ids:[...ids]});
  const invalidCustomers=customers.filter(c=>text(c.line_user_id)&&!isFormalLineUserId(c.line_user_id));
  let sequence={table_present:sequencePresent,row_present:false,ok:false,last_value:null,max:999999};
  if(sequencePresent){const row=await first(db,'SELECT sequence_key,last_value FROM customer_identity_sequence WHERE sequence_key=? LIMIT 1','canonical_customer_id');const n=Number(row&&row.last_value);sequence={table_present:true,row_present:!!row,ok:!!row&&Number.isInteger(n)&&n>=0&&n<=999999,last_value:row?n:null,max:999999};}
  const categories={
    customer_id_missing:customers.filter(c=>!text(c.customer_id)).length,
    name_null:customers.filter(c=>c.name==null).length,
    name_empty:customers.filter(c=>c.name!=null&&!text(c.name)).length,
    name_placeholder:customers.filter(c=>text(c.name)==='名称未設定').length,
    line_display_name_missing:hasDisplay?customers.filter(c=>!text(c.line_display_name)).length:customers.length,
    line_user_id_missing:customers.filter(c=>!text(c.line_user_id)).length,
    line_user_id_invalid_format:invalidCustomers.length,
    line_user_id_equals_customer_id:customers.filter(c=>text(c.line_user_id)&&text(c.line_user_id)===text(c.customer_id)).length,
    customer_id_reservation_prefix:customers.filter(c=>text(c.customer_id).toLowerCase().startsWith('reservation-')).length,
    registry_missing_for_line_linked_customers:registryMissing.length,
    registry_customer_id_mismatch:registryMismatch.length,
    same_formal_line_id_multiple_customers:duplicateLineGroups.length,
    same_customer_id_conflicting_line_ids:conflictingCustomerGroups.length,
    sequence_corrupt:sequence.ok?0:1
  };
  categories.name_null_or_empty=categories.name_null+categories.name_empty;
  return {
    ok:true,build:BUILD,read_only:true,mutation_executed:false,ddl_executed:false,repair_executed:false,
    customer_count:customers.length,
    customer_columns:{line_display_name:hasDisplay},
    categories,
    registry:{table_present:registryPresent,registry_missing_for_line_linked_customers:categories.registry_missing_for_line_linked_customers,registry_customer_id_mismatch:categories.registry_customer_id_mismatch},
    sequence,
    samples:{
      invalid_line_user_id:invalidCustomers.slice(0,SAMPLE_LIMIT).map(c=>({customer_id:text(c.customer_id)||null,line_user_id:text(c.line_user_id)||null})),
      registry_missing:registryMissing.slice(0,SAMPLE_LIMIT).map(c=>({customer_id:text(c.customer_id)||null,line_user_id:text(c.line_user_id)||null})),
      registry_mismatch:registryMismatch.slice(0,SAMPLE_LIMIT).map(c=>({customer_id:text(c.customer_id)||null,line_user_id:text(c.line_user_id)||null,registry_customer_id:text(registryByLine.get(text(c.line_user_id))?.customer_id)||null})),
      duplicate_line_identity:duplicateLineGroups.slice(0,SAMPLE_LIMIT),
      conflicting_customer_identity:conflictingCustomerGroups.slice(0,SAMPLE_LIMIT)
    }
  };
}

export async function handleIdentityDamageDiagnostic(req,env){
  const u=new URL(req.url);
  if(u.pathname!=='/api/internal/customer-identity/damage-diagnostic')return null;
  if(req.method!=='GET')return json({ok:false,error:'method_not_allowed',read_only:true},405);
  const expected=text(env&&env.CRM_INTERNAL_TOKEN);
  if(!expected)return json({ok:false,error:'identity_diagnostic_auth_not_configured',read_only:true},503);
  if(internalToken(req)!==expected)return json({ok:false,error:'unauthorized',read_only:true},401);
  try{const out=await runIdentityDamageDiagnostic(env);const status=out.statusCode||200;delete out.statusCode;return json(out,status);}catch(e){return json({ok:false,error:'identity_diagnostic_failed',detail:text(e&&e.message||e),build:BUILD,read_only:true},500);}
}
export function identityDamageDiagnosticHealth(){return{identity_damage_diagnostic_enabled:true,identity_damage_diagnostic_endpoint:'/api/internal/customer-identity/damage-diagnostic',identity_damage_diagnostic_read_only:true,identity_damage_diagnostic_repair:false,identity_damage_diagnostic_categories:'A-M',identity_damage_diagnostic_build:BUILD};}
