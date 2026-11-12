// RES-15: CRM service-owned *synthetic SQL planner*, NOT a production writer.
// Neither imported by the active entrypoint nor registered as an endpoint.
// Output is only tested with disposable in-memory SQLite having fabricated
// res15_* columns; there is no schema migration, real DB access or rollout.
const DEFINITIONS=Object.freeze({
 crm_customer:Object.freeze({table:'customers',key:'customer_id',keyType:'customer',fields:['line_user_id','deleted_at']}),
 crm_registry:Object.freeze({table:'customer_identity_registry',key:'line_user_id',keyType:'line',fields:['customer_id','status']})
});
const plain=v=>v!==null&&typeof v==='object'&&!Array.isArray(v)&&
 (Object.getPrototypeOf(v)===Object.prototype||Object.getPrototypeOf(v)===null);
const safe=v=>typeof v==='string'&&v.length>0&&v.length<=160&&v===v.trim()&&!/[\x00-\x1f\x7f]/.test(v);
const keyOk=(kind,v)=>kind==='integer'?Number.isSafeInteger(v)&&v>0:
 kind==='customer'?typeof v==='string'&&/^\\d{2}(?!000000)\\d{6}$/.test(v):
 kind==='line'?typeof v==='string'&&/^U[0-9a-fA-F]{32}$/.test(v):safe(v);
export const RES15_SYNTHETIC_SERVICE='crm';
export const RES15_SYNTHETIC_ATOMIC_SCOPES=Object.freeze(Object.keys(DEFINITIONS));
const denied=reason=>({ok:false,reason,synthetic_only:true,production_authorized:false,service_wired:false});
export async function planRes15ServiceSyntheticMutation(input={},context={}){
 // This context check restricts the intended test harness, NOT a trust
 // boundary or protection from an arbitrary caller with real DB access.
 if(context?.synthetic_only!==true||context?.environment!=='isolated_sqlite'||context?.remote!==false)
  return denied('synthetic_context_required');
 const def=DEFINITIONS[input?.scope],key=input?.entity_key,expected=input?.expected_revision;
 const id=input?.mutation_id,changes=input?.changes;
 if(!def||!keyOk(def.keyType,key)||!safe(id)||!Number.isSafeInteger(expected)||
  expected<1||expected>=Number.MAX_SAFE_INTEGER)return denied('invalid_request');
 if(!plain(changes))return denied('invalid_changes');
 const props=Object.getOwnPropertyDescriptors(changes),columns=Object.keys(props).sort();
 if(!columns.length||columns.length>def.fields.length||columns.some(k=>
  !def.fields.includes(k)||!Object.prototype.hasOwnProperty.call(props[k],'value')||
  !(props[k].value===null||safe(props[k].value)||
    (k==='intake_id'&&Number.isSafeInteger(props[k].value)&&props[k].value>0))))
   return denied('invalid_material_values');
 const snapshot=columns.map(k=>[k,props[k].value]);
 let hash;
 try{
  const payload=JSON.stringify(['crm',input.scope,key,expected,snapshot]);
  const raw=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(payload));
  hash=Array.from(new Uint8Array(raw),v=>v.toString(16).padStart(2,'0')).join('');
 }catch{return denied('fingerprint_unavailable')}
 const sql='UPDATE '+def.table+' SET '+columns.map(k=>k+'=?').join(',')+
  ',res15_revision=res15_revision+1,res15_last_mutation_id=?,res15_payload_sha256=?'+
  ' WHERE '+def.key+'=? AND res15_revision=? AND '+
  '(res15_last_mutation_id IS NULL OR res15_last_mutation_id<>?) AND '+
  '(SELECT COUNT(*) FROM '+def.table+' WHERE '+def.key+'=?)=1'+
  ' RETURNING res15_revision AS revision,res15_last_mutation_id AS mutation_id,res15_payload_sha256 AS payload_sha256';
 return Object.freeze({ok:true,kind:'synthetic_sql_plan',sql,
  bindings:Object.freeze([...snapshot.map(p=>p[1]),id,hash,key,expected,id,key]),
  digest:hash,expected_next_revision:expected+1,scope:input.scope,
  synthetic_only:true,production_authorized:false,service_wired:false,
  complete_writer_coverage:false,multi_row_atomic:false,review_required:true});
}
