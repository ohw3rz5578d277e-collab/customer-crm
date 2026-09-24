import { readMemberFamilyByCustomer } from './crm-member-family-identity.mjs';
import { readMemberMemorySourceByCustomer } from './crm-member-memory-source.mjs';
import { buildMemorySyncPlan } from './member-memory-sync-plan.mjs';

const BUILD='crm-member-memory-plan-20260924-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const QUERY_BATCH=50;

const text=v=>v==null?'':String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-crm-member-memory-plan-build':BUILD,
      'x-robots-tag':'noindex, nofollow',
      'referrer-policy':'no-referrer'
    }
  });
}

function bearer(request){
  const h=text(request.headers.get('authorization'));
  return /^Bearer\s+/i.test(h)?h.replace(/^Bearer\s+/i,'').trim():'';
}

function internalAllowed(request,env){
  const expected=text(env?.CRM_INTERNAL_TOKEN);
  const supplied=text(request.headers.get('x-internal-token')||bearer(request));
  return !!expected&&supplied===expected;
}

async function first(env,sql,params=[]){
  let q=env.DB.prepare(sql);
  if(params.length)q=q.bind(...params);
  return (await q.first())||null;
}

async function all(env,sql,params=[]){
  let q=env.DB.prepare(sql);
  if(params.length)q=q.bind(...params);
  const r=await q.all();
  return r.results||[];
}

async function tableExists(env,name){
  return !!(await first(env,"SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",[name]));
}

async function existingMemoriesForSource(env,sourceMemories){
  if(!(await tableExists(env,'member_memories'))){
    return {schema_applied:false,rows:[]};
  }

  const ids=[...new Set((sourceMemories||[]).map(x=>text(x.source_reservation_id)).filter(Boolean))];
  const rows=[];
  for(let i=0;i<ids.length;i+=QUERY_BATCH){
    const batch=ids.slice(i,i+QUERY_BATCH);
    const marks=batch.map(()=>'?').join(',');
    rows.push(...await all(
      env,
      `SELECT memory_id,family_id,source_system,source_customer_id,source_reservation_id,deleted_at
         FROM member_memories
        WHERE source_system='customer-crm'
          AND source_reservation_id IN (${marks})
        LIMIT 200`,
      batch
    ));
  }
  return {schema_applied:true,rows};
}

export async function buildMemberMemoryPlanForFamily(env,{family_id,customer_id}={}){
  const familyId=text(family_id);
  const customerId=text(customer_id);

  if(!familyId||familyId.length>MAX_FAMILY_ID){
    return {status:'invalid_family_id',write_executed:false};
  }
  if(!CUSTOMER_ID_RE.test(customerId)){
    return {status:'invalid_customer_id',write_executed:false};
  }

  const familyResult=await readMemberFamilyByCustomer(env,customerId);
  if(familyResult.status!=='linked'){
    return {status:familyResult.status,write_executed:false};
  }
  if(text(familyResult.family?.family_id)!==familyId){
    return {status:'family_access_denied',write_executed:false};
  }

  const source=await readMemberMemorySourceByCustomer(env,customerId);
  if(source.status!=='ok'){
    return {status:source.status,write_executed:false};
  }

  const existing=await existingMemoriesForSource(env,source.memories);
  const plan=buildMemorySyncPlan({
    family_id:familyId,
    customer_id:customerId,
    source_memories:source.memories,
    existing_memories:existing.rows
  });

  if(!plan.ok){
    return {
      status:'memory_sync_conflict',
      family_id:familyId,
      customer_id:customerId,
      source_diagnostics:source.diagnostics,
      member_memory_schema_applied:existing.schema_applied,
      plan,
      write_executed:false
    };
  }

  return {
    status:'ok',
    family_id:familyId,
    customer_id:customerId,
    source_diagnostics:source.diagnostics,
    member_memory_schema_applied:existing.schema_applied,
    plan,
    write_executed:false
  };
}

export async function handleMemberMemoryPlanRequest(request,env){
  const url=new URL(request.url);
  const m=url.pathname.match(/^\/api\/internal\/member-memory-plan\/family\/([^/]{1,128})\/customer\/(\d{8})$/);
  if(!m)return null;
  if(request.method!=='GET')return json({ok:false,error:'method_not_allowed'},405);
  if(!internalAllowed(request,env))return json({ok:false,error:'authentication_required'},401);

  const familyId=decodeURIComponent(m[1]);
  const result=await buildMemberMemoryPlanForFamily(env,{family_id:familyId,customer_id:m[2]});

  if(result.status==='ok')return json({ok:true,...result});
  if(result.status==='family_access_denied')return json({ok:false,error:'family_access_denied'},403);
  if(result.status==='customer_not_found')return json({ok:false,error:'customer_not_found'},404);
  if(result.status==='memory_sync_conflict'||result.status==='ambiguous_family_identity')return json({ok:false,error:result.status,review_required:true,...result},409);
  if(result.status.endsWith('_schema_missing')||result.status==='schema_not_applied')return json({ok:false,error:result.status},409);
  return json({ok:false,error:result.status},400);
}

export function memberMemoryPlanHealth(){
  return {
    member_memory_plan:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    family_authorization_required:true,
    exact_customer_id_only:true,
    source_reservation_id_idempotency:true,
    cross_family_fail_closed:true,
    existing_memory_global_source_check:true,
    production_write:false
  };
}
