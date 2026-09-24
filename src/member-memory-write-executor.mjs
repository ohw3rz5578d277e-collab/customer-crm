import { buildMemberMemoryPlanForFamily } from './crm-member-memory-plan.mjs';

const BUILD='member-memory-write-executor-20260924-01';
const WRITE_MODE='enabled';
const MAX_BATCH=200;
const encoder=new TextEncoder();

const text=v=>v==null?'':String(v).trim();

function writeEnabled(env){
  return text(env?.MEMBER_MEMORY_WRITE_MODE).toLowerCase()===WRITE_MODE;
}

function hex(bytes){
  let out='';
  for(const b of bytes)out+=b.toString(16).padStart(2,'0');
  return out;
}

async function deterministicMemoryId(row){
  const sourceSystem=text(row?.source_system);
  const reservationId=text(row?.source_reservation_id);
  const digest=new Uint8Array(
    await crypto.subtle.digest(
      'SHA-256',
      encoder.encode(`${sourceSystem}:reservation:${reservationId}`)
    )
  );
  return `mem_${hex(digest).slice(0,32)}`;
}

function safeHttpsUrl(value){
  try{
    const u=new URL(text(value));
    return u.protocol==='https:'?u.toString():'';
  }catch{
    return '';
  }
}

function normalizeCreateRow(row){
  return {
    family_id:text(row?.family_id),
    source_system:text(row?.source_system)||'customer-crm',
    source_customer_id:text(row?.source_customer_id),
    source_reservation_id:text(row?.source_reservation_id),
    shoot_date:text(row?.shoot_date),
    genre:text(row?.genre),
    title:text(row?.title)||text(row?.genre)||'MEMORY',
    delivery_link_id:text(row?.delivery_link_id),
    amazon_photos_url:safeHttpsUrl(row?.amazon_photos_url),
    published:row?.published===1?1:0
  };
}

function validCreateRow(row){
  return !!(
    row.family_id
    && row.source_system
    && /^\d{8}$/.test(row.source_customer_id)
    && row.source_reservation_id
    && row.source_reservation_id.length<=160
    && (row.published===0||row.published===1)
  );
}

function fail(status,extra={}){
  return {
    status,
    write_executed:false,
    created_count:0,
    ...extra
  };
}

export async function executeMemberHistoricalMemorySync(env,{
  family_id,
  customer_id,
  approved=false
}={}){
  if(approved!==true){
    return fail('write_approval_required');
  }
  if(!writeEnabled(env)){
    return fail('member_memory_write_disabled');
  }
  if(typeof env?.DB?.batch!=='function'){
    return fail('atomic_batch_required');
  }

  // Rebuild the exact current plan immediately before any write.
  const before=await buildMemberMemoryPlanForFamily(env,{family_id,customer_id});
  if(before.status!=='ok'){
    return fail(before.status,{
      review_required:before.status==='memory_sync_conflict'||before.status==='ambiguous_family_identity',
      plan:before.plan||null
    });
  }
  if(before.member_memory_schema_applied!==true){
    return fail('member_memory_schema_not_applied');
  }
  if(before.plan?.ok!==true||before.plan.conflicts?.length){
    return fail('memory_sync_conflict',{
      review_required:true,
      plan:before.plan||null
    });
  }

  const createRows=(before.plan.to_create||[]).map(normalizeCreateRow);
  if(createRows.length===0){
    return {
      status:'ok',
      write_executed:false,
      created_count:0,
      already_synced_count:(before.plan.already_synced||[]).length,
      skipped_count:(before.plan.skipped||[]).length,
      idempotent_noop:true
    };
  }
  if(createRows.length>MAX_BATCH){
    return fail('memory_write_batch_too_large',{requested_count:createRows.length});
  }
  if(createRows.some(row=>!validCreateRow(row))){
    return fail('invalid_memory_write_plan',{review_required:true});
  }

  const statements=[];
  const memoryIds=[];
  for(const row of createRows){
    const memoryId=await deterministicMemoryId(row);
    memoryIds.push(memoryId);
    statements.push(
      env.DB.prepare(
        `INSERT INTO member_memories (
          memory_id,
          family_id,
          source_system,
          source_customer_id,
          source_reservation_id,
          shoot_date,
          genre,
          title,
          delivery_link_id,
          amazon_photos_url,
          published
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)`
      ).bind(
        memoryId,
        row.family_id,
        row.source_system,
        row.source_customer_id,
        row.source_reservation_id,
        row.shoot_date||null,
        row.genre||null,
        row.title,
        row.delivery_link_id||null,
        row.amazon_photos_url||null,
        row.published
      )
    );
  }

  try{
    await env.DB.batch(statements);
  }catch(error){
    // Do not retry blindly. The unique source key and a fresh follow-up plan determine
    // whether another writer won the race or whether review is required.
    const afterFailure=await buildMemberMemoryPlanForFamily(env,{family_id,customer_id});
    if(
      afterFailure.status==='ok'
      && afterFailure.member_memory_schema_applied===true
      && afterFailure.plan?.ok===true
      && (afterFailure.plan.to_create||[]).length===0
      && (afterFailure.plan.conflicts||[]).length===0
    ){
      return {
        status:'ok',
        write_executed:false,
        created_count:0,
        already_synced_count:(afterFailure.plan.already_synced||[]).length,
        skipped_count:(afterFailure.plan.skipped||[]).length,
        idempotent_noop:true,
        concurrent_writer_won:true
      };
    }
    return fail('member_memory_write_failed',{
      review_required:true,
      error_class:text(error?.name)||'Error'
    });
  }

  // Verify canonical idempotency/family boundaries after the atomic batch.
  const after=await buildMemberMemoryPlanForFamily(env,{family_id,customer_id});
  if(
    after.status!=='ok'
    || after.member_memory_schema_applied!==true
    || after.plan?.ok!==true
    || (after.plan.to_create||[]).length!==0
    || (after.plan.conflicts||[]).length!==0
  ){
    return {
      status:'member_memory_postwrite_verification_failed',
      write_executed:true,
      created_count:createRows.length,
      created_memory_ids:memoryIds,
      review_required:true
    };
  }

  return {
    status:'ok',
    write_executed:true,
    created_count:createRows.length,
    created_memory_ids:memoryIds,
    already_synced_count:(after.plan.already_synced||[]).length,
    skipped_count:(after.plan.skipped||[]).length,
    idempotent_noop:false
  };
}

export function memberMemoryWriteExecutorHealth(env){
  return {
    member_memory_write_executor:true,
    build:BUILD,
    write_mode:writeEnabled(env)?'enabled':'disabled',
    write_default_disabled:true,
    explicit_approved_flag_required:true,
    atomic_batch_required:true,
    prewrite_plan_rebuild:true,
    postwrite_plan_verification:true,
    deterministic_memory_id:true,
    source_reservation_id_idempotency:true,
    exact_customer_id_only:true,
    explicit_family_link_only:true,
    canonical_crm_write:false,
    member_memory_media_write:false,
    line_send:false,
    production_route_wired:false,
    production_write_enabled:false
  };
}

export const __test={
  writeEnabled,
  deterministicMemoryId,
  safeHttpsUrl,
  normalizeCreateRow,
  validCreateRow,
  MAX_BATCH
};
