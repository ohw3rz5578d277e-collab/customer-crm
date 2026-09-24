const BUILD='member-favorites-rate-limit-20260925-01';
const MODE='enabled';
const WINDOW_SECONDS=60;
const CUSTOMER_LIMIT=20;
const MEMORY_LIMIT=6;
const CUSTOMER_SCOPE='customer';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const MAX_MEMORY_ID=160;

const text=v=>v==null?'':String(v).trim();

function modeEnabled(env){
  return text(env?.MEMBER_FAVORITES_RATE_LIMIT_MODE).toLowerCase()===MODE;
}

function validSession(session){
  const familyId=text(session?.family_id);
  const customerId=text(session?.customer_id);
  if(!familyId||familyId.length>MAX_FAMILY_ID)return {ok:false,error:'invalid_member_session'};
  if(!CUSTOMER_ID_RE.test(customerId))return {ok:false,error:'invalid_member_session'};
  return {ok:true,family_id:familyId,customer_id:customerId};
}

function validMemoryId(value){
  const raw=value==null?'':String(value);
  if(!raw||raw.length>MAX_MEMORY_ID)return false;
  if(/[\u0000-\u001f\u007f]/.test(raw))return false;
  if(raw!==raw.trim())return false;
  return true;
}

function nowSeconds(value){
  if(Number.isFinite(Number(value)))return Math.floor(Number(value));
  return Math.floor(Date.now()/1000);
}

function bucketStart(now){
  return Math.floor(now/WINDOW_SECONDS)*WINDOW_SECONDS;
}

function retryAfterSeconds(now,windowStart){
  return Math.max(1,(windowStart+WINDOW_SECONDS)-now);
}

async function first(env,sql,params=[]){
  let q=env.DB.prepare(sql);
  if(params.length)q=q.bind(...params);
  return (await q.first())||null;
}

async function run(env,sql,params=[]){
  let q=env.DB.prepare(sql);
  if(params.length)q=q.bind(...params);
  return q.run();
}

async function tableExists(env,name){
  return !!(await first(
    env,
    "SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
    [name]
  ));
}

async function incrementScope(env,{
  family_id,
  customer_id,
  scope_key,
  window_started_at
}){
  const result=await run(
    env,
    `INSERT INTO member_favorite_mutation_rate_limits (
       family_id,
       customer_id,
       scope_key,
       window_started_at,
       attempt_count,
       updated_at
     ) VALUES (?,?,?,?,1,CURRENT_TIMESTAMP)
     ON CONFLICT(family_id,customer_id,scope_key,window_started_at)
     DO UPDATE SET
       attempt_count=attempt_count+1,
       updated_at=CURRENT_TIMESTAMP`,
    [family_id,customer_id,scope_key,window_started_at]
  );

  if(result?.success===false){
    return {ok:false,error:'rate_limit_counter_write_failed'};
  }

  const row=await first(
    env,
    `SELECT attempt_count
       FROM member_favorite_mutation_rate_limits
       WHERE family_id=?
         AND customer_id=?
         AND scope_key=?
         AND window_started_at=?
       LIMIT 1`,
    [family_id,customer_id,scope_key,window_started_at]
  );

  const count=Number(row?.attempt_count);
  if(!Number.isInteger(count)||count<1){
    return {ok:false,error:'rate_limit_counter_verification_failed'};
  }

  return {ok:true,count};
}

export async function consumeMemberFavoriteMutationRateLimit(
  env,
  session,
  {
    memory_id,
    now_seconds
  }={}
){
  if(!modeEnabled(env)){
    return {
      status:'rate_limit_disabled',
      allowed:false,
      counter_write_executed:false
    };
  }

  if(typeof env?.DB?.prepare!=='function'){
    return {
      status:'database_unavailable',
      allowed:false,
      counter_write_executed:false
    };
  }

  const normalized=validSession(session);
  if(!normalized.ok){
    return {
      status:normalized.error,
      allowed:false,
      counter_write_executed:false
    };
  }

  if(!(await tableExists(env,'member_favorite_mutation_rate_limits'))){
    return {
      status:'rate_limit_schema_not_applied',
      allowed:false,
      counter_write_executed:false
    };
  }

  const now=nowSeconds(now_seconds);
  const windowStartedAt=bucketStart(now);

  const customerCounter=await incrementScope(env,{
    family_id:normalized.family_id,
    customer_id:normalized.customer_id,
    scope_key:CUSTOMER_SCOPE,
    window_started_at:windowStartedAt
  });

  if(!customerCounter.ok){
    return {
      status:customerCounter.error,
      allowed:false,
      counter_write_executed:true
    };
  }

  const retryAfter=retryAfterSeconds(now,windowStartedAt);

  if(customerCounter.count>CUSTOMER_LIMIT){
    return {
      status:'rate_limited',
      allowed:false,
      scope:'customer',
      retry_after_seconds:retryAfter,
      counter_write_executed:true
    };
  }

  if(!validMemoryId(memory_id)){
    return {
      status:'invalid_memory_id',
      allowed:false,
      retry_after_seconds:null,
      counter_write_executed:true
    };
  }

  const memoryCounter=await incrementScope(env,{
    family_id:normalized.family_id,
    customer_id:normalized.customer_id,
    scope_key:`memory:${memory_id}`,
    window_started_at:windowStartedAt
  });

  if(!memoryCounter.ok){
    return {
      status:memoryCounter.error,
      allowed:false,
      counter_write_executed:true
    };
  }

  if(memoryCounter.count>MEMORY_LIMIT){
    return {
      status:'rate_limited',
      allowed:false,
      scope:'memory',
      retry_after_seconds:retryAfter,
      counter_write_executed:true
    };
  }

  return {
    status:'ok',
    allowed:true,
    retry_after_seconds:null,
    counter_write_executed:true,
    limits:{
      window_seconds:WINDOW_SECONDS,
      customer_limit:CUSTOMER_LIMIT,
      memory_limit:MEMORY_LIMIT
    }
  };
}

export function memberFavoritesRateLimitHealth(env){
  return {
    member_favorites_rate_limit:true,
    build:BUILD,
    mode:modeEnabled(env)?'enabled':'disabled',
    default_disabled:true,
    storage:'member_favorite_mutation_rate_limits',
    production_schema_applied:false,
    production_route_wired:false,
    fixed_window_seconds:WINDOW_SECONDS,
    customer_attempt_limit:CUSTOMER_LIMIT,
    memory_attempt_limit:MEMORY_LIMIT,
    authenticated_member_scope:true,
    family_customer_scope:true,
    invalid_memory_consumes_customer_quota:true,
    exact_memory_scope_when_valid:true,
    retry_after_supported:true,
    counter_write_required_when_enabled:true,
    canonical_crm_write:false,
    line_send:false,
    favorite_write:false
  };
}

export const __test={
  modeEnabled,
  validSession,
  validMemoryId,
  nowSeconds,
  bucketStart,
  retryAfterSeconds,
  incrementScope,
  WINDOW_SECONDS,
  CUSTOMER_LIMIT,
  MEMORY_LIMIT
};
