const BUILD = "customer-identity-resolver-20260818-01";

function text(v){ return v == null ? "" : String(v).trim(); }
function nowIso(){ return new Date().toISOString(); }
function json(data,status=200){ return new Response(JSON.stringify(data,null,2),{status,headers:{"content-type":"application/json; charset=utf-8","cache-control":"no-store"}}); }
function bearer(request){ const a=text(request.headers.get("authorization")); return /^Bearer\s+/i.test(a)?a.replace(/^Bearer\s+/i,"").trim():""; }
function internalToken(request){ return text(request.headers.get("x-internal-token")) || bearer(request); }
function isLineUserId(v){ return /^U[0-9a-fA-F]{20,}$/.test(text(v)); }

export function formatCanonicalCustomerId(id){
  const n=Number(id);
  if(!Number.isSafeInteger(n)||n<1) throw new Error("invalid_registry_id");
  return "C"+String(n).padStart(8,"0");
}

function failure(error,status=409,extra={}){ return {ok:false,statusCode:status,error,review_required:status===409,...extra}; }

async function all(db,sql,...params){
  let s=db.prepare(sql); if(params.length)s=s.bind(...params); const r=await s.all(); return r.results||[];
}
async function first(db,sql,...params){
  let s=db.prepare(sql); if(params.length)s=s.bind(...params); return await s.first();
}
async function run(db,sql,...params){
  let s=db.prepare(sql); if(params.length)s=s.bind(...params); return await s.run();
}

async function existingByLine(db,lineUserId){
  return all(db,`SELECT customer_id,line_user_id,name,acquisition_source,created_at,updated_at FROM customers WHERE line_user_id=? LIMIT 3`,lineUserId);
}
async function registryByLine(db,lineUserId){
  return first(db,`SELECT * FROM customer_identity_registry WHERE line_user_id=? LIMIT 1`,lineUserId);
}
async function registryByIdempotency(db,key){
  return first(db,`SELECT * FROM customer_identity_registry WHERE idempotency_key=? LIMIT 1`,key);
}
async function customerById(db,customerId){
  return first(db,`SELECT customer_id,line_user_id,name FROM customers WHERE customer_id=? LIMIT 1`,customerId);
}

async function reconcileExistingRegistry(db,existing,reg,lineUserId){
  if(!reg) return null;
  const existingId=text(existing.customer_id), regId=text(reg.customer_id);
  if(regId && regId!==existingId) return failure("identity_registry_mismatch",409,{customer_id:existingId});
  if(!regId){
    try{
      await run(db,`UPDATE customer_identity_registry SET customer_id=?,status='active',updated_at=CURRENT_TIMESTAMP WHERE id=? AND line_user_id=? AND customer_id IS NULL`,existingId,reg.id,lineUserId);
    }catch(_){
      const refreshed=await registryByLine(db,lineUserId);
      if(!refreshed||text(refreshed.customer_id)!==existingId) return failure("identity_registry_mismatch",409,{customer_id:existingId});
    }
  }else if(text(reg.status)!=="active"){
    await run(db,`UPDATE customer_identity_registry SET status='active',updated_at=CURRENT_TIMESTAMP WHERE id=? AND line_user_id=?`,reg.id,lineUserId);
  }
  return null;
}

async function resumeRegistry(db,reg,input){
  const lineUserId=input.line_user_id;
  let customerId=text(reg.customer_id);
  if(!customerId){
    customerId=formatCanonicalCustomerId(reg.id);
    try{
      await run(db,`UPDATE customer_identity_registry SET customer_id=?,updated_at=CURRENT_TIMESTAMP WHERE id=? AND line_user_id=? AND customer_id IS NULL`,customerId,reg.id,lineUserId);
    }catch(_){
      const refreshed=await registryByLine(db,lineUserId);
      if(!refreshed) return failure("identity_registry_missing_after_allocation",500,{review_required:true});
      if(text(refreshed.customer_id)!==customerId) return failure("identity_customer_id_collision",409,{customer_id:customerId});
      reg=refreshed;
    }
  }

  const byId=await customerById(db,customerId);
  if(byId){
    if(text(byId.line_user_id)!==lineUserId) {
      await run(db,`UPDATE customer_identity_registry SET status='conflict',updated_at=CURRENT_TIMESTAMP WHERE id=?`,reg.id).catch?.(()=>{});
      return failure("identity_customer_id_collision",409,{customer_id:customerId});
    }
    await run(db,`UPDATE customer_identity_registry SET status='active',updated_at=CURRENT_TIMESTAMP WHERE id=? AND line_user_id=?`,reg.id,lineUserId);
    return {ok:true,status:"resolved_existing",customer_id:customerId,created:false,replayed:true,canonical:true};
  }

  const name=text(input.customer_name)||"名称未設定";
  try{
    await run(db,`INSERT INTO customers (customer_id,line_user_id,name,acquisition_source,created_at,updated_at) VALUES (?,?,?,?,?,?)`,customerId,lineUserId,name,input.source,nowIso(),nowIso());
  }catch(error){
    const after=await customerById(db,customerId);
    if(after && text(after.line_user_id)===lineUserId){
      await run(db,`UPDATE customer_identity_registry SET status='active',updated_at=CURRENT_TIMESTAMP WHERE id=? AND line_user_id=?`,reg.id,lineUserId);
      return {ok:true,status:"resolved_existing",customer_id:customerId,created:false,replayed:true,canonical:true};
    }
    return failure("customer_create_failed",500,{review_required:true,customer_id:customerId});
  }

  await run(db,`UPDATE customer_identity_registry SET status='active',updated_at=CURRENT_TIMESTAMP WHERE id=? AND line_user_id=? AND customer_id=?`,reg.id,lineUserId,customerId);
  return {ok:true,status:"created",customer_id:customerId,created:true,canonical:true};
}

export async function resolveOrCreateCustomerIdentity(env,input={}){
  if(!env||!env.DB) return failure("db_binding_missing",500,{review_required:true});
  const lineUserId=text(input.line_user_id), idempotencyKey=text(input.idempotency_key), source=text(input.source), customerName=text(input.customer_name);
  if(!isLineUserId(lineUserId)) return failure("invalid_line_user_id",400,{review_required:false});
  if(!idempotencyKey) return failure("idempotency_key_required",400,{review_required:false});
  if(!source) return failure("source_required",400,{review_required:false});

  const db=env.DB;
  try{
    const existing=await existingByLine(db,lineUserId);
    if(existing.length>1) return failure("duplicate_existing_line_identity",409,{customer_count:existing.length});

    const idem=await registryByIdempotency(db,idempotencyKey);
    if(idem && text(idem.line_user_id)!==lineUserId) return failure("identity_idempotency_conflict",409);
    const lineReg=await registryByLine(db,lineUserId);

    if(existing.length===1){
      if(lineReg && idem && Number(lineReg.id)!==Number(idem.id)) return failure("identity_registry_mismatch",409);
      const mismatch=await reconcileExistingRegistry(db,existing[0],lineReg,lineUserId);
      if(mismatch) return mismatch;
      return {ok:true,status:"resolved_existing",customer_id:text(existing[0].customer_id),created:false,canonical:true,replayed:!!idem};
    }

    if(lineReg){
      if(idem && Number(idem.id)!==Number(lineReg.id)) return failure("identity_registry_mismatch",409);
      return resumeRegistry(db,lineReg,{line_user_id:lineUserId,idempotency_key:idempotencyKey,source,customer_name:customerName});
    }

    try{
      await run(db,`INSERT INTO customer_identity_registry (customer_id,line_user_id,idempotency_key,source,status,created_at,updated_at,raw_json) VALUES (NULL,?,?,?,'allocating',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?)`,lineUserId,idempotencyKey,source,JSON.stringify({source,customer_name:customerName||null,received_at:nowIso()}));
    }catch(_){
      const idemAfter=await registryByIdempotency(db,idempotencyKey);
      if(idemAfter && text(idemAfter.line_user_id)!==lineUserId) return failure("identity_idempotency_conflict",409);
      const lineAfter=await registryByLine(db,lineUserId);
      if(!lineAfter) return failure("identity_allocation_failed",500,{review_required:true});
      return resumeRegistry(db,lineAfter,{line_user_id:lineUserId,idempotency_key:idempotencyKey,source,customer_name:customerName});
    }

    const allocated=await registryByLine(db,lineUserId);
    if(!allocated) return failure("identity_allocation_failed",500,{review_required:true});
    return resumeRegistry(db,allocated,{line_user_id:lineUserId,idempotency_key:idempotencyKey,source,customer_name:customerName});
  }catch(error){
    return failure("identity_internal_failure",500,{review_required:true});
  }
}

export async function handleCustomerIdentityResolver(request,env){
  const u=new URL(request.url);
  if(u.pathname!=="/api/internal/customer-identity/resolve-or-create") return null;
  if(request.method!=="POST") return json({ok:false,error:"method_not_allowed"},405);
  const expected=text(env&&env.CRM_INTERNAL_TOKEN);
  if(!expected) return json({ok:false,error:"identity_auth_not_configured"},503);
  if(internalToken(request)!==expected) return json({ok:false,error:"unauthorized"},401);
  let body={}; try{body=await request.json();}catch(_){return json({ok:false,error:"invalid_json"},400);}
  const out=await resolveOrCreateCustomerIdentity(env,body);
  const status=out.statusCode||200; delete out.statusCode;
  return json(out,status);
}

export function customerIdentityHealth(env){
  return {
    customer_identity_resolver_enabled:true,
    customer_identity_owner:"customer-crm",
    customer_identity_registry_table:"customer_identity_registry",
    customer_identity_format:"C+global-sequence",
    customer_identity_internal_auth_configured:!!text(env&&env.CRM_INTERNAL_TOKEN),
    customer_identity_name_matching:false,
    customer_identity_reservation_generator:false,
    customer_identity_build:BUILD
  };
}
