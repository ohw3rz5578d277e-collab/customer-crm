const BUILD = "customer-identity-resolver-20260823-existing-missing-id-01";
const SEQUENCE_KEY = "canonical_customer_id";
const MAX_SEQUENCE = 999999;
const MAX_COLLISION_RETRIES = 32;

function text(v){ return v == null ? "" : String(v).trim(); }
function nowIso(){ return new Date().toISOString(); }
function json(data,status=200){ return new Response(JSON.stringify(data,null,2),{status,headers:{"content-type":"application/json; charset=utf-8","cache-control":"no-store"}}); }
function bearer(request){ const a=text(request.headers.get("authorization")); return /^Bearer\s+/i.test(a)?a.replace(/^Bearer\s+/i,"").trim():""; }
function internalToken(request){ return text(request.headers.get("x-internal-token")) || bearer(request); }
function failure(error,status=409,extra={}){ return {ok:false,statusCode:status,error,review_required:status===409,...extra}; }
function changedRows(result){ return Number(result?.meta?.changes ?? result?.changes ?? result?.rowsAffected ?? 0); }

export function isFormalLineUserId(v){ return /^U[0-9a-fA-F]{20,}$/.test(text(v)); }
export function isPlaceholderCustomerName(v){ const s=text(v); return !s || s==="名称未設定"; }

export function jstYear(date=new Date()){
  const parts=new Intl.DateTimeFormat("en-US",{timeZone:"Asia/Tokyo",year:"numeric"}).formatToParts(date);
  return Number(parts.find(x=>x.type==="year")?.value);
}
export function formatCanonicalCustomerId(year,sequence){
  const y=Number(year), n=Number(sequence);
  if(!Number.isInteger(y)||y<2000||y>9999) throw new Error("invalid_customer_identity_year");
  if(!Number.isInteger(n)||n<1||n>MAX_SEQUENCE) throw new Error("customer_identity_sequence_exhausted");
  return String(y%100).padStart(2,"0")+String(n).padStart(6,"0");
}

async function all(db,sql,...params){ let s=db.prepare(sql); if(params.length)s=s.bind(...params); const r=await s.all(); return r.results||[]; }
async function first(db,sql,...params){ let s=db.prepare(sql); if(params.length)s=s.bind(...params); return await s.first(); }
async function run(db,sql,...params){ let s=db.prepare(sql); if(params.length)s=s.bind(...params); return await s.run(); }
async function existingByLine(db,lineUserId){ return all(db,`SELECT customer_id,line_user_id,name,line_display_name,acquisition_source,created_at,updated_at FROM customers WHERE line_user_id=? LIMIT 3`,lineUserId); }
async function registryByLine(db,lineUserId){ return first(db,`SELECT * FROM customer_identity_registry WHERE line_user_id=? LIMIT 1`,lineUserId); }
async function registryByIdempotency(db,key){ return first(db,`SELECT * FROM customer_identity_registry WHERE idempotency_key=? LIMIT 1`,key); }
async function registryByCustomerId(db,customerId){ return first(db,`SELECT * FROM customer_identity_registry WHERE customer_id=? LIMIT 1`,customerId); }
async function customerById(db,customerId){ return first(db,`SELECT customer_id,line_user_id,name,line_display_name FROM customers WHERE customer_id=? LIMIT 1`,customerId); }
async function globalSequenceValueInUse(db,sequence){
  const suffix=String(Number(sequence)).padStart(6,"0");
  const pattern="[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]";
  const customer=await first(db,`SELECT customer_id FROM customers WHERE length(customer_id)=8 AND customer_id GLOB ? AND substr(customer_id,3,6)=? LIMIT 1`,pattern,suffix);
  if(customer) return true;
  const registry=await first(db,`SELECT customer_id FROM customer_identity_registry WHERE customer_id IS NOT NULL AND length(customer_id)=8 AND customer_id GLOB ? AND substr(customer_id,3,6)=? LIMIT 1`,pattern,suffix);
  return !!registry;
}

function effectiveName(existingName,customerName,lineDisplayName){
  const existing=text(existingName), incoming=text(customerName), display=text(lineDisplayName);
  if(!isPlaceholderCustomerName(existing)) return existing;
  if(!isPlaceholderCustomerName(incoming)) return incoming;
  if(display) return display;
  return existing || "名称未設定";
}

async function applyCustomerMetadata(db,customer,input){
  if(!customer) return customer;
  const customerId=text(customer.customer_id);
  const currentName=text(customer.name);
  const currentDisplay=text(customer.line_display_name);
  const incomingName=text(input.customer_name);
  const incomingDisplay=text(input.line_display_name);
  const nextName=effectiveName(currentName,incomingName,incomingDisplay);
  const nextDisplay=incomingDisplay || currentDisplay;
  if(nextName===currentName && nextDisplay===currentDisplay) return customer;
  await run(db,`UPDATE customers SET name=?,line_display_name=?,updated_at=? WHERE customer_id=? AND line_user_id=?`,nextName||null,nextDisplay||null,nowIso(),customerId,input.line_user_id);
  return {...customer,name:nextName,line_display_name:nextDisplay};
}

async function allocateSequence(db){
  const row=await first(db,`UPDATE customer_identity_sequence SET last_value=last_value+1,updated_at=CURRENT_TIMESTAMP WHERE sequence_key=? AND last_value<? RETURNING last_value`,SEQUENCE_KEY,MAX_SEQUENCE);
  if(!row) return failure("customer_identity_sequence_exhausted",409);
  const value=Number(row.last_value);
  if(!Number.isInteger(value)||value<1||value>MAX_SEQUENCE) return failure("customer_identity_sequence_exhausted",409);
  return {ok:true,value};
}

async function allocateCustomerId(db,year){
  for(let attempt=0;attempt<MAX_COLLISION_RETRIES;attempt++){
    const allocated=await allocateSequence(db);
    if(!allocated.ok) return allocated;
    const candidate=formatCanonicalCustomerId(year,allocated.value);
    if(await globalSequenceValueInUse(db,allocated.value)) continue;
    if(!(await customerById(db,candidate)) && !(await registryByCustomerId(db,candidate))) return {ok:true,customer_id:candidate,sequence:allocated.value};
  }
  return failure("identity_customer_id_collision_limit",409,{global_sequence:true});
}

function rawRegistryInput(input){
  return JSON.stringify({
    source:text(input.source),
    customer_name:text(input.customer_name)||null,
    line_display_name:text(input.line_display_name)||null,
    webhook_event_id:text(input.webhook_event_id)||null,
    followed_at:text(input.followed_at)||null,
    received_at:nowIso()
  });
}

async function createMissingRegistry(db,existing,input){
  const customerId=text(existing.customer_id);
  if(!customerId) return failure("existing_customer_id_missing",409);
  const byId=await registryByCustomerId(db,customerId);
  if(byId && text(byId.line_user_id)!==input.line_user_id) return failure("identity_registry_mismatch",409,{customer_id:customerId});
  try{
    await run(db,`INSERT INTO customer_identity_registry (customer_id,line_user_id,idempotency_key,source,status,created_at,updated_at,raw_json) VALUES (?,?,?,?,'active',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?)`,customerId,input.line_user_id,input.idempotency_key,input.source,rawRegistryInput(input));
  }catch(_){
    const lineAfter=await registryByLine(db,input.line_user_id);
    if(!lineAfter || text(lineAfter.customer_id)!==customerId) return failure("identity_registry_mismatch",409,{customer_id:customerId});
  }
  return null;
}

async function reconcileExistingRegistry(db,existing,reg,input){
  if(!reg) return createMissingRegistry(db,existing,input);
  const existingId=text(existing.customer_id), regId=text(reg.customer_id);
  if(!existingId) return failure("existing_customer_id_missing",409);
  if(regId && regId!==existingId) return failure("identity_registry_mismatch",409,{customer_id:existingId});
  if(!regId){
    try{ await run(db,`UPDATE customer_identity_registry SET customer_id=?,status='active',updated_at=CURRENT_TIMESTAMP WHERE id=? AND line_user_id=? AND customer_id IS NULL`,existingId,reg.id,input.line_user_id); }
    catch(_){ const refreshed=await registryByLine(db,input.line_user_id); if(!refreshed||text(refreshed.customer_id)!==existingId) return failure("identity_registry_mismatch",409,{customer_id:existingId}); }
  }else if(text(reg.status)!=="active") await run(db,`UPDATE customer_identity_registry SET status='active',updated_at=CURRENT_TIMESTAMP WHERE id=? AND line_user_id=?`,reg.id,input.line_user_id);
  return null;
}

async function resolveExistingMissingCustomerId(db,existing,reg,input,year){
  const lineUserId=input.line_user_id;
  const regId=text(reg&&reg.customer_id);
  if(regId) return failure("identity_registry_mismatch",409,{customer_id:regId});

  const allocation=await allocateCustomerId(db,year);
  if(!allocation.ok) return allocation;
  let customerId=allocation.customer_id;
  let updatedExisting={...existing,customer_id:customerId};

  const assigned=await run(db,`UPDATE customers SET customer_id=?,updated_at=? WHERE line_user_id=? AND (customer_id IS NULL OR trim(customer_id)='')`,customerId,nowIso(),lineUserId);
  if(changedRows(assigned)!==1){
    const refreshed=await existingByLine(db,lineUserId);
    if(refreshed.length>1) return failure("duplicate_existing_line_identity",409,{customer_count:refreshed.length});
    const winner=text(refreshed[0]&&refreshed[0].customer_id);
    if(!winner) return failure("existing_customer_id_assignment_failed",409);
    customerId=winner;
    updatedExisting=refreshed[0];
  }

  const refreshedReg=await registryByLine(db,lineUserId);
  const mismatch=await reconcileExistingRegistry(db,updatedExisting,refreshedReg,input);
  if(mismatch) return mismatch;
  const finalCustomer=await applyCustomerMetadata(db,updatedExisting,input);
  return {ok:true,status:"resolved_existing",customer_id:text(finalCustomer.customer_id),created:false,canonical:true,replayed:false};
}

async function resumeRegistry(db,reg,input,year){
  const lineUserId=input.line_user_id;
  let customerId=text(reg.customer_id);
  if(!customerId){
    const allocation=await allocateCustomerId(db,year);
    if(!allocation.ok) return allocation;
    customerId=allocation.customer_id;
    try{
      await run(db,`UPDATE customer_identity_registry SET customer_id=?,updated_at=CURRENT_TIMESTAMP WHERE id=? AND line_user_id=? AND customer_id IS NULL`,customerId,reg.id,lineUserId);
      const refreshed=await registryByLine(db,lineUserId);
      if(!refreshed) return failure("identity_registry_missing_after_allocation",500,{review_required:true});
      if(text(refreshed.customer_id)!==customerId){ customerId=text(refreshed.customer_id); reg=refreshed; }
    }catch(_){
      const refreshed=await registryByLine(db,lineUserId);
      if(!refreshed||!text(refreshed.customer_id)) return failure("identity_registry_missing_after_allocation",500,{review_required:true});
      customerId=text(refreshed.customer_id); reg=refreshed;
    }
  }

  const byId=await customerById(db,customerId);
  if(byId){
    if(text(byId.line_user_id)!==lineUserId) return failure("identity_customer_id_collision",409,{customer_id:customerId});
    await applyCustomerMetadata(db,byId,input);
    await run(db,`UPDATE customer_identity_registry SET status='active',updated_at=CURRENT_TIMESTAMP WHERE id=? AND line_user_id=?`,reg.id,lineUserId);
    return {ok:true,status:"resolved_existing",customer_id:customerId,created:false,replayed:true,canonical:true};
  }

  const lineDisplayName=text(input.line_display_name);
  const name=effectiveName("",input.customer_name,lineDisplayName);
  try{ await run(db,`INSERT INTO customers (customer_id,line_user_id,name,line_display_name,acquisition_source,created_at,updated_at) VALUES (?,?,?,?,?,?,?)`,customerId,lineUserId,name,lineDisplayName||null,input.source,nowIso(),nowIso()); }
  catch(_){
    const after=await customerById(db,customerId);
    if(after && text(after.line_user_id)===lineUserId){
      await applyCustomerMetadata(db,after,input);
      await run(db,`UPDATE customer_identity_registry SET status='active',updated_at=CURRENT_TIMESTAMP WHERE id=? AND line_user_id=?`,reg.id,lineUserId);
      return {ok:true,status:"resolved_existing",customer_id:customerId,created:false,replayed:true,canonical:true};
    }
    return failure("customer_create_failed",500,{review_required:true,customer_id:customerId});
  }
  await run(db,`UPDATE customer_identity_registry SET status='active',updated_at=CURRENT_TIMESTAMP WHERE id=? AND line_user_id=? AND customer_id=?`,reg.id,lineUserId,customerId);
  return {ok:true,status:"created",customer_id:customerId,created:true,canonical:true};
}

export async function resolveOrCreateCustomerIdentity(env,input={},options={}){
  if(!env||!env.DB) return failure("db_binding_missing",500,{review_required:true});
  const lineUserId=text(input.line_user_id), idempotencyKey=text(input.idempotency_key), source=text(input.source), customerName=text(input.customer_name), lineDisplayName=text(input.line_display_name);
  if(!isFormalLineUserId(lineUserId)) return failure("invalid_line_user_id",400,{review_required:false});
  if(!idempotencyKey) return failure("idempotency_key_required",400,{review_required:false});
  if(!source) return failure("source_required",400,{review_required:false});
  const year=options.year==null?jstYear():Number(options.year);
  if(!Number.isInteger(year)) return failure("invalid_customer_identity_year",500,{review_required:true});

  const normalizedInput={...input,line_user_id:lineUserId,idempotency_key:idempotencyKey,source,customer_name:customerName,line_display_name:lineDisplayName};
  const db=env.DB;
  try{
    const existing=await existingByLine(db,lineUserId);
    if(existing.length>1) return failure("duplicate_existing_line_identity",409,{customer_count:existing.length});
    const idem=await registryByIdempotency(db,idempotencyKey);
    if(idem && text(idem.line_user_id)!==lineUserId) return failure("identity_idempotency_conflict",409);
    const lineReg=await registryByLine(db,lineUserId);

    if(existing.length===1){
      if(lineReg && idem && Number(lineReg.id)!==Number(idem.id)) return failure("identity_registry_mismatch",409);
      if(!text(existing[0].customer_id)) return resolveExistingMissingCustomerId(db,existing[0],lineReg,normalizedInput,year);
      const mismatch=await reconcileExistingRegistry(db,existing[0],lineReg,normalizedInput);
      if(mismatch) return mismatch;
      await applyCustomerMetadata(db,existing[0],normalizedInput);
      return {ok:true,status:"resolved_existing",customer_id:text(existing[0].customer_id),created:false,canonical:true,replayed:!!idem};
    }
    if(lineReg){
      if(idem && Number(idem.id)!==Number(lineReg.id)) return failure("identity_registry_mismatch",409);
      return resumeRegistry(db,lineReg,normalizedInput,year);
    }

    try{ await run(db,`INSERT INTO customer_identity_registry (customer_id,line_user_id,idempotency_key,source,status,created_at,updated_at,raw_json) VALUES (NULL,?,?,?,'allocating',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?)`,lineUserId,idempotencyKey,source,rawRegistryInput(normalizedInput)); }
    catch(_){
      const idemAfter=await registryByIdempotency(db,idempotencyKey);
      if(idemAfter && text(idemAfter.line_user_id)!==lineUserId) return failure("identity_idempotency_conflict",409);
      const lineAfter=await registryByLine(db,lineUserId);
      if(!lineAfter) return failure("identity_allocation_failed",500,{review_required:true});
      return resumeRegistry(db,lineAfter,normalizedInput,year);
    }
    const allocated=await registryByLine(db,lineUserId);
    if(!allocated) return failure("identity_allocation_failed",500,{review_required:true});
    return resumeRegistry(db,allocated,normalizedInput,year);
  }catch(_){ return failure("identity_internal_failure",500,{review_required:true}); }
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
    customer_identity_sequence_table:"customer_identity_sequence",
    customer_identity_format:"YY+6digit-global-sequence",
    customer_identity_sequence_max:MAX_SEQUENCE,
    customer_identity_year_timezone:"Asia/Tokyo",
    customer_identity_global_sequence_collision_check:true,
    customer_identity_internal_auth_configured:!!text(env&&env.CRM_INTERNAL_TOKEN),
    customer_identity_name_matching:false,
    customer_identity_reservation_generator:false,
    customer_identity_line_user_id_pattern:"^U[0-9a-fA-F]{20,}$",
    customer_identity_line_display_name_metadata_only:true,
    customer_identity_build:BUILD
  };
}
