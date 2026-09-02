import { allocateCustomerId, isFormalLineUserId, jstYear, resolveOrCreateCustomerIdentity } from './customer-identity-resolver.mjs';

const CUSTOMER_ID_RE=/^\d{8}$/;
const text=v=>v==null?'':String(v).trim();
const changedRows=result=>Number(result?.meta?.changes??result?.changes??result?.rowsAffected??0);
const failure=(error,statusCode=409,extra={})=>({ok:false,error,statusCode,review_required:statusCode===409,...extra});

function parseCustomerRef(value){const m=text(value).match(/^row:(\d+)$/);if(!m)return null;const n=Number(m[1]);return Number.isSafeInteger(n)&&n>0?n:null}
async function first(db,sql,...params){let s=db.prepare(sql);if(params.length)s=s.bind(...params);return await s.first()}
async function run(db,sql,...params){let s=db.prepare(sql);if(params.length)s=s.bind(...params);return await s.run()}
async function customerByRef(db,rowid){return first(db,`SELECT rowid AS customer_ref,customer_id,line_user_id,name,line_display_name,deleted_at FROM customers WHERE rowid=? LIMIT 1`,rowid)}

export function customerRefFromRowid(rowid){const n=Number(rowid);return Number.isSafeInteger(n)&&n>0?`row:${n}`:''}

export async function assignCanonicalCustomerIdToCustomerRef(env,input={},options={}){
  if(!env?.DB)return failure('db_binding_missing',500,{review_required:true});
  const rowid=parseCustomerRef(input.customer_ref);if(!rowid)return failure('invalid_customer_ref',400,{review_required:false});
  const row=await customerByRef(env.DB,rowid);if(!row||text(row.deleted_at))return failure('customer_not_found',404,{review_required:false});
  const existing=text(row.customer_id);
  if(existing){
    if(!CUSTOMER_ID_RE.test(existing))return failure('invalid_existing_customer_id',409,{review_required:true});
    const line=text(row.line_user_id);
    if(line&&isFormalLineUserId(line)){
      const reconciled=await resolveOrCreateCustomerIdentity(env,{line_user_id:line,idempotency_key:`owner-customer-id-autofill:${rowid}`,source:'owner_customer_id_autofill',customer_name:text(row.name),line_display_name:text(row.line_display_name)},options);
      if(!reconciled.ok)return reconciled;
    }else if(line)return failure('invalid_existing_line_identity',409,{review_required:true});
    return {ok:true,customer_id:existing,already_assigned:true,canonical:true,customer_ref:customerRefFromRowid(rowid)};
  }

  const line=text(row.line_user_id);
  if(line){
    if(!isFormalLineUserId(line))return failure('invalid_existing_line_identity',409,{review_required:true});
    const out=await resolveOrCreateCustomerIdentity(env,{line_user_id:line,idempotency_key:`owner-customer-id-autofill:${rowid}`,source:'owner_customer_id_autofill',customer_name:text(row.name),line_display_name:text(row.line_display_name)},options);
    if(!out.ok)return out;
    const after=await customerByRef(env.DB,rowid),winner=text(after?.customer_id);
    if(!CUSTOMER_ID_RE.test(winner)||winner!==text(out.customer_id))return failure('identity_row_target_mismatch',409,{review_required:true});
    return {...out,customer_ref:customerRefFromRowid(rowid),already_assigned:false};
  }

  const year=options.year==null?jstYear():Number(options.year);if(!Number.isInteger(year))return failure('invalid_customer_identity_year',500,{review_required:true});
  const allocation=await allocateCustomerId(env.DB,year);if(!allocation.ok)return allocation;
  const assigned=await run(env.DB,`UPDATE customers SET customer_id=?,updated_at=CURRENT_TIMESTAMP WHERE rowid=? AND (customer_id IS NULL OR trim(customer_id)='')`,allocation.customer_id,rowid);
  if(changedRows(assigned)===1)return {ok:true,customer_id:allocation.customer_id,already_assigned:false,canonical:true,customer_ref:customerRefFromRowid(rowid),sequence:allocation.sequence};
  const after=await customerByRef(env.DB,rowid),winner=text(after?.customer_id);
  if(CUSTOMER_ID_RE.test(winner))return {ok:true,customer_id:winner,already_assigned:true,concurrent_winner:true,canonical:true,customer_ref:customerRefFromRowid(rowid)};
  if(winner)return failure('invalid_existing_customer_id',409,{review_required:true});
  return failure('customer_id_assignment_failed',409,{review_required:true});
}

export const customerIdAutofillContract=Object.freeze({
  owner:'customer-crm',
  format:'YY+6digit-global-sequence',
  sequence_key:'canonical_customer_id',
  row_reference_is_identity:false,
  name_matching:false,
  phone_matching:false,
  family_matching:false,
  birthdate_matching:false,
  line_send:false,
  reservation_write:false,
  customer_write_scope:'missing_customer_id_only'
});
