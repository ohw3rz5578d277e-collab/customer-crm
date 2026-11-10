// RES-15: disabled-by-default service-only CRM identity evidence.
// Route is reachable only after separate Owner authorization + deployment + env setup.
// Never creates/changes customers; never emits Customer ID, LINE ID, names or raw rows.
export const CRM_RES15_ROUTE="/api/internal/res15/identity-read";
export const CRM_RES15_QUERIES=Object.freeze({
 customer_by_id:"SELECT customer_id,line_user_id,COALESCE(deleted_at,'') AS deleted_at FROM customers WHERE customer_id=? LIMIT 3",
 customer_by_line:"SELECT customer_id,line_user_id,COALESCE(deleted_at,'') AS deleted_at FROM customers WHERE line_user_id=? LIMIT 3",
 registry_by_id:"SELECT customer_id,line_user_id,status FROM customer_identity_registry WHERE customer_id=? LIMIT 3",
 registry_by_line:"SELECT customer_id,line_user_id,status FROM customer_identity_registry WHERE line_user_id=? LIMIT 3"
});
const CUSTOMER=/^\d{2}(?!000000)\d{6}$/;
const LINE=/^U[0-9a-fA-F]{32}$/;
function response(data,status=200){
 return new Response(JSON.stringify(data),{status,headers:{"content-type":"application/json; charset=utf-8","cache-control":"no-store","referrer-policy":"no-referrer","x-content-type-options":"nosniff","x-frame-options":"DENY"}});
}
function denied(error,status){return response({ok:false,error,write_allowed:false,auto_replay_allowed:false},status);}
async function tokenMatches(supplied,secret){
 if(typeof supplied!=="string"||typeof secret!=="string"||secret.length<32||
    supplied.length<32||supplied.length>256)return false;
 const enc=new TextEncoder();
 const [a,b]=await Promise.all([crypto.subtle.digest("SHA-256",enc.encode(supplied)),
   crypto.subtle.digest("SHA-256",enc.encode(secret))]);
 const av=new Uint8Array(a),bv=new Uint8Array(b);
 let mismatch=0;for(let i=0;i<av.length;i++)mismatch|=av[i]^bv[i];
 return mismatch===0;
}
async function all(db,sql,param){
 const data=await db.prepare(sql).bind(param).all();
 if(!data||!Array.isArray(data.results)||data.results.length>3)throw Error("invalid_read_result");
 return data.results;
}
export async function crmRes15IdentityRead(request,env){
 const url=new URL(request.url);
 if(url.pathname!==CRM_RES15_ROUTE)return null;
 // This flag is intentionally NOT added to wrangler vars or Secrets by this PR.
 if(env?.RES15_CRM_IDENTITY_READ_ENABLED!=="true")return denied("not_found",404);
 if(request.method!=="POST")return denied("method_not_allowed",405);
 if(url.search||request.headers.has("origin")||request.headers.has("cookie")||
    request.headers.has("sec-fetch-site")||request.headers.has("sec-fetch-mode")||
    request.headers.has("authorization"))return denied("service_request_only",403);
 if(!/^application\/json(?:\s*;|$)/i.test(request.headers.get("content-type")||""))
  return denied("json_required",415);
 if(!await tokenMatches(request.headers.get("x-res15-read-token"),
   env.RES15_CRM_IDENTITY_READ_TOKEN))return denied("unauthorized",401);
 if(!env.DB||typeof env.DB.prepare!=="function")return denied("db_unavailable",503);
 const raw=await request.text();
 if(raw.length>512)return denied("invalid_request",400);
 let input;try{input=JSON.parse(raw)}catch{return denied("invalid_request",400)}
 if(!input||Array.isArray(input)||typeof input!=="object"||
    Object.keys(input).sort().join(",")!=="blocked_line_user_id,candidate_customer_id"||
    typeof input.candidate_customer_id!=="string"||
    typeof input.blocked_line_user_id!=="string"||
    !CUSTOMER.test(input.candidate_customer_id)||!LINE.test(input.blocked_line_user_id))
  return denied("invalid_request",400);
 let results;
 try{
  results={
   customer_by_id:await all(env.DB,CRM_RES15_QUERIES.customer_by_id,input.candidate_customer_id),
   customer_by_line:await all(env.DB,CRM_RES15_QUERIES.customer_by_line,input.blocked_line_user_id),
   registry_by_id:await all(env.DB,CRM_RES15_QUERIES.registry_by_id,input.candidate_customer_id),
   registry_by_line:await all(env.DB,CRM_RES15_QUERIES.registry_by_line,input.blocked_line_user_id)
  };
 }catch(_){return denied("read_unavailable",503)}
 const values=Object.values(results);
 let verdict="matched_snapshot_only";
 if(values.some(rows=>rows.length!==1))verdict="missing_or_ambiguous";
 else{
  const customerRows=[results.customer_by_id[0],results.customer_by_line[0]];
  const registryRows=[results.registry_by_id[0],results.registry_by_line[0]];
  const matches=r=>typeof r.customer_id==="string"&&
    typeof r.line_user_id==="string"&&r.customer_id===input.candidate_customer_id&&
    r.line_user_id===input.blocked_line_user_id;
  if(customerRows.some(r=>!matches(r))||registryRows.some(r=>!matches(r)))
   verdict="identity_disagreement";
  else if(customerRows.some(r=>typeof r.deleted_at!=="string"||r.deleted_at!=="")||
    registryRows.some(r=>r.status!=="active"))verdict="inactive_or_deleted";
 }
 return response({ok:true,verdict,lookups:4,source:"customer-crm",
  source_provenance_attested:false, // independently verified rollout required
  write_allowed:false,customer_id_generated:false,identity_rebound:false,
  auto_replay_allowed:false,owner_review_required:true});
}
