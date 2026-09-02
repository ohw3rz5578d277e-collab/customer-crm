const CUSTOMER_ID_RE=/^\d{8}$/;
const text=v=>v==null?'':String(v).trim();
function json(data,status=200){return new Response(JSON.stringify(data),{status,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store','x-robots-tag':'noindex, nofollow'}})}
async function first(env,sql,params=[]){let q=env.DB.prepare(sql);if(params.length)q=q.bind(...params);return (await q.first())||null}

export async function guardCustomer360ProfileWrite(request,env){
  if(request.method!=='PATCH')return null;
  const match=new URL(request.url).pathname.match(/^\/api\/customer360\/profile\/([0-9]{8})$/);if(!match)return null;
  const cloned=request.clone();const body=await cloned.json().catch(()=>null);if(!body||typeof body!=='object'||Array.isArray(body))return null;
  const changes=body.changes&&typeof body.changes==='object'?body.changes:body;
  if(!Object.prototype.hasOwnProperty.call(changes,'referrer_customer_id'))return null;
  const ref=text(changes.referrer_customer_id);if(!ref)return null;
  if(!CUSTOMER_ID_RE.test(ref))return json({ok:false,error:'invalid_referrer_customer_id'},400);
  if(ref===match[1])return json({ok:false,error:'referrer_customer_id_self_reference'},409);
  const row=await first(env,"SELECT customer_id FROM customers WHERE CAST(customer_id AS TEXT)=? AND COALESCE(deleted_at,'')='' LIMIT 1",[ref]);
  if(!row||text(row.customer_id)!==ref)return json({ok:false,error:'referrer_customer_not_found'},409);
  return null;
}

export function customer360ProfileWriteGuardHealth(){return{referrer_customer_id_exact_existing_customer_only:true,referrer_name_identity_fallback:false,self_referral_denied:true};}
