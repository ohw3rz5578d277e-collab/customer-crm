// Read-only exact Customer ID batch name lookup for Reservation.
// No fallback, no fuzzy matching, no DDL, and no writes.

const BUILD='crm-internal-customer-name-batch-20260910-01';
const MAX_IDS=100;
const CUSTOMER_ID_RE=/^\d{8}$/;

function text(v){return String(v==null?'':v).trim()}
function json(data,status=200){return new Response(JSON.stringify(data,null,2),{status,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store, no-cache, must-revalidate, max-age=0','x-crm-internal-customer-name-batch-build':BUILD}})}
function bearer(req){const h=text(req.headers.get('authorization'));return /^Bearer\s+/i.test(h)?h.replace(/^Bearer\s+/i,'').trim():''}
function internalAllowed(req,env){const supplied=text(req.headers.get('x-internal-token')||bearer(req));const expected=text(env&&env.CRM_INTERNAL_TOKEN);return !!expected&&supplied===expected}
function parseIds(url){
  const raw=text(url.searchParams.get('ids'));
  if(!raw)return {ok:false,code:'customer_ids_missing',ids:[]};
  const values=raw.split(',').map(text).filter(Boolean);
  if(values.length>MAX_IDS)return {ok:false,code:'too_many_customer_ids',ids:[]};
  const ids=[];const seen=new Set();
  for(const id of values){
    if(!CUSTOMER_ID_RE.test(id))return {ok:false,code:'invalid_customer_id',ids:[]};
    if(!seen.has(id)){seen.add(id);ids.push(id)}
  }
  if(!ids.length)return {ok:false,code:'customer_ids_missing',ids:[]};
  return {ok:true,code:'',ids};
}
async function selectCustomers(env,ids){
  try{
    const placeholders=ids.map(()=>'?').join(',');
    let stmt=env.DB.prepare(`SELECT customer_id, name, updated_at FROM customers WHERE CAST(customer_id AS TEXT) IN (${placeholders}) AND COALESCE(deleted_at,'')=''`);
    stmt=stmt.bind(...ids);
    const result=await stmt.all();
    return {ok:true,rows:result.results||[]};
  }catch(e){return {ok:false,rows:[],error:text(e&&e.message||e)}}
}

export async function handleInternalCustomerNameBatch(req,env){
  const url=new URL(req.url);
  if(url.pathname!=='/api/internal/customer-name-batch')return null;
  if(req.method!=='GET')return json({ok:false,error:'Method Not Allowed'},405);
  if(!env||!env.DB)return json({ok:false,error:'D1 DB binding missing'},500);
  if(!internalAllowed(req,env))return json({ok:false,error:'CRM_INTERNAL_TOKEN authentication required'},401);

  const parsed=parseIds(url);
  if(!parsed.ok)return json({ok:false,build:BUILD,code:parsed.code,error:parsed.code},400);

  const selected=await selectCustomers(env,parsed.ids);
  if(!selected.ok)return json({ok:false,build:BUILD,code:'customer_lookup_failed',error:'customer_lookup_failed'},500);

  const byId=new Map();
  for(const row of selected.rows){
    const id=text(row&&row.customer_id);
    if(CUSTOMER_ID_RE.test(id)&&parsed.ids.includes(id)&&!byId.has(id)){
      byId.set(id,{customer_id:id,name:text(row.name),updated_at:text(row.updated_at)});
    }
  }
  const items=parsed.ids.filter(id=>byId.has(id)).map(id=>byId.get(id));
  const missing=parsed.ids.filter(id=>!byId.has(id));
  return json({ok:true,build:BUILD,source:'customer-crm',lookup_key:'customer_id',fallback_used:false,requested_count:parsed.ids.length,found_count:items.length,items,missing});
}

export function internalCustomerNameBatchHealth(env){
  return {
    internal_customer_name_batch_enabled:true,
    internal_customer_name_batch_read_only:true,
    internal_customer_name_batch_method:'GET',
    internal_customer_name_batch_lookup_key:'customer_id',
    internal_customer_name_batch_fallback:false,
    internal_customer_name_batch_max_ids:MAX_IDS,
    internal_customer_name_batch_service_auth:'CRM_INTERNAL_TOKEN',
    internal_customer_name_batch_token_configured:!!text(env&&env.CRM_INTERNAL_TOKEN),
    internal_customer_name_batch_build:BUILD
  };
}
