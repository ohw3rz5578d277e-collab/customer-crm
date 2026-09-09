const BUILD='crm-customer360-media-20260908-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_AVATAR_CHARS=96000;
const text=v=>v==null?'':String(v).trim();
function json(data,status=200){return new Response(JSON.stringify(data),{status,headers:{'content-type':'application/json; charset=utf-8','cache-control':'no-store','x-crm-customer360-media-build':BUILD,'x-robots-tag':'noindex, nofollow','referrer-policy':'no-referrer'}})}
function accessEmail(request){return text(request.headers.get('cf-access-authenticated-user-email')||request.headers.get('Cf-Access-Authenticated-User-Email')||request.headers.get('cf-access-user-email'))}
function bearer(request){const h=text(request.headers.get('authorization'));return /^Bearer\s+/i.test(h)?h.replace(/^Bearer\s+/i,'').trim():''}
function internalAllowed(request,env){const supplied=text(request.headers.get('x-internal-token')||bearer(request)),expected=text(env?.CRM_INTERNAL_TOKEN);return !!expected&&supplied===expected}
function ownerAllowed(request,env){return env?.CRM_LOCAL_TEST_AUTH==='1'||!!accessEmail(request)}
async function first(env,sql,params=[]){let q=env.DB.prepare(sql);if(params.length)q=q.bind(...params);return (await q.first())||null}
async function all(env,sql,params=[]){let q=env.DB.prepare(sql);if(params.length)q=q.bind(...params);const r=await q.all();return r.results||[]}
async function tableExists(env,name){return !!(await first(env,"SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",[name]))}
async function exactCustomer(env,id){if(!CUSTOMER_ID_RE.test(id))return null;const c=await first(env,"SELECT customer_id FROM customers WHERE CAST(customer_id AS TEXT)=? AND COALESCE(deleted_at,'')='' LIMIT 1",[id]);return c&&text(c.customer_id)===id?c:null}
function validHttpsUrl(v){try{const u=new URL(text(v));return u.protocol==='https:'?u.toString():''}catch{return''}}
function validAvatar(v){const s=text(v);if(!s)return'';if(s.length>MAX_AVATAR_CHARS)return'';return /^data:image\/(?:jpeg|png|webp);base64,[A-Za-z0-9+/=]+$/.test(s)?s:''}
async function readMedia(env,id){
  let avatar_data_url='',avatar_updated_at='';
  if(await tableExists(env,'customer_profile_media')){
    const r=await first(env,'SELECT avatar_data_url,avatar_updated_at FROM customer_profile_media WHERE customer_id=? LIMIT 1',[id]);
    avatar_data_url=text(r?.avatar_data_url);avatar_updated_at=text(r?.avatar_updated_at);
  }
  let delivery_links=[];
  if(await tableExists(env,'customer_delivery_links')){
    const rows=await all(env,"SELECT link_id,customer_id,reservation_id,provider,label,url,delivered_at,created_at FROM customer_delivery_links WHERE customer_id=? ORDER BY COALESCE(delivered_at,created_at) DESC, created_at DESC LIMIT 50",[id]);
    delivery_links=rows.map(r=>({link_id:text(r.link_id),customer_id:id,reservation_id:text(r.reservation_id),provider:text(r.provider)||'amazon_photos',label:text(r.label),url:text(r.url),delivered_at:text(r.delivered_at),created_at:text(r.created_at)}));
  }
  return{customer_id:id,avatar_data_url,avatar_updated_at,latest_delivery_link:delivery_links[0]||null,delivery_links};
}
async function patchAvatar(request,env,id){
  if(env?.CRM_CUSTOMER360_WRITE_ENABLED!=='1')return json({ok:false,error:'customer360_write_disabled'},403);
  if(!ownerAllowed(request,env))return json({ok:false,error:'owner_auth_required'},401);
  if(!(await exactCustomer(env,id)))return json({ok:false,error:'customer_not_found'},404);
  if(!(await tableExists(env,'customer_profile_media')))return json({ok:false,error:'customer_media_schema_not_applied'},409);
  const body=await request.json().catch(()=>null);if(!body||typeof body!=='object')return json({ok:false,error:'invalid_json'},400);
  const raw=body.avatar_data_url==null?'':text(body.avatar_data_url),avatar=raw?validAvatar(raw):'';
  if(raw&&!avatar)return json({ok:false,error:'invalid_avatar_data',max_chars:MAX_AVATAR_CHARS},400);
  const actor=accessEmail(request)||'local-test';
  await env.DB.prepare("INSERT INTO customer_profile_media(customer_id,avatar_data_url,avatar_updated_at,updated_at,updated_by) VALUES(?,?,datetime('now'),datetime('now'),?) ON CONFLICT(customer_id) DO UPDATE SET avatar_data_url=excluded.avatar_data_url,avatar_updated_at=datetime('now'),updated_at=datetime('now'),updated_by=excluded.updated_by").bind(id,avatar||null,actor).run();
  return json({ok:true,media:await readMedia(env,id)});
}
async function addDeliveryLink(request,env,id){
  if(env?.CRM_CUSTOMER360_WRITE_ENABLED!=='1')return json({ok:false,error:'customer360_write_disabled'},403);
  if(!ownerAllowed(request,env))return json({ok:false,error:'owner_auth_required'},401);
  if(!(await exactCustomer(env,id)))return json({ok:false,error:'customer_not_found'},404);
  if(!(await tableExists(env,'customer_delivery_links')))return json({ok:false,error:'customer_media_schema_not_applied'},409);
  const body=await request.json().catch(()=>null);if(!body||typeof body!=='object')return json({ok:false,error:'invalid_json'},400);
  const url=validHttpsUrl(body.url);if(!url)return json({ok:false,error:'invalid_delivery_url'},400);
  const provider=text(body.provider)||'amazon_photos';if(provider!=='amazon_photos')return json({ok:false,error:'unsupported_delivery_provider'},400);
  const linkId='dl_'+crypto.randomUUID(),actor=accessEmail(request)||'local-test',label=text(body.label).slice(0,80),reservationId=text(body.reservation_id).slice(0,120),deliveredAt=text(body.delivered_at).slice(0,40);
  await env.DB.prepare("INSERT INTO customer_delivery_links(link_id,customer_id,reservation_id,provider,label,url,delivered_at,created_at,created_by) VALUES(?,?,?,?,?,?,NULLIF(?,''),datetime('now'),?)").bind(linkId,id,reservationId||null,provider,label||null,url,deliveredAt,actor).run();
  return json({ok:true,media:await readMedia(env,id)});
}
export async function handleCustomer360MediaRequest(request,env){
  const url=new URL(request.url),m=url.pathname.match(/^\/api\/customer360\/media\/(\d{8})(?:\/(delivery-links))?\/?$/);if(!m)return null;
  const id=m[1],sub=m[2]||'';
  if(!ownerAllowed(request,env)&&!internalAllowed(request,env))return json({ok:false,error:'unauthorized'},401);
  if(!(await exactCustomer(env,id)))return json({ok:false,error:'customer_not_found'},404);
  if(request.method==='GET')return json({ok:true,media:await readMedia(env,id)});
  if(!sub&&request.method==='PATCH')return patchAvatar(request,env,id);
  if(sub==='delivery-links'&&request.method==='POST')return addDeliveryLink(request,env,id);
  return json({ok:false,error:'method_not_allowed'},405);
}
export function customer360MediaHealth(){return{
  customer360_media_api:true,
  customer360_avatar_manual_upload:true,
  customer360_avatar_original_storage:false,
  customer360_avatar_max_chars:MAX_AVATAR_CHARS,
  customer360_delivery_provider:'amazon_photos',
  customer360_delivery_link_history:true,
  customer360_media_customer_id_generation:false,
  customer360_media_paid_storage_required:false
}}
