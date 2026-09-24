const BUILD = 'crm-member-family-identity-20260924-01';
const CUSTOMER_ID_RE = /^\d{8}$/;

const text = v => v == null ? '' : String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-crm-member-family-build':BUILD,
      'x-robots-tag':'noindex, nofollow',
      'referrer-policy':'no-referrer'
    }
  });
}

function bearer(request){
  const h=text(request.headers.get('authorization'));
  return /^Bearer\s+/i.test(h) ? h.replace(/^Bearer\s+/i,'').trim() : '';
}

function internalAllowed(request,env){
  const supplied=text(request.headers.get('x-internal-token')||bearer(request));
  const expected=text(env?.CRM_INTERNAL_TOKEN);
  return !!expected && supplied===expected;
}

async function first(env,sql,params=[]){
  let q=env.DB.prepare(sql);
  if(params.length) q=q.bind(...params);
  return (await q.first()) || null;
}

async function all(env,sql,params=[]){
  let q=env.DB.prepare(sql);
  if(params.length) q=q.bind(...params);
  const r=await q.all();
  return r.results || [];
}

async function tableExists(env,name){
  return !!(await first(env,"SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",[name]));
}

export async function readMemberFamilyByCustomer(env,customerId){
  const id=text(customerId);
  if(!CUSTOMER_ID_RE.test(id)) return {status:'invalid_customer_id',customer_id:id,family:null};
  if(!(await tableExists(env,'customer_family_groups')) || !(await tableExists(env,'customer_family_customer_links'))){
    return {status:'schema_not_applied',customer_id:id,family:null};
  }

  const links=await all(
    env,
    "SELECT family_id,customer_id,relation,access_role FROM customer_family_customer_links WHERE customer_id=? AND COALESCE(deleted_at,'')='' LIMIT 2",
    [id]
  );
  if(links.length===0) return {status:'unlinked',customer_id:id,family:null};
  if(links.length>1) return {status:'ambiguous_family_identity',customer_id:id,family:null};
  const link=links[0];

  const family=await first(
    env,
    "SELECT family_id,display_name,status,created_at,updated_at FROM customer_family_groups WHERE family_id=? AND status='active' LIMIT 1",
    [text(link.family_id)]
  );
  if(!family) return {status:'family_inactive_or_missing',customer_id:id,family:null};

  const members=await all(
    env,
    "SELECT customer_id,relation,access_role FROM customer_family_customer_links WHERE family_id=? AND COALESCE(deleted_at,'')='' ORDER BY created_at,customer_id",
    [text(family.family_id)]
  );

  return {
    status:'linked',
    customer_id:id,
    family:{
      family_id:text(family.family_id),
      display_name:text(family.display_name),
      relation:text(link.relation),
      access_role:text(link.access_role),
      member_customer_ids:members.map(x=>text(x.customer_id)).filter(CUSTOMER_ID_RE.test.bind(CUSTOMER_ID_RE))
    }
  };
}

export async function handleMemberFamilyIdentityReadRequest(request,env){
  const url=new URL(request.url);
  const m=url.pathname.match(/^\/api\/internal\/member-family\/customer\/(\d{8})$/);
  if(!m) return null;
  if(request.method!=='GET') return json({ok:false,error:'method_not_allowed'},405);
  if(!internalAllowed(request,env)) return json({ok:false,error:'authentication_required'},401);

  const result=await readMemberFamilyByCustomer(env,m[1]);
  if(result.status==='linked') return json({ok:true,...result});
  if(result.status==='unlinked') return json({ok:true,...result});
  if(result.status==='schema_not_applied') return json({ok:false,error:'member_family_schema_not_applied'},409);
  if(result.status==='ambiguous_family_identity') return json({ok:false,error:'ambiguous_family_identity',review_required:true},409);
  return json({ok:false,error:result.status},400);
}

export function memberFamilyIdentityHealth(){
  return {
    member_family_identity_foundation:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    customer_identity_source:'canonical_customer_id_only',
    family_auto_inference:false,
    duplicate_active_family_link_fail_closed:true,
    name_match:false,
    address_match:false,
    phone_match:false,
    email_match:false,
    line_display_name_match:false,
    customer_id_generation:false,
    family_id_generation:false,
    production_write:false
  };
}

export const __test={CUSTOMER_ID_RE};
