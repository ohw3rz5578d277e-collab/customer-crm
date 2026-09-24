import { isCompletedReservationStatus } from './crm-reservation-status-contract.mjs';

const BUILD='crm-member-memory-source-20260924-01';
const CUSTOMER_ID_RE=/^\d{8}$/;
const MAX_RESERVATION_ROWS=500;

const text=v=>v==null?'':String(v).trim();

function json(data,status=200){
  return new Response(JSON.stringify(data),{
    status,
    headers:{
      'content-type':'application/json; charset=utf-8',
      'cache-control':'no-store',
      'x-crm-member-memory-source-build':BUILD,
      'x-robots-tag':'noindex, nofollow',
      'referrer-policy':'no-referrer'
    }
  });
}

function bearer(request){
  const h=text(request.headers.get('authorization'));
  return /^Bearer\s+/i.test(h)?h.replace(/^Bearer\s+/i,'').trim():'';
}

function internalAllowed(request,env){
  const expected=text(env?.CRM_INTERNAL_TOKEN);
  const supplied=text(request.headers.get('x-internal-token')||bearer(request));
  return !!expected&&supplied===expected;
}

async function first(env,sql,params=[]){
  let q=env.DB.prepare(sql);
  if(params.length)q=q.bind(...params);
  return (await q.first())||null;
}

async function all(env,sql,params=[]){
  let q=env.DB.prepare(sql);
  if(params.length)q=q.bind(...params);
  const r=await q.all();
  return r.results||[];
}

async function tableExists(env,name){
  return !!(await first(env,"SELECT name FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",[name]));
}

function validHttpsUrl(value){
  try{
    const u=new URL(text(value));
    return u.protocol==='https:'?u.toString():'';
  }catch{
    return '';
  }
}

function stableReservationId(row){
  const id=text(row?.reservation_id);
  return id&&id.length<=160?id:'';
}

export function isLegacyCompletedImport(row){
  return text(row?.source)==='customer_csv_import'
    && text(row?.status)==='CSV取込'
    && /^CSV-[A-F0-9]{8,}$/i.test(stableReservationId(row))
    && !!text(row?.shoot_date);
}

export function isMemoryEligibleReservation(row){
  return isCompletedReservationStatus(row?.status)||isLegacyCompletedImport(row);
}

function currentReservationRows(rows){
  const byReservation=new Map();
  const missingStable=[];
  for(const row of rows||[]){
    const reservationId=stableReservationId(row);
    if(!reservationId){
      missingStable.push(row);
      continue;
    }
    if(!byReservation.has(reservationId))byReservation.set(reservationId,row);
  }
  return {current:[...byReservation.values()],missingStable};
}

function deliveryMap(rows,customerId){
  const map=new Map();
  let unassigned=0;
  for(const row of rows||[]){
    if(text(row.customer_id)!==customerId)continue;
    const reservationId=text(row.reservation_id);
    const url=validHttpsUrl(row.url);
    if(!reservationId||text(row.provider)!=='amazon_photos'||!url){
      unassigned++;
      continue;
    }
    if(!map.has(reservationId)){
      map.set(reservationId,{
        link_id:text(row.link_id),
        provider:'amazon_photos',
        url,
        delivered_at:text(row.delivered_at),
        created_at:text(row.created_at)
      });
    }
  }
  return {map,unassigned};
}

export async function readMemberMemorySourceByCustomer(env,customerId){
  const id=text(customerId);
  if(!CUSTOMER_ID_RE.test(id))return {status:'invalid_customer_id',customer_id:id,memories:[]};
  if(!(await tableExists(env,'customers')))return {status:'customers_schema_missing',customer_id:id,memories:[]};

  const customer=await first(
    env,
    "SELECT customer_id FROM customers WHERE CAST(customer_id AS TEXT)=? AND COALESCE(deleted_at,'')='' LIMIT 1",
    [id]
  );
  if(!customer||text(customer.customer_id)!==id)return {status:'customer_not_found',customer_id:id,memories:[]};

  if(!(await tableExists(env,'customer_reservations'))){
    return {status:'reservation_schema_missing',customer_id:id,memories:[]};
  }

  // Newest row first. Repeated event rows for one reservation collapse to the latest state.
  const reservationRows=await all(
    env,
    `SELECT reservation_id,customer_id,genre,shoot_date,plan_label,place,status,source,created_at,updated_at
       FROM customer_reservations
       WHERE CAST(customer_id AS TEXT)=?
         AND COALESCE(deleted_at,'')=''
       ORDER BY COALESCE(updated_at,created_at,'') DESC, COALESCE(created_at,'') DESC
       LIMIT ${MAX_RESERVATION_ROWS}`,
    [id]
  );

  const exactRows=reservationRows.filter(row=>text(row.customer_id)===id);
  const {current,missingStable}=currentReservationRows(exactRows);

  let deliveryRows=[];
  if(await tableExists(env,'customer_delivery_links')){
    deliveryRows=await all(
      env,
      `SELECT link_id,customer_id,reservation_id,provider,url,delivered_at,created_at
         FROM customer_delivery_links
         WHERE customer_id=?
         ORDER BY COALESCE(delivered_at,created_at,'') DESC, created_at DESC
         LIMIT 500`,
      [id]
    );
  }
  const deliveries=deliveryMap(deliveryRows,id);

  const memories=current
    .filter(isMemoryEligibleReservation)
    .map(row=>{
      const reservationId=stableReservationId(row);
      const delivery=deliveries.map.get(reservationId)||null;
      return {
        source_system:'customer-crm',
        source_customer_id:id,
        source_reservation_id:reservationId,
        shoot_date:text(row.shoot_date),
        genre:text(row.genre),
        plan_label:text(row.plan_label),
        place:text(row.place),
        source_status:text(row.status),
        source:text(row.source),
        eligibility:isLegacyCompletedImport(row)?'legacy_completed_import':'completed_reservation',
        amazon_photos_url:delivery?.url||'',
        delivery_link_id:delivery?.link_id||'',
        delivered_at:delivery?.delivered_at||'',
        sync_eligible:true
      };
    })
    .sort((a,b)=>a.shoot_date.localeCompare(b.shoot_date)||a.source_reservation_id.localeCompare(b.source_reservation_id));

  return {
    status:'ok',
    customer_id:id,
    source:'customer-crm',
    lookup_key:'customer_id',
    fallback_used:false,
    memories,
    diagnostics:{
      exact_reservation_rows:exactRows.length,
      current_reservations:current.length,
      eligible_memories:memories.length,
      missing_stable_reservation_id:missingStable.length,
      unassigned_delivery_links:deliveries.unassigned
    }
  };
}

export async function handleMemberMemorySourceReadRequest(request,env){
  const url=new URL(request.url);
  const m=url.pathname.match(/^\/api\/internal\/member-memory-source\/customer\/(\d{8})$/);
  if(!m)return null;
  if(request.method!=='GET')return json({ok:false,error:'method_not_allowed'},405);
  if(!internalAllowed(request,env))return json({ok:false,error:'authentication_required'},401);

  const result=await readMemberMemorySourceByCustomer(env,m[1]);
  if(result.status==='ok')return json({ok:true,...result});
  if(result.status==='customer_not_found')return json({ok:false,error:'customer_not_found'},404);
  if(result.status.endsWith('_schema_missing'))return json({ok:false,error:result.status},409);
  return json({ok:false,error:result.status},400);
}

export function memberMemorySourceHealth(){
  return {
    member_memory_source:true,
    build:BUILD,
    read_only:true,
    production_route_wired:false,
    exact_customer_id_only:true,
    completed_status_contract_reused:true,
    legacy_completed_import_supported:true,
    latest_reservation_state_wins:true,
    amazon_photos_exact_reservation_only:true,
    customer_name_fallback:false,
    fuzzy_matching:false,
    production_write:false
  };
}
