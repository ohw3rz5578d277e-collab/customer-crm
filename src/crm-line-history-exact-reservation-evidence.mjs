import { createHash } from 'node:crypto';

function text(v){return v==null?'':String(v).trim()}
function add(map,key,value){
  if(!key)return;
  if(!map.has(key))map.set(key,[]);
  map.get(key).push(value);
}

function reservationHash(v){
  return createHash('sha256').update(text(v)).digest('hex').slice(0,24);
}

export function buildExactReservationEvidence({reservationHistory=[],crmReservations=[]}={}){
  const crmByReservation=new Map();

  for(const row of crmReservations||[]){
    const reservationId=text(row.reservation_id??row.reservationId);
    if(!reservationId)continue;

    add(crmByReservation,reservationId,{
      reservation_id:reservationId,
      target_customer_id:text(row.target_customer_id??row.customer_id??row.crm_customer_id)
    });
  }

  const rows=[];
  const seen=new Set();

  for(const row of reservationHistory||[]){
    const sourceCustomerId=text(
      row.source_customer_id??
      row.reservation_customer_id??
      row.customer_id
    );
    const reservationId=text(row.reservation_id??row.reservationId);

    if(!sourceCustomerId||!reservationId)continue;

    for(const target of crmByReservation.get(reservationId)||[]){
      const key=[
        sourceCustomerId,
        reservationId,
        target.target_customer_id
      ].join('\u0000');

      if(seen.has(key))continue;
      seen.add(key);

      rows.push({
        source_customer_id:sourceCustomerId,
        reservation_id_hash:reservationHash(reservationId),
        target_customer_id:target.target_customer_id
      });
    }
  }

  rows.sort((a,b)=>
    a.source_customer_id.localeCompare(b.source_customer_id)||
    a.reservation_id_hash.localeCompare(b.reservation_id_hash)||
    a.target_customer_id.localeCompare(b.target_customer_id)
  );

  return {
    planner:'line_history_exact_reservation_evidence_v1',
    reservation_history_rows:(reservationHistory||[]).length,
    crm_reservation_rows:(crmReservations||[]).length,
    exact_evidence_rows:rows.length,
    source_groups_with_exact_match:new Set(rows.map(x=>x.source_customer_id)).size,
    rows,
    safety:{
      production_d1_read:0,
      production_d1_write:0,
      customer_id_generation:0,
      customer_update:0,
      customer_delete:0,
      line_send:0,
      raw_reservation_id_output:false,
      raw_line_user_id_output:false,
      message_text_output:false
    }
  };
}
