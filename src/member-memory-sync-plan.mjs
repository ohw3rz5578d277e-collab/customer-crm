const text=v=>v==null?'':String(v).trim();

function keyOf(row){
  const system=text(row?.source_system);
  const reservation=text(row?.source_reservation_id);
  return system&&reservation?`${system}:reservation:${reservation}`:'';
}

export function buildMemorySyncPlan({family_id,source_memories=[],existing_memories=[]}={}){
  const familyId=text(family_id);
  if(!familyId)return {ok:false,error:'family_id_required'};

  const existingKeys=new Set();
  for(const row of existing_memories||[]){
    const key=keyOf(row);
    if(key)existingKeys.add(key);
  }

  const seen=new Set();
  const to_create=[];
  const already_synced=[];
  const skipped=[];
  const conflicts=[];

  for(const row of source_memories||[]){
    const reservationId=text(row?.source_reservation_id);
    const sourceSystem=text(row?.source_system)||'customer-crm';
    const key=`${sourceSystem}:reservation:${reservationId}`;

    if(row?.sync_eligible!==true){
      skipped.push({source_reservation_id:reservationId,reason:'source_not_sync_eligible'});
      continue;
    }
    if(!reservationId){
      skipped.push({source_reservation_id:'',reason:'stable_reservation_id_required'});
      continue;
    }
    if(seen.has(key)){
      conflicts.push({source_reservation_id:reservationId,reason:'duplicate_source_reservation'});
      continue;
    }
    seen.add(key);

    if(existingKeys.has(key)){
      already_synced.push({source_reservation_id:reservationId,idempotency_key:key});
      continue;
    }

    to_create.push({
      family_id:familyId,
      source_system:sourceSystem,
      source_customer_id:text(row.source_customer_id),
      source_reservation_id:reservationId,
      idempotency_key:key,
      shoot_date:text(row.shoot_date),
      genre:text(row.genre),
      title:text(row.genre)||'MEMORY',
      amazon_photos_url:text(row.amazon_photos_url),
      delivery_link_id:text(row.delivery_link_id),
      published:1
    });
  }

  return {
    ok:conflicts.length===0,
    family_id:familyId,
    to_create,
    already_synced,
    skipped,
    conflicts,
    write_executed:false
  };
}
