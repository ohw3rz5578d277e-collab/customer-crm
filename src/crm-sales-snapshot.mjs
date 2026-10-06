function text(v){return v==null?'':String(v).trim()}

function collectCustomerRows(value,out=[]){
  if(Array.isArray(value)){
    for(const item of value)collectCustomerRows(item,out);
    return out;
  }
  if(!value||typeof value!=='object')return out;
  const hasCustomerId=Object.prototype.hasOwnProperty.call(value,'customer_id');
  const hasName=Object.prototype.hasOwnProperty.call(value,'name');
  if(hasCustomerId&&hasName){
    out.push(value);
    return out;
  }
  for(const item of Object.values(value))collectCustomerRows(item,out);
  return out;
}

export function parseProductionSnapshotText(raw){
  const source=String(raw??'').replace(/^\uFEFF/,'');
  if(!source.trim())throw new Error('production_snapshot_empty');

  let parsed;
  try{
    parsed=JSON.parse(source);
  }catch{
    throw new Error('production_snapshot_invalid_json');
  }

  const discovered=collectCustomerRows(parsed,[]);
  if(!discovered.length)throw new Error('production_snapshot_customer_rows_empty');

  const seen=new Set();
  const rows=[];
  for(const row of discovered){
    const customerId=text(row?.customer_id);
    if(!customerId)throw new Error('production_snapshot_customer_id_required');
    if(seen.has(customerId))throw new Error('production_snapshot_duplicate_customer_id');
    seen.add(customerId);
    rows.push(row);
  }
  return rows;
}
