function text(v){return v==null?'':String(v).trim()}

export const PRODUCTION_SNAPSHOT_FORMAT='customer-crm-production-identity-snapshot-v1';
export const PRODUCTION_SNAPSHOT_SCOPE='all_customer_identities';

export function parseProductionSnapshotText(raw){
  const source=String(raw??'').replace(/^\uFEFF/,'');
  if(!source.trim())throw new Error('production_snapshot_empty');

  let parsed;
  try{parsed=JSON.parse(source)}catch{throw new Error('production_snapshot_invalid_json')}

  if(!parsed||typeof parsed!=='object'||Array.isArray(parsed))throw new Error('production_snapshot_envelope_required');
  if(parsed.snapshot_format!==PRODUCTION_SNAPSHOT_FORMAT)throw new Error('production_snapshot_format_invalid');
  if(parsed.complete!==true)throw new Error('production_snapshot_complete_required');
  if(parsed.query_scope!==PRODUCTION_SNAPSHOT_SCOPE)throw new Error('production_snapshot_scope_invalid');
  if(!Number.isInteger(parsed.customer_count)||parsed.customer_count<0)throw new Error('production_snapshot_customer_count_invalid');
  if(!Array.isArray(parsed.customers))throw new Error('production_snapshot_customers_array_required');
  if(parsed.customer_count!==parsed.customers.length)throw new Error('production_snapshot_customer_count_mismatch');
  if(parsed.customers.length===0)throw new Error('production_snapshot_customer_rows_empty');

  const seen=new Set(),rows=[];
  for(const row of parsed.customers){
    if(!row||typeof row!=='object'||Array.isArray(row))throw new Error('production_snapshot_customer_row_invalid');
    if(!Object.prototype.hasOwnProperty.call(row,'customer_id'))throw new Error('production_snapshot_customer_id_required');
    if(!Object.prototype.hasOwnProperty.call(row,'name'))throw new Error('production_snapshot_customer_name_field_required');
    const customerId=text(row.customer_id);
    if(!customerId)throw new Error('production_snapshot_customer_id_required');
    if(seen.has(customerId))throw new Error('production_snapshot_duplicate_customer_id');
    seen.add(customerId);rows.push(row);
  }
  return rows;
}
