import assert from 'node:assert/strict';
import fs from 'node:fs';
import {handleInternalCustomerNameBatch,internalCustomerNameBatchHealth} from '../src/crm-internal-customer-name-batch.mjs';

const source=fs.readFileSync(new URL('../src/crm-internal-customer-name-batch.mjs',import.meta.url),'utf8');
assert.doesNotMatch(source,/\b(?:INSERT|UPDATE|DELETE|ALTER|CREATE|DROP|REPLACE)\b/i,'batch reader must remain read-only');
assert.match(source,/^const CUSTOMER_ID_RE=\/\^\\d\{8\}\$\//m,'exact 8-digit Customer ID guard missing');
assert.match(source,/fallback_used:false/,'fallback must stay disabled');
assert.doesNotMatch(source,/name\s*=\s*\?|phone\s*=\s*\?|line_user_id\s*=\s*\?/i,'fuzzy/secondary identity lookup must not be introduced');

class FakeDB{
  constructor(rows){this.rows=rows;this.calls=[]}
  prepare(sql){
    const db=this;
    const state={sql,args:[]};
    db.calls.push(state);
    return {
      bind(...args){state.args=args;return this},
      async all(){
        const ids=new Set(state.args.map(String));
        return {results:db.rows.filter(r=>ids.has(String(r.customer_id))&&!String(r.deleted_at||''))};
      }
    };
  }
}
function request(ids,{method='GET',token='secret'}={}){
  const url='https://crm.example/api/internal/customer-name-batch'+(ids==null?'':'?ids='+encodeURIComponent(ids));
  return new Request(url,{method,headers:token?{'x-internal-token':token}:{}});
}
async function body(response){return JSON.parse(await response.text())}

const rows=[
  {customer_id:'25000001',name:'山田 花子',updated_at:'2026-09-10T00:00:00Z',deleted_at:''},
  {customer_id:'25000002',name:'佐藤 太郎',updated_at:'2026-09-09T00:00:00Z',deleted_at:''},
  {customer_id:'25000003',name:'削除済み',updated_at:'2026-09-08T00:00:00Z',deleted_at:'2026-09-09T00:00:00Z'},
  {customer_id:'25000004',name:'山田 花子',updated_at:'2026-09-07T00:00:00Z',deleted_at:''}
];

{
  const r=await handleInternalCustomerNameBatch(request('25000001',{token:''}),{DB:new FakeDB(rows),CRM_INTERNAL_TOKEN:'secret'});
  assert.equal(r.status,401);
}
{
  const r=await handleInternalCustomerNameBatch(request('25000001',{method:'POST'}),{DB:new FakeDB(rows),CRM_INTERNAL_TOKEN:'secret'});
  assert.equal(r.status,405);
}
{
  const r=await handleInternalCustomerNameBatch(request('2500000x'),{DB:new FakeDB(rows),CRM_INTERNAL_TOKEN:'secret'});
  assert.equal(r.status,400);
  assert.equal((await body(r)).code,'invalid_customer_id');
}
{
  const ids=Array.from({length:101},(_,i)=>String(25001000+i)).join(',');
  const r=await handleInternalCustomerNameBatch(request(ids),{DB:new FakeDB(rows),CRM_INTERNAL_TOKEN:'secret'});
  assert.equal(r.status,400);
  assert.equal((await body(r)).code,'too_many_customer_ids');
}
{
  const db=new FakeDB(rows);
  const r=await handleInternalCustomerNameBatch(request('25000002,25000001,25000001,25000003'),{DB:db,CRM_INTERNAL_TOKEN:'secret'});
  assert.equal(r.status,200);
  const j=await body(r);
  assert.equal(j.ok,true);
  assert.equal(j.lookup_key,'customer_id');
  assert.equal(j.fallback_used,false);
  assert.equal(j.requested_count,3);
  assert.equal(j.found_count,2);
  assert.deepEqual(j.items.map(x=>x.customer_id),['25000002','25000001']);
  assert.deepEqual(j.items.map(x=>x.name),['佐藤 太郎','山田 花子']);
  assert.deepEqual(j.missing,['25000003']);
  assert.equal(j.items.some(x=>x.customer_id==='25000004'),false,'same-name unrequested customer must never cross-link');
  assert.equal(db.calls.length,1);
  assert.match(db.calls[0].sql,/^SELECT customer_id, name, updated_at FROM customers/i);
  assert.deepEqual(db.calls[0].args,['25000002','25000001','25000003']);
}

const health=internalCustomerNameBatchHealth({CRM_INTERNAL_TOKEN:'secret'});
assert.equal(health.internal_customer_name_batch_read_only,true);
assert.equal(health.internal_customer_name_batch_lookup_key,'customer_id');
assert.equal(health.internal_customer_name_batch_fallback,false);
assert.equal(health.internal_customer_name_batch_max_ids,100);
assert.equal(health.internal_customer_name_batch_token_configured,true);

console.log('CRM_INTERNAL_CUSTOMER_NAME_BATCH_EXACT_ID=PASS');
console.log('CRM_INTERNAL_CUSTOMER_NAME_BATCH_FUZZY_MATCHING=0');
console.log('CRM_INTERNAL_CUSTOMER_NAME_BATCH_WRITES=0');
