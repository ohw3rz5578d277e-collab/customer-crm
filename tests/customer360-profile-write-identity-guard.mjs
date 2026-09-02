import assert from 'node:assert/strict';
import { guardCustomer360ProfileWrite, customer360ProfileWriteGuardHealth } from '../src/crm-customer360-profile-write-guard.mjs';

function envWith(existing=[]){
  return {DB:{prepare(sql){const stmt={args:[],bind(...args){stmt.args=args;return stmt},async first(){if(sql.includes('SELECT customer_id FROM customers')){const id=String(stmt.args[0]||'');return existing.includes(id)?{customer_id:id}:null}return null}};return stmt}}};
}
async function patch(customerId,changes,env){return guardCustomer360ProfileWrite(new Request(`https://example.test/api/customer360/profile/${customerId}`,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({changes})}),env)}

const missing=await patch('26000101',{referrer_customer_id:'26000999'},envWith([]));
assert.equal(missing?.status,409);
assert.equal((await missing.json()).error,'referrer_customer_not_found');

const self=await patch('26000101',{referrer_customer_id:'26000101'},envWith(['26000101']));
assert.equal(self?.status,409);
assert.equal((await self.json()).error,'referrer_customer_id_self_reference');

const invalid=await patch('26000101',{referrer_customer_id:'山田花子'},envWith([]));
assert.equal(invalid?.status,400);

const valid=await patch('26000101',{referrer_customer_id:'26000102'},envWith(['26000102']));
assert.equal(valid,null);

const nameOnly=await patch('26000101',{referrer_name:'山田 花子'},envWith([]));
assert.equal(nameOnly,null,'referrer_name may be saved as text but must never resolve identity');

const h=customer360ProfileWriteGuardHealth();
assert.equal(h.referrer_customer_id_exact_existing_customer_only,true);
assert.equal(h.referrer_name_identity_fallback,false);
assert.equal(h.self_referral_denied,true);
console.log('REFERRER_EXACT_EXISTING_CUSTOMER_ONLY=PASS');
console.log('REFERRER_NAME_IDENTITY_FALLBACK=0');
