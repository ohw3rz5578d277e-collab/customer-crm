import assert from 'node:assert/strict';
import { handleCustomer360Request } from '../src/crm-customer360-runtime.mjs';
import { isCompletedReservationStatus } from '../src/crm-reservation-status-contract.mjs';

for(const status of ['納品済み','納品完了','本納品済み','本納品完了','delivered','completed']){
  assert.equal(isCompletedReservationStatus(status),true,'completed status missing: '+status);
}

const customer={
  customer_id:'26000031',
  name:'案内NG 顧客',
  line_user_id:'U_BLOCKED',
  phone:'09000000000',
  email:'blocked@example.com',
  total_revenue:50000,
  repeat_count:1,
  first_shoot_date:'2026-01-10',
  last_shoot_date:'2026-01-10',
  dormant_days:220,
  deleted_at:''
};
const marketing={customer_id:'26000031',marketing_opt_out:0,preferred_contact_channel:'LINE'};
const enrichment={customer_id:'26000031',marketing_contact_permission:'denied'};
const existing=new Set(['customer_marketing_profiles','customer_profile_enrichment']);

const env={
  CRM_LOCAL_TEST_AUTH:'1',
  DB:{
    prepare(sql){
      let args=[];
      const stmt={
        bind(...values){args=values;return stmt},
        async first(){
          if(sql.includes('sqlite_master'))return existing.has(String(args[0]||''))?{name:String(args[0])}:null;
          return null;
        },
        async all(){
          if(sql.includes('SELECT rowid AS __customer_ref,* FROM customers'))return{results:[customer]};
          if(sql.includes('SELECT * FROM customer_marketing_profiles'))return{results:[marketing]};
          if(sql.includes('SELECT customer_id,marketing_contact_permission FROM customer_profile_enrichment'))return{results:[enrichment]};
          return{results:[]};
        },
        async run(){throw new Error('read_only_test_write_attempt')}
      };
      return stmt;
    }
  }
};

const response=await handleCustomer360Request(new Request('https://example.test/api/customer360/approach-queue?horizon_days=365&status=all'),env);
assert.equal(response.status,200);
const body=await response.json();
assert.equal(body.ok,true);
assert.equal(body.items.length,1);
assert.equal(body.items[0].customer_id,'26000031');
assert.equal(body.items[0].contact.code,'blocked_opt_out');
assert.equal(body.items[0].contact.ready,false);
assert.equal(body.summary.opted_out,1);
assert.equal(body.meta.line_send,false);
assert.equal(body.meta.automatic_contact,false);
assert.ok(!JSON.stringify(body).includes('09000000000'));
assert.ok(!JSON.stringify(body).includes('blocked@example.com'));

console.log('CUSTOMER360_PROFILE_MARKETING_DENIAL_BLOCK=PASS');
console.log('RESERVATION_DELIVERED_STATUS_ALIGNMENT=PASS');
console.log('PRODUCTION_D1_WRITE=0');
console.log('LINE_SEND=0');
