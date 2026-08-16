import fs from 'node:fs';
import { handleInternalCustomerDetail, internalCustomerDetailHealth } from '../src/crm-internal-customer-detail.mjs';

function assert(condition, message){ if(!condition) throw new Error(message); }

class FakeDB {
  constructor({customers=[], reservations=[], failCustomer=false}={}){
    this.customers=customers;
    this.reservations=reservations;
    this.failCustomer=failCustomer;
    this.sql=[];
    this.writeCount=0;
  }
  prepare(sql){
    this.sql.push(sql);
    if(/\b(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE)\b/i.test(sql)) this.writeCount++;
    const db=this;
    return {
      params:[],
      bind(...params){ this.params=params; return this; },
      async first(){
        const params=this.params;
        if(sql.includes("sqlite_master")){
          const table=String(params[0]||'');
          if(table==='customers') return {name:'customers'};
          if(table==='customer_reservations') return {name:'customer_reservations'};
          return null;
        }
        if(sql.includes('FROM customers')){
          if(db.failCustomer) throw new Error('forced customer read failure');
          const id=String(params[0]||'');
          return db.customers.find(x=>String(x.customer_id)===id) || null;
        }
        return null;
      },
      async all(){
        const params=this.params;
        if(sql.includes('FROM customer_reservations')){
          const id=String(params[0]||'');
          return {results:db.reservations.filter(x=>String(x.customer_id)===id)};
        }
        return {results:[]};
      }
    };
  }
}

function req(customerId, token='secret'){
  const q=customerId===undefined?'':`?customer_id=${encodeURIComponent(customerId)}`;
  return new Request(`https://customer-crm-api/api/internal/customer-detail${q}`, {headers:{'x-internal-token':token}});
}
async function body(response){ return JSON.parse(await response.text()); }

const customers=[
  {customer_id:'C-001',name:'顧客A',furigana:'こきゃくえー',line_display_name:'LINE A',line_user_id:'U-A',phone:'09011111111',email:'a@example.test',address:'大阪A',repeat_count:3,total_revenue:86400,customer_rank:'リピーター',memo:'A memo',last_shoot_date:'2026-07-26',genre_history:'マタニティ',acquisition_source:'instagram',updated_at:'2026-08-15'},
  {customer_id:'C-002',name:'顧客B',line_display_name:'LINE B',line_user_id:'',phone:'09022222222',total_revenue:null,repeat_count:1,last_shoot_date:'2026-06-01',genre_history:'お宮参り'}
];
const reservations=[
  {id:2,reservation_id:'R-A2',customer_id:'C-001',customer_name:'顧客A',genre:'マタニティ',shoot_date:'2026-07-26',total_amount:30000,status:'撮影終了'},
  {id:1,reservation_id:'R-A1',customer_id:'C-001',customer_name:'顧客A',genre:'ファミリー',shoot_date:'2026-01-20',total_amount:26400,status:'本納品済'},
  {id:3,reservation_id:'R-B1',customer_id:'C-002',customer_name:'顧客B',genre:'お宮参り',shoot_date:'2026-06-01',total_amount:24800,status:'本納品済'}
];

let passed=0;
async function test(name, fn){ await fn(); passed++; console.log(`PASS ${passed}: ${name}`); }

await test('CASE 1 exact customer_id returns CRM customer and exact history', async()=>{
  const db=new FakeDB({customers,reservations});
  const r=await handleInternalCustomerDetail(req('C-001'),{DB:db,CRM_INTERNAL_TOKEN:'secret'});
  const j=await body(r);
  assert(r.status===200,'status');
  assert(j.ok===true&&j.lookup_key==='customer_id'&&j.fallback_used===false,'strict source flags');
  assert(j.customer.customer_id==='C-001'&&j.customer.name==='顧客A','customer match');
  assert(j.customer.line_linked===true&&j.customer.line_display_name==='LINE A','LINE info');
  assert(j.reservation_history.available===true&&j.reservation_history.count===2,'history count');
  assert(j.reservation_history.items.every(x=>x.customer_id==='C-001'),'history exact customer only');
  assert(j.cumulative_sales.amount===86400&&j.cumulative_sales.source==='customer-crm.customers.total_revenue','formal cumulative sales source');
  assert(db.writeCount===0,'D1 write must remain zero');
});

await test('CASE 2 different customer_id cannot mix customers', async()=>{
  const db=new FakeDB({customers,reservations});
  const r=await handleInternalCustomerDetail(req('C-002'),{DB:db,CRM_INTERNAL_TOKEN:'secret'});
  const j=await body(r);
  assert(j.customer.customer_id==='C-002'&&j.customer.name==='顧客B','wrong customer returned');
  assert(j.reservation_history.items.length===1&&j.reservation_history.items[0].customer_id==='C-002','reservation cross-mix');
  assert(!j.reservation_history.items.some(x=>x.customer_id==='C-001'),'customer A leaked');
});

await test('CASE 3 customer_id missing is a distinct safe error', async()=>{
  const db=new FakeDB({customers,reservations});
  const r=await handleInternalCustomerDetail(req(undefined),{DB:db,CRM_INTERNAL_TOKEN:'secret'});
  const j=await body(r);
  assert(r.status===400&&j.code==='customer_id_missing','missing id handling');
  assert(db.writeCount===0,'write on missing id');
});

await test('CASE 4 CRM customer not found returns 404 without fallback', async()=>{
  const db=new FakeDB({customers,reservations});
  const r=await handleInternalCustomerDetail(req('C-404'),{DB:db,CRM_INTERNAL_TOKEN:'secret'});
  const j=await body(r);
  assert(r.status===404&&j.code==='customer_not_found'&&j.customer_id==='C-404','not found handling');
  assert(db.writeCount===0,'write on not found');
});

await test('CASE 5 CRM read failure returns controlled error', async()=>{
  const db=new FakeDB({customers,reservations,failCustomer:true});
  const r=await handleInternalCustomerDetail(req('C-001'),{DB:db,CRM_INTERNAL_TOKEN:'secret'});
  const j=await body(r);
  assert(r.status===500&&j.error==='customer_lookup_failed','failure handling');
});

await test('internal auth rejects missing or wrong token', async()=>{
  const db=new FakeDB({customers,reservations});
  const r=await handleInternalCustomerDetail(req('C-001','wrong'),{DB:db,CRM_INTERNAL_TOKEN:'secret'});
  assert(r.status===401,'auth failure status');
});

await test('source contains no customer name id or LINE fallback and no writes', async()=>{
  const source=fs.readFileSync(new URL('../src/crm-internal-customer-detail.mjs',import.meta.url),'utf8');
  assert(source.includes('WHERE CAST(customer_id AS TEXT)=?'),'strict customer lookup missing');
  assert(!source.includes('CAST(id AS TEXT)=? OR'),'id fallback found');
  assert(!source.includes('customer_name=?'),'name fallback found');
  assert(!source.includes('line_user_id AS TEXT)=?'),'LINE fallback found');
  assert(!/\b(INSERT INTO|UPDATE customers|DELETE FROM|CREATE TABLE|ALTER TABLE|DROP TABLE)\b/i.test(source),'write/DDL found');
});

await test('health advertises strict read-only customer_id behavior', async()=>{
  const h=internalCustomerDetailHealth({CRM_INTERNAL_TOKEN:'secret'});
  assert(h.internal_customer_detail_enabled===true,'health enabled');
  assert(h.internal_customer_detail_read_only===true,'health read-only');
  assert(h.internal_customer_detail_lookup_key==='customer_id'&&h.internal_customer_detail_fallback===false,'health key/fallback');
  assert(h.internal_customer_detail_token_configured===true,'health token state');
});

console.log(`CRM_INTERNAL_CUSTOMER_DETAIL=${passed}/${passed} PASS`);
