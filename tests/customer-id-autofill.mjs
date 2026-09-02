import fs from 'node:fs';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {assignCanonicalCustomerIdToCustomerRef,customerRefFromRowid} from '../src/crm-customer-id-autofill-runtime.mjs';
import {handleCustomer360Request} from '../src/crm-customer360-runtime.mjs';

const registryMigration=fs.readFileSync('migrations_managed/20260818_customer_identity_registry.sql','utf8');
const sequenceMigration=fs.readFileSync('migrations_managed/20260818_customer_identity_sequence.sql','utf8');
const uid=n=>'U'+String(n).padStart(32,'0');
function d1(db){return{prepare(sql){let params=[];return{bind(...v){params=v;return this},async all(){return{results:db.prepare(sql).all(...params)}},async first(){return db.prepare(sql).get(...params)||null},async run(){return db.prepare(sql).run(...params)}}}}}
function setup(rows=[]){const raw=new DatabaseSync(':memory:');raw.exec(`CREATE TABLE customers(customer_id TEXT,name TEXT,line_display_name TEXT,line_user_id TEXT,acquisition_source TEXT,created_at TEXT,updated_at TEXT,deleted_at TEXT);`);for(const r of rows)raw.prepare(`INSERT INTO customers(customer_id,name,line_display_name,line_user_id,acquisition_source) VALUES(?,?,?,?,?)`).run(r.customer_id??null,r.name||'',r.line_display_name||null,r.line_user_id||null,'legacy');raw.exec(registryMigration);raw.exec(sequenceMigration);return{raw,env:{DB:d1(raw),CRM_LOCAL_TEST_AUTH:'1'}}}
const rowRef=(raw,index=0)=>customerRefFromRowid(raw.prepare(`SELECT rowid FROM customers ORDER BY rowid LIMIT 1 OFFSET ?`).get(index).rowid);
const seq=raw=>Number(raw.prepare(`SELECT last_value FROM customer_identity_sequence WHERE sequence_key='canonical_customer_id'`).get().last_value);
const customer=(raw,index=0)=>raw.prepare(`SELECT rowid,* FROM customers ORDER BY rowid LIMIT 1 OFFSET ?`).get(index);
const registry=raw=>raw.prepare(`SELECT * FROM customer_identity_registry ORDER BY id LIMIT 1`).get();

// A formal LINE missing ID -> existing canonical resolver path + registry consistency.
{const {raw,env}=setup([{line_user_id:uid(1),name:'LINE顧客'}]);const out=await assignCanonicalCustomerIdToCustomerRef(env,{customer_ref:rowRef(raw)},{year:2026});assert.equal(out.ok,true);assert.match(out.customer_id,/^26\d{6}$/);assert.equal(customer(raw).customer_id,out.customer_id);assert.equal(registry(raw).customer_id,out.customer_id);assert.equal(registry(raw).line_user_id,uid(1));}
// B no LINE missing ID -> same global allocator/sequence.
{const {raw,env}=setup([{name:'電話顧客'}]);const before=seq(raw);const out=await assignCanonicalCustomerIdToCustomerRef(env,{customer_ref:rowRef(raw)},{year:2026});assert.equal(out.ok,true);assert.equal(out.customer_id,'26000001');assert.equal(seq(raw),before+1);assert.equal(customer(raw).customer_id,out.customer_id);}
// C already canonical -> identical ID, no sequence consumption.
{const {raw,env}=setup([{customer_id:'26000123',name:'既存'}]);const before=seq(raw);const out=await assignCanonicalCustomerIdToCustomerRef(env,{customer_ref:rowRef(raw)},{year:2026});assert.equal(out.ok,true);assert.equal(out.customer_id,'26000123');assert.equal(out.already_assigned,true);assert.equal(seq(raw),before);}
// D duplicate/retry -> one ID only; second request consumes no additional sequence.
{const {raw,env}=setup([{name:'再送'}]);const ref=rowRef(raw),a=await assignCanonicalCustomerIdToCustomerRef(env,{customer_ref:ref},{year:2026}),after=seq(raw),b=await assignCanonicalCustomerIdToCustomerRef(env,{customer_ref:ref},{year:2026});assert.equal(a.customer_id,b.customer_id);assert.equal(seq(raw),after);assert.equal(raw.prepare(`SELECT COUNT(*) n FROM customers`).get().n,1);}
// E registry conflict on formal LINE -> fail closed.
{const {raw,env}=setup([{line_user_id:uid(5),name:'競合'}]);raw.prepare(`INSERT INTO customer_identity_registry(customer_id,line_user_id,idempotency_key,source,status,created_at,updated_at,raw_json) VALUES(?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,'{}')`).run('26000999',uid(5),'conflict','legacy','active');const out=await assignCanonicalCustomerIdToCustomerRef(env,{customer_ref:rowRef(raw)},{year:2026});assert.equal(out.ok,false);assert.equal(out.statusCode,409);assert.equal(out.review_required,true);assert.equal(customer(raw).customer_id,null);}
// F invalid non-empty ID -> no overwrite, review required, no sequence change.
{const {raw,env}=setup([{customer_id:'ABC123',name:'要確認'}]);const before=seq(raw),out=await assignCanonicalCustomerIdToCustomerRef(env,{customer_ref:rowRef(raw)},{year:2026});assert.equal(out.error,'invalid_existing_customer_id');assert.equal(out.statusCode,409);assert.equal(customer(raw).customer_id,'ABC123');assert.equal(seq(raw),before);}
// G collision candidate -> skip used global suffix and allocate next one.
{const {raw,env}=setup([{customer_id:'25000001',name:'既存suffix'},{name:'新規'}]);raw.prepare(`UPDATE customer_identity_sequence SET last_value=0 WHERE sequence_key='canonical_customer_id'`).run();const out=await assignCanonicalCustomerIdToCustomerRef(env,{customer_ref:rowRef(raw,1)},{year:2026});assert.equal(out.customer_id,'26000002');assert.equal(seq(raw),2);}
// H concurrent conditional-update callers converge to winner without overwrite.
{const {raw,env}=setup([{name:'同時実行'}]);const ref=rowRef(raw);const [a,b]=await Promise.all([assignCanonicalCustomerIdToCustomerRef(env,{customer_ref:ref},{year:2026}),assignCanonicalCustomerIdToCustomerRef(env,{customer_ref:ref},{year:2026})]);assert.equal(a.ok,true);assert.equal(b.ok,true);assert.equal(a.customer_id,b.customer_id);assert.equal(customer(raw).customer_id,a.customer_id);assert.match(a.customer_id,/^26\d{6}$/);}
// Authorized Owner endpoint only; unauthenticated request is rejected before mutation.
{const {raw,env}=setup([{name:'API'}]);const ref=rowRef(raw),before=seq(raw);const noAuth={...env,CRM_LOCAL_TEST_AUTH:'0'};const denied=await handleCustomer360Request(new Request('https://crm/api/customer360/customer-id/allocate',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({customer_ref:ref})}),noAuth);assert.equal(denied.status,401);assert.equal(seq(raw),before);const ok=await handleCustomer360Request(new Request('https://crm/api/customer360/customer-id/allocate',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({customer_ref:ref})}),env);assert.equal(ok.status,200);const body=await ok.json();assert.equal(body.ok,true);assert.match(body.customer_id,/^26\d{6}$/);}
// Technical reference is row targeting only and does not encode PII.
assert.equal(customerRefFromRowid(42),'row:42');assert.equal(customerRefFromRowid('x'),'');
console.log('CUSTOMER_ID_AUTOFILL_MATRIX=A-H PASS');
console.log('FORMAT_YY_PLUS_6=PASS');
console.log('GLOBAL_SEQUENCE=PASS');
console.log('SEQUENCE_MONOTONIC=PASS');
console.log('PRODUCTION_D1_WRITE=0');
console.log('PRODUCTION_ID_GENERATION=0');
