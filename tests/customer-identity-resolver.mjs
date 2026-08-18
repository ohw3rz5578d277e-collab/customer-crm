import fs from 'node:fs';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {resolveOrCreateCustomerIdentity,handleCustomerIdentityResolver,formatCanonicalCustomerId} from '../src/customer-identity-resolver.mjs';

const registryMigration=fs.readFileSync('migrations_managed/20260818_customer_identity_registry.sql','utf8');
const sequenceMigration=fs.readFileSync('migrations_managed/20260818_customer_identity_sequence.sql','utf8');
const uid=n=>'U'+String(n).padStart(32,'0');
function d1(db,fail={}){return{prepare(sql){let params=[];return{bind(...v){params=v;return this},async all(){return{results:db.prepare(sql).all(...params)}},async first(){return db.prepare(sql).get(...params)||null},async run(){if(fail.customerInsertOnce&&/INSERT INTO customers/i.test(sql)){fail.customerInsertOnce=false;throw new Error('injected')};return db.prepare(sql).run(...params)}}}}}
function setup(fail={},customers=[]){const db=new DatabaseSync(':memory:');db.exec(`CREATE TABLE customers(customer_id TEXT PRIMARY KEY,name TEXT,line_user_id TEXT,acquisition_source TEXT,created_at TEXT,updated_at TEXT);`);for(const c of customers)db.prepare(`INSERT INTO customers(customer_id,name,line_user_id,acquisition_source) VALUES(?,?,?,?)`).run(c.id,c.name||'',c.line||null,'legacy');db.exec(registryMigration);db.exec(sequenceMigration);return{raw:db,env:{DB:d1(db,fail),CRM_INTERNAL_TOKEN:'test-secret'}}}
const count=(db,t)=>Number(db.prepare(`SELECT COUNT(*) n FROM ${t}`).get().n);
const input=(n,key='k'+n)=>({line_user_id:uid(n),idempotency_key:key,source:'reservation-ai-line'});

// FORMAT A-E
assert.equal(formatCanonicalCustomerId(2026,1),'26000001');
assert.equal(formatCanonicalCustomerId(2026,126),'26000126');
assert.equal(formatCanonicalCustomerId(2027,126),'27000126');
assert.equal(formatCanonicalCustomerId(2026,999999),'26999999');
assert.throws(()=>formatCanonicalCustomerId(2026,1000000),/customer_identity_sequence_exhausted/);

// allocator CASE 1: initialize from max numeric suffix 126 -> 127.
{const {raw,env}=setup({},[{id:'26000126'}]);const r=await resolveOrCreateCustomerIdentity(env,input(1),{year:2026});assert.equal(r.customer_id,'26000127');assert.equal(raw.prepare(`SELECT last_value FROM customer_identity_sequence`).get().last_value,127);}
// CASE 2: candidate collision skips without modifying existing customer.
{const {raw,env}=setup({},[{id:'25000127'}]);raw.prepare(`UPDATE customer_identity_sequence SET last_value=126`).run();raw.prepare(`INSERT INTO customers(customer_id,name,line_user_id) VALUES(?,?,?)`).run('26000127','keep',uid(99));const r=await resolveOrCreateCustomerIdentity(env,input(2),{year:2026});assert.equal(r.customer_id,'26000128');assert.equal(raw.prepare(`SELECT name FROM customers WHERE customer_id='26000127'`).get().name,'keep');}
// CASE 3 existing exact LINE reuse; no allocation.
{const {raw,env}=setup({},[{id:'26000126',line:uid(3)}]);const before=raw.prepare(`SELECT last_value FROM customer_identity_sequence`).get().last_value;const r=await resolveOrCreateCustomerIdentity(env,input(3),{year:2026});assert.equal(r.customer_id,'26000126');assert.equal(raw.prepare(`SELECT last_value FROM customer_identity_sequence`).get().last_value,before);}
// CASE 4 new numeric 8-digit customer.
{const {raw,env}=setup();const r=await resolveOrCreateCustomerIdentity(env,input(4),{year:2026});assert.equal(r.customer_id,'26000001');assert.match(r.customer_id,/^\d{8}$/);assert.equal(count(raw,'customers'),1);}
// CASE 5 replay same ID.
{const {raw,env}=setup();const a=await resolveOrCreateCustomerIdentity(env,input(5),{year:2026});const b=await resolveOrCreateCustomerIdentity(env,input(5),{year:2026});assert.equal(a.customer_id,b.customer_id);assert.equal(count(raw,'customers'),1);}
// CASE 6 same LINE concurrent converges to one registry/customer.
{const {raw,env}=setup();const [a,b]=await Promise.all([resolveOrCreateCustomerIdentity(env,input(6),{year:2026}),resolveOrCreateCustomerIdentity(env,input(6),{year:2026})]);assert.equal(a.customer_id,b.customer_id);assert.equal(count(raw,'customer_identity_registry'),1);assert.equal(count(raw,'customers'),1);}
// CASE 7 same idempotency different LINE -> 409.
{const {env}=setup();await resolveOrCreateCustomerIdentity(env,input(7,'shared'),{year:2026});const r=await resolveOrCreateCustomerIdentity(env,input(8,'shared'),{year:2026});assert.equal(r.error,'identity_idempotency_conflict');assert.equal(r.statusCode,409);}
// CASE 8 duplicate existing LINE -> review.
{const {raw,env}=setup();raw.prepare(`INSERT INTO customers(customer_id,line_user_id) VALUES(?,?)`).run('26000020',uid(9));raw.prepare(`INSERT INTO customers(customer_id,line_user_id) VALUES(?,?)`).run('26000021',uid(9));const r=await resolveOrCreateCustomerIdentity(env,input(9),{year:2026});assert.equal(r.error,'duplicate_existing_line_identity');assert.equal(r.statusCode,409);}
// CASE 9 customer insert failure -> retry same assigned ID.
{const fail={customerInsertOnce:true};const {raw,env}=setup(fail);const a=await resolveOrCreateCustomerIdentity(env,input(10),{year:2026});assert.equal(a.customer_id,'26000001');const b=await resolveOrCreateCustomerIdentity(env,input(10),{year:2026});assert.equal(b.customer_id,'26000001');assert.equal(count(raw,'customers'),1);}
// CASE 10 year boundary retry preserves registry ID.
{const {env}=setup();const a=await resolveOrCreateCustomerIdentity(env,input(11),{year:2026});const b=await resolveOrCreateCustomerIdentity(env,input(11),{year:2027});assert.equal(a.customer_id,'26000001');assert.equal(b.customer_id,'26000001');}
// exhaustion fails closed and never emits 9 digits.
{const {raw,env}=setup();raw.prepare(`UPDATE customer_identity_sequence SET last_value=999999`).run();const r=await resolveOrCreateCustomerIdentity(env,input(12),{year:2026});assert.equal(r.error,'customer_identity_sequence_exhausted');assert.equal(count(raw,'customers'),0);}
// C-prefix existing data remains reusable but generator never creates C-prefix.
{const {env}=setup({},[{id:'C00000001',line:uid(13)}]);const r=await resolveOrCreateCustomerIdentity(env,input(13),{year:2026});assert.equal(r.customer_id,'C00000001');}
// no LINE reject + no-name display.
{const {env}=setup();const r=await resolveOrCreateCustomerIdentity(env,{customer_name:'name',idempotency_key:'x',source:'reservation-ai-line'},{year:2026});assert.equal(r.statusCode,400);}
{const {raw,env}=setup();const r=await resolveOrCreateCustomerIdentity(env,input(14),{year:2026});assert.equal(raw.prepare(`SELECT name FROM customers WHERE customer_id=?`).get(r.customer_id).name,'名称未設定');}
// auth preserved.
{const {env}=setup();const reqBody=JSON.stringify(input(15));const noAuth=await handleCustomerIdentityResolver(new Request('https://internal/api/internal/customer-identity/resolve-or-create',{method:'POST',headers:{'content-type':'application/json'},body:reqBody}),env);assert.equal(noAuth.status,401);}

console.log('customer identity YY+6digit format/allocator/resolver/auth PASS');
