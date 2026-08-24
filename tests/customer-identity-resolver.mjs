import fs from 'node:fs';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {resolveOrCreateCustomerIdentity,handleCustomerIdentityResolver,formatCanonicalCustomerId} from '../src/customer-identity-resolver.mjs';

const registryMigration=fs.readFileSync('migrations_managed/20260818_customer_identity_registry.sql','utf8');
const sequenceMigration=fs.readFileSync('migrations_managed/20260818_customer_identity_sequence.sql','utf8');
const uid=n=>'U'+String(n).padStart(32,'0');
function d1(db,fail={}){return{prepare(sql){let params=[];return{bind(...v){params=v;return this},async all(){return{results:db.prepare(sql).all(...params)}},async first(){return db.prepare(sql).get(...params)||null},async run(){if(fail.customerInsertOnce&&/INSERT INTO customers/i.test(sql)){fail.customerInsertOnce=false;throw new Error('injected')};return db.prepare(sql).run(...params)}}}}}
function setup(fail={},customers=[],registries=[]){const db=new DatabaseSync(':memory:');db.exec(`CREATE TABLE customers(customer_id TEXT PRIMARY KEY,name TEXT,line_display_name TEXT,line_user_id TEXT,acquisition_source TEXT,created_at TEXT,updated_at TEXT);`);for(const c of customers)db.prepare(`INSERT INTO customers(customer_id,name,line_display_name,line_user_id,acquisition_source) VALUES(?,?,?,?,?)`).run(c.id??null,c.name||'',c.display||null,c.line||null,'legacy');db.exec(registryMigration);for(const r of registries)db.prepare(`INSERT INTO customer_identity_registry(customer_id,line_user_id,idempotency_key,source,status,created_at,updated_at,raw_json) VALUES(?,?,?,?,?,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,?)`).run(r.customer_id??null,r.line_user_id,r.idempotency_key,r.source||'reservation-ai-line',r.status||'allocating',r.raw_json||'{}');db.exec(sequenceMigration);return{raw:db,env:{DB:d1(db,fail),CRM_INTERNAL_TOKEN:'test-secret'}}}
const count=(db,t)=>Number(db.prepare(`SELECT COUNT(*) n FROM ${t}`).get().n);
const input=(n,key='k'+n,extra={})=>({line_user_id:uid(n),idempotency_key:key,source:'reservation-ai-line',...extra});
const customerForLine=(db,n)=>db.prepare(`SELECT * FROM customers WHERE line_user_id=?`).get(uid(n));
const registryForLine=(db,n)=>db.prepare(`SELECT * FROM customer_identity_registry WHERE line_user_id=?`).get(uid(n));
const seq=(db)=>Number(db.prepare(`SELECT last_value FROM customer_identity_sequence WHERE sequence_key='canonical_customer_id'`).get().last_value);

assert.equal(formatCanonicalCustomerId(2026,1),'26000001');
assert.equal(formatCanonicalCustomerId(2026,126),'26000126');
assert.equal(formatCanonicalCustomerId(2027,126),'27000126');
assert.equal(formatCanonicalCustomerId(2026,999999),'26999999');
assert.throws(()=>formatCanonicalCustomerId(2026,1000000),/customer_identity_sequence_exhausted/);

// initialize from numeric max; preserve allocator contract.
{const {raw,env}=setup({},[{id:'26000126'}]);const r=await resolveOrCreateCustomerIdentity(env,input(1),{year:2026});assert.equal(r.customer_id,'26000127');assert.equal(seq(raw),127);}
// global six-digit sequence may not be reused across years even if the sequence row drifts backwards.
{const {raw,env}=setup({},[{id:'25000127'}]);raw.prepare(`UPDATE customer_identity_sequence SET last_value=126`).run();const r=await resolveOrCreateCustomerIdentity(env,input(2),{year:2026});assert.equal(r.customer_id,'26000128');assert.equal(seq(raw),128);}
// existing exact LINE customer is reused and a missing registry is reconciled without allocation.
{const {raw,env}=setup({},[{id:'26000126',line:uid(3),name:'Real Name'}]);const before=seq(raw);const r=await resolveOrCreateCustomerIdentity(env,input(3,'existing-registry-reconcile',{line_display_name:'LINE Nick'}),{year:2026});assert.equal(r.customer_id,'26000126');assert.equal(seq(raw),before);assert.equal(registryForLine(raw,3).customer_id,'26000126');assert.equal(raw.prepare(`SELECT name FROM customers WHERE customer_id='26000126'`).get().name,'Real Name');assert.equal(raw.prepare(`SELECT line_display_name FROM customers WHERE customer_id='26000126'`).get().line_display_name,'LINE Nick');}

// E1 existing customer customer_id NULL + registry missing -> allocate, update same row, create registry, resolved_existing.
{const {raw,env}=setup({},[{id:null,line:uid(101),name:'Existing Null'}]);const r=await resolveOrCreateCustomerIdentity(env,input(101,'e1',{line_display_name:'E1 LINE'}),{year:2026});assert.equal(r.ok,true);assert.equal(r.status,'resolved_existing');assert.equal(r.created,false);assert.equal(r.customer_id,'26000001');assert.match(r.customer_id,/^\d{8}$/);assert.equal(count(raw,'customers'),1);const c=customerForLine(raw,101);assert.equal(c.customer_id,'26000001');assert.equal(c.name,'Existing Null');assert.equal(c.line_display_name,'E1 LINE');const reg=registryForLine(raw,101);assert.equal(reg.customer_id,'26000001');assert.equal(reg.status,'active');}
// E2 existing customer customer_id empty + registry ID NULL -> allocate, update same row and registry to same ID.
{const {raw,env}=setup({},[{id:'',line:uid(102),name:'Existing Empty'}],[{customer_id:null,line_user_id:uid(102),idempotency_key:'e2'}]);const r=await resolveOrCreateCustomerIdentity(env,input(102,'e2',{line_display_name:'E2 LINE'}),{year:2026});assert.equal(r.ok,true);assert.equal(r.status,'resolved_existing');assert.equal(r.customer_id,'26000001');assert.equal(count(raw,'customers'),1);assert.equal(customerForLine(raw,102).customer_id,'26000001');assert.equal(registryForLine(raw,102).customer_id,'26000001');assert.equal(registryForLine(raw,102).status,'active');}
// E3 repeat E1 -> same ID, no second normal allocation, duplicate customer 0.
{const {raw,env}=setup({},[{id:null,line:uid(103),name:'Repeat'}]);const a=await resolveOrCreateCustomerIdentity(env,input(103,'e3a'),{year:2026});const afterFirstSeq=seq(raw);const b=await resolveOrCreateCustomerIdentity(env,input(103,'e3b'),{year:2026});assert.equal(a.customer_id,b.customer_id);assert.equal(seq(raw),afterFirstSeq);assert.equal(count(raw,'customers'),1);assert.equal(count(raw,'customer_identity_registry'),1);}
// E4 concurrent race -> callers converge to winner ID; no overwrite and no duplicate customer.
{const {raw,env}=setup({},[{id:null,line:uid(104),name:'Race'}]);const [a,b]=await Promise.all([resolveOrCreateCustomerIdentity(env,input(104,'e4a'),{year:2026}),resolveOrCreateCustomerIdentity(env,input(104,'e4b'),{year:2026})]);assert.equal(a.status,'resolved_existing');assert.equal(b.status,'resolved_existing');assert.equal(a.customer_id,b.customer_id);assert.equal(customerForLine(raw,104).customer_id,a.customer_id);assert.equal(registryForLine(raw,104).customer_id,a.customer_id);assert.equal(count(raw,'customers'),1);assert.equal(count(raw,'customer_identity_registry'),1);}
// E5 registry conflicting non-null ID -> fail closed; no customer overwrite.
{const {raw,env}=setup({},[{id:null,line:uid(105),name:'Conflict'}],[{customer_id:'26000999',line_user_id:uid(105),idempotency_key:'e5'}]);const r=await resolveOrCreateCustomerIdentity(env,input(105,'e5'),{year:2026});assert.equal(r.error,'identity_registry_mismatch');assert.equal(r.statusCode,409);assert.equal(customerForLine(raw,105).customer_id,null);assert.equal(registryForLine(raw,105).customer_id,'26000999');}
// E6 duplicate customer rows for same LINE -> review_required; auto-merge 0.
{const {raw,env}=setup({},[{id:null,line:uid(106),name:'Dupe A'},{id:'',line:uid(106),name:'Dupe B'}]);const r=await resolveOrCreateCustomerIdentity(env,input(106,'e6'),{year:2026});assert.equal(r.error,'duplicate_existing_line_identity');assert.equal(r.statusCode,409);assert.equal(r.review_required,true);assert.equal(count(raw,'customers'),2);}
// E7 existing canonical customer -> no allocation, behavior preserved.
{const {raw,env}=setup({},[{id:'26000126',line:uid(107),name:'Canonical'}]);const before=seq(raw);const r=await resolveOrCreateCustomerIdentity(env,input(107,'e7'),{year:2026});assert.equal(r.status,'resolved_existing');assert.equal(r.customer_id,'26000126');assert.equal(seq(raw),before);assert.equal(count(raw,'customers'),1);}
// E8 new LINE customer -> created with exact canonical 8 digits.
{const {raw,env}=setup();const r=await resolveOrCreateCustomerIdentity(env,input(108,'e8'),{year:2026});assert.equal(r.status,'created');assert.equal(r.created,true);assert.equal(r.customer_id,'26000001');assert.match(r.customer_id,/^\d{8}$/);assert.equal(count(raw,'customers'),1);}
// E9 same name but different formal LINE IDs -> separate identities.
{const {raw,env}=setup();const a=await resolveOrCreateCustomerIdentity(env,input(109,'e9a',{customer_name:'Same Name'}),{year:2026});const b=await resolveOrCreateCustomerIdentity(env,input(110,'e9b',{customer_name:'Same Name'}),{year:2026});assert.notEqual(a.customer_id,b.customer_id);assert.equal(count(raw,'customers'),2);}
// E10 same display name but different formal LINE IDs -> separate identities.
{const {raw,env}=setup();const a=await resolveOrCreateCustomerIdentity(env,input(111,'e10a',{line_display_name:'Same Display'}),{year:2026});const b=await resolveOrCreateCustomerIdentity(env,input(112,'e10b',{line_display_name:'Same Display'}),{year:2026});assert.notEqual(a.customer_id,b.customer_id);assert.equal(count(raw,'customers'),2);}

// new customer is numeric 8 digit and stores display name as metadata/fallback display name.
{const {raw,env}=setup();const r=await resolveOrCreateCustomerIdentity(env,input(4,'new-display',{line_display_name:'はな'}),{year:2026});assert.equal(r.customer_id,'26000001');assert.match(r.customer_id,/^\d{8}$/);const c=raw.prepare(`SELECT * FROM customers WHERE customer_id=?`).get(r.customer_id);assert.equal(c.name,'はな');assert.equal(c.line_display_name,'はな');}
// same LINE/idempotency replay converges.
{const {raw,env}=setup();const a=await resolveOrCreateCustomerIdentity(env,input(5),{year:2026});const b=await resolveOrCreateCustomerIdentity(env,input(5),{year:2026});assert.equal(a.customer_id,b.customer_id);assert.equal(count(raw,'customers'),1);assert.equal(count(raw,'customer_identity_registry'),1);}
// same LINE concurrent calls converge to one customer/registry.
{const {raw,env}=setup();const [a,b]=await Promise.all([resolveOrCreateCustomerIdentity(env,input(6),{year:2026}),resolveOrCreateCustomerIdentity(env,input(6),{year:2026})]);assert.equal(a.customer_id,b.customer_id);assert.equal(count(raw,'customer_identity_registry'),1);assert.equal(count(raw,'customers'),1);}
// same idempotency key cannot be reused for a different LINE identity.
{const {env}=setup();await resolveOrCreateCustomerIdentity(env,input(7,'shared'),{year:2026});const r=await resolveOrCreateCustomerIdentity(env,input(8,'shared'),{year:2026});assert.equal(r.error,'identity_idempotency_conflict');assert.equal(r.statusCode,409);}
// duplicate existing formal LINE identity fails closed.
{const {raw,env}=setup();raw.prepare(`INSERT INTO customers(customer_id,line_user_id) VALUES(?,?)`).run('26000020',uid(9));raw.prepare(`INSERT INTO customers(customer_id,line_user_id) VALUES(?,?)`).run('26000021',uid(9));const r=await resolveOrCreateCustomerIdentity(env,input(9),{year:2026});assert.equal(r.error,'duplicate_existing_line_identity');assert.equal(r.statusCode,409);assert.equal(r.review_required,true);}
// customer insert failure resumes the same allocated registry/customer ID.
{const fail={customerInsertOnce:true};const {raw,env}=setup(fail);const a=await resolveOrCreateCustomerIdentity(env,input(10),{year:2026});assert.equal(a.customer_id,'26000001');const b=await resolveOrCreateCustomerIdentity(env,input(10),{year:2026});assert.equal(b.customer_id,'26000001');assert.equal(count(raw,'customers'),1);}
// year boundary replay preserves previously allocated canonical ID.
{const {env}=setup();const a=await resolveOrCreateCustomerIdentity(env,input(11),{year:2026});const b=await resolveOrCreateCustomerIdentity(env,input(11),{year:2027});assert.equal(a.customer_id,'26000001');assert.equal(b.customer_id,'26000001');}
// exhaustion fails closed and never emits a 9 digit ID.
{const {raw,env}=setup();raw.prepare(`UPDATE customer_identity_sequence SET last_value=999999`).run();const r=await resolveOrCreateCustomerIdentity(env,input(12),{year:2026});assert.equal(r.error,'customer_identity_sequence_exhausted');assert.equal(count(raw,'customers'),0);}
// legacy C-prefix customer remains reusable, but the generator itself never emits C-prefix.
{const {env}=setup({},[{id:'C00000001',line:uid(13)}]);const r=await resolveOrCreateCustomerIdentity(env,input(13),{year:2026});assert.equal(r.customer_id,'C00000001');}
// invalid/missing LINE fails closed; unnamed customer fallback remains explicit.
{const {env}=setup();const r=await resolveOrCreateCustomerIdentity(env,{customer_name:'name',idempotency_key:'x',source:'reservation-ai-line'},{year:2026});assert.equal(r.statusCode,400);assert.equal(r.error,'invalid_line_user_id');}
{const {raw,env}=setup();const r=await resolveOrCreateCustomerIdentity(env,input(14),{year:2026});assert.equal(raw.prepare(`SELECT name FROM customers WHERE customer_id=?`).get(r.customer_id).name,'名称未設定');}
// auth is mandatory.
{const {env}=setup();const reqBody=JSON.stringify(input(15));const noAuth=await handleCustomerIdentityResolver(new Request('https://internal/api/internal/customer-identity/resolve-or-create',{method:'POST',headers:{'content-type':'application/json'},body:reqBody}),env);assert.equal(noAuth.status,401);}

console.log('customer identity YY+6digit global resolver / existing missing ID reconcile / metadata / registry PASS');
