import fs from 'node:fs';
import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {resolveOrCreateCustomerIdentity,handleCustomerIdentityResolver,formatCanonicalCustomerId} from '../src/customer-identity-resolver.mjs';

const migration=fs.readFileSync('migrations_managed/20260818_customer_identity_registry.sql','utf8');
const uid=n=>'U'+String(n).padStart(32,'0');

function d1(db,fail={}){
  return {prepare(sql){
    let params=[];
    return {
      bind(...v){params=v;return this;},
      async all(){return {results:db.prepare(sql).all(...params)};},
      async first(){return db.prepare(sql).get(...params)||null;},
      async run(){
        if(fail.customerInsertOnce && /INSERT INTO customers/i.test(sql)){fail.customerInsertOnce=false;throw new Error('injected customer insert failure');}
        return db.prepare(sql).run(...params);
      }
    };
  }};
}
function setup(fail={}){
  const db=new DatabaseSync(':memory:');
  db.exec(`CREATE TABLE customers(
    customer_id TEXT PRIMARY KEY,
    name TEXT,
    line_user_id TEXT,
    acquisition_source TEXT,
    created_at TEXT,
    updated_at TEXT
  );`);
  db.exec(migration);
  return {raw:db,env:{DB:d1(db,fail),CRM_INTERNAL_TOKEN:'test-secret'}};
}
function count(db,table){return Number(db.prepare(`SELECT COUNT(*) n FROM ${table}`).get().n);}

assert.equal(formatCanonicalCustomerId(1),'C00000001');
assert.equal(formatCanonicalCustomerId(284),'C00000284');
assert.equal(formatCanonicalCustomerId(100000000),'C100000000');

// CASE 1 + 11: existing LINE exact lookup reuses legacy numeric ID without insert.
{
  const {raw,env}=setup();
  raw.prepare(`INSERT INTO customers(customer_id,name,line_user_id,acquisition_source) VALUES(?,?,?,?)`).run('26012345','既存',uid(1),'legacy');
  const before=count(raw,'customers');
  const r=await resolveOrCreateCustomerIdentity(env,{line_user_id:uid(1),idempotency_key:'k1',source:'reservation-ai-line',customer_name:'別名'});
  assert.equal(r.ok,true); assert.equal(r.customer_id,'26012345'); assert.equal(r.created,false); assert.equal(count(raw,'customers'),before);
  assert.equal(count(raw,'customer_identity_registry'),0);
}

// CASE 2: new LINE allocates global registry id and creates C-ID customer.
{
  const {raw,env}=setup();
  const r=await resolveOrCreateCustomerIdentity(env,{line_user_id:uid(2),idempotency_key:'k2',source:'reservation-ai-line',customer_name:'新規'});
  assert.equal(r.status,'created'); assert.equal(r.customer_id,'C00000001'); assert.equal(count(raw,'customer_identity_registry'),1); assert.equal(count(raw,'customers'),1);
}

// CASE 3: exact replay returns same ID with no duplicate.
{
  const {raw,env}=setup();
  const input={line_user_id:uid(3),idempotency_key:'k3',source:'reservation-ai-line',customer_name:'新規'};
  const a=await resolveOrCreateCustomerIdentity(env,input); const b=await resolveOrCreateCustomerIdentity(env,input);
  assert.equal(a.customer_id,b.customer_id); assert.equal(count(raw,'customer_identity_registry'),1); assert.equal(count(raw,'customers'),1); assert.equal(b.created,false);
}

// CASE 4: concurrent same LINE/request converges to one registry/customer.
{
  const {raw,env}=setup();
  const input={line_user_id:uid(4),idempotency_key:'k4',source:'reservation-ai-line'};
  const [a,b]=await Promise.all([resolveOrCreateCustomerIdentity(env,input),resolveOrCreateCustomerIdentity(env,input)]);
  assert.equal(a.customer_id,b.customer_id); assert.equal(count(raw,'customer_identity_registry'),1); assert.equal(count(raw,'customers'),1);
}

// CASE 5: idempotency key cannot cross LINE identities.
{
  const {env}=setup();
  await resolveOrCreateCustomerIdentity(env,{line_user_id:uid(5),idempotency_key:'shared',source:'reservation-ai-line'});
  const r=await resolveOrCreateCustomerIdentity(env,{line_user_id:uid(6),idempotency_key:'shared',source:'reservation-ai-line'});
  assert.equal(r.ok,false); assert.equal(r.error,'identity_idempotency_conflict'); assert.equal(r.statusCode,409); assert.equal(r.review_required,true);
}

// CASE 6: duplicate existing customers for one LINE fail closed.
{
  const {raw,env}=setup();
  raw.prepare(`INSERT INTO customers(customer_id,line_user_id,name) VALUES(?,?,?)`).run('26000001',uid(7),'A');
  raw.prepare(`INSERT INTO customers(customer_id,line_user_id,name) VALUES(?,?,?)`).run('26000002',uid(7),'B');
  const r=await resolveOrCreateCustomerIdentity(env,{line_user_id:uid(7),idempotency_key:'k7',source:'reservation-ai-line'});
  assert.equal(r.error,'duplicate_existing_line_identity'); assert.equal(r.statusCode,409); assert.equal(count(raw,'customer_identity_registry'),0);
}

// CASE 7: name-only/no LINE is rejected.
{
  const {env}=setup();
  const r=await resolveOrCreateCustomerIdentity(env,{customer_name:'名前だけ',idempotency_key:'k8',source:'reservation-ai-line'});
  assert.equal(r.error,'invalid_line_user_id'); assert.equal(r.statusCode,400);
}

// CASE 8: name is optional and does not participate in matching.
{
  const {raw,env}=setup();
  const r=await resolveOrCreateCustomerIdentity(env,{line_user_id:uid(8),idempotency_key:'k9',source:'reservation-ai-line'});
  assert.equal(r.status,'created');
  const row=raw.prepare(`SELECT name FROM customers WHERE customer_id=?`).get(r.customer_id);
  assert.equal(row.name,'名称未設定');
}

// CASE 9: failure after registry allocation reuses the same canonical ID on retry.
{
  const fail={customerInsertOnce:true}; const {raw,env}=setup(fail); const input={line_user_id:uid(9),idempotency_key:'k10',source:'reservation-ai-line'};
  const first=await resolveOrCreateCustomerIdentity(env,input);
  assert.equal(first.error,'customer_create_failed'); assert.equal(first.customer_id,'C00000001'); assert.equal(count(raw,'customer_identity_registry'),1); assert.equal(count(raw,'customers'),0);
  const second=await resolveOrCreateCustomerIdentity(env,input);
  assert.equal(second.customer_id,'C00000001'); assert.equal(second.status,'created'); assert.equal(count(raw,'customer_identity_registry'),1); assert.equal(count(raw,'customers'),1);
}

// CASE 10: pre-existing C-ID collision with another LINE fails closed.
{
  const {raw,env}=setup();
  raw.prepare(`INSERT INTO customers(customer_id,line_user_id,name) VALUES(?,?,?)`).run('C00000001',uid(10),'collision');
  const r=await resolveOrCreateCustomerIdentity(env,{line_user_id:uid(11),idempotency_key:'k11',source:'reservation-ai-line'});
  assert.equal(r.error,'identity_customer_id_collision'); assert.equal(r.statusCode,409); assert.equal(count(raw,'customers'),1);
}

// CASE 12: alphanumeric canonical ID survives exact customer lookup as TEXT.
{
  const {raw,env}=setup();
  const created=await resolveOrCreateCustomerIdentity(env,{line_user_id:uid(12),idempotency_key:'k12',source:'reservation-ai-line'});
  assert.match(created.customer_id,/^C\d{8,}$/);
  const stored=raw.prepare(`SELECT customer_id FROM customers WHERE customer_id=?`).get(created.customer_id);
  assert.equal(stored.customer_id,created.customer_id);
}

// Internal endpoint is secret-only and never accepts an unauthenticated browser call.
{
  const {env}=setup();
  const noAuth=await handleCustomerIdentityResolver(new Request('https://internal/api/internal/customer-identity/resolve-or-create',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({line_user_id:uid(13),idempotency_key:'k13',source:'reservation-ai-line'})}),env);
  assert.equal(noAuth.status,401);
  const authed=await handleCustomerIdentityResolver(new Request('https://internal/api/internal/customer-identity/resolve-or-create',{method:'POST',headers:{'content-type':'application/json','x-internal-token':'test-secret'},body:JSON.stringify({line_user_id:uid(13),idempotency_key:'k13',source:'reservation-ai-line'})}),env);
  assert.equal(authed.status,200);
}

console.log('customer identity resolver CASE 1-12 + auth PASS');
