import assert from 'node:assert/strict';
import {DatabaseSync} from 'node:sqlite';
import {readFileSync} from 'node:fs';
import {planRes15ServiceSyntheticMutation,RES15_SYNTHETIC_ATOMIC_SCOPES} from '../src/crm-res15-synthetic-revision-adapter.mjs';
const entry=readFileSync('src/production-index-crm-customer360-entry.js','utf8');
assert.equal(entry.includes('crm-res15-synthetic-revision-adapter.mjs'),false,'entrypoint must not import synthetic planner');
const raw=new DatabaseSync(':memory:');
const cases=[
 ['crm_customer','customers','customer_id','TEXT PRIMARY KEY','line_user_id TEXT,deleted_at TEXT','26001234',{line_user_id:'U'+('a'.repeat(32))}],
 ['crm_registry','customer_identity_registry','line_user_id','TEXT PRIMARY KEY','customer_id TEXT,status TEXT','U'+('b'.repeat(32)),{customer_id:'26001234',status:'active'}]
];
const context={synthetic_only:true,environment:'isolated_sqlite',remote:false};
const plan=(scope,entity_key,revision,mutation_id,changes,options=context)=>planRes15ServiceSyntheticMutation(
 {scope,entity_key,expected_revision:revision,mutation_id,changes},options);
const exec=(v)=>raw.prepare(v.sql).all(...v.bindings);
const row=(table,key,value)=>raw.prepare('SELECT * FROM '+table+' WHERE '+key+'=?').get(value);
assert.equal(RES15_SYNTHETIC_ATOMIC_SCOPES.length,2);
for(const [scope,table,key,keyType,columns,value,changes] of cases){
 raw.exec('CREATE TABLE '+table+' ('+key+' '+keyType+','+columns+
  ',res15_revision INTEGER NOT NULL,res15_last_mutation_id TEXT,res15_payload_sha256 TEXT)');
 raw.prepare('INSERT INTO '+table+' ('+key+',res15_revision) VALUES (?,?)').run(value,3);
 const req=await plan(scope,value,3,'mutation-'+scope,changes);
 assert.equal(req.ok,true,scope);
 assert.equal(req.production_authorized,false);
 assert.equal(req.service_wired,false);
 assert.equal(req.complete_writer_coverage,false);
 assert.equal(req.multi_row_atomic,false);
 assert.equal(exec(req).length,1);
 assert.equal(row(table,key,value).res15_revision,4);
 assert.equal(exec(req).length,0,'idempotent retry cannot advance');
 const current=await plan(scope,value,4,'mutation-'+scope,changes);
 assert.equal(exec(current).length,0,'current-revision mutation-ID replay must not advance');
 assert.equal(row(table,key,value).res15_revision,4);
 const other=await plan(scope,value,4,'new-'+scope,changes);
 assert.equal(exec(other).length,1);
 assert.equal(row(table,key,value).res15_revision,5);
 assert.equal(exec(req).length,0,'ABA stale cannot advance');
 // Capture a mutable caller value before crypto awaits.
 const later={...changes},first=Object.keys(later)[0],original=later[first];
 const future=plan(scope,value,5,'snapshot-'+scope,later);
 later[first]='post-hash-tamper';
 const captured=await future;
 assert.equal(exec(captured).length,1);
 assert.equal(row(table,key,value)[first],original);
 assert.equal(row(table,key,value).res15_revision,6);
 const forged=await plan(scope,value,6,'missing-row-'+scope,changes);
 assert.equal(exec(forged).length,1);
 assert.equal(row(table,key,value).res15_revision,7);
 const nonexistent=await plan(scope,scope==='crm_registry'?'U'+('c'.repeat(32)):'26009999',7,'missing-target-'+scope,changes);
 assert.equal(exec(nonexistent).length,0,'zero-row must not advance');
}
const first=cases[0],target=first[6],base=await plan(first[0],first[5],7,'simultaneous',target);
const competitor=await plan(first[0],first[5],7,'simultaneous-other',target);
assert.equal(exec(base).length,1);
assert.equal(exec(competitor).length,0,'same revision contenders: one winner');
assert.equal(row(first[1],first[2],first[5]).res15_revision,8);
// Single UPDATE must atomically roll back the material field and revision.
raw.exec("CREATE TRIGGER abort_res15 BEFORE UPDATE ON "+first[1]+
  " WHEN NEW.res15_last_mutation_id='abort-test' BEGIN SELECT RAISE(ABORT,'synthetic injected error'); END");
const failed=await plan(first[0],first[5],8,'abort-test',target);
const before=row(first[1],first[2],first[5]);
assert.throws(()=>exec(failed),/synthetic injected error/);
const after=row(first[1],first[2],first[5]);
assert.deepEqual(after,before);
let negative=0;
for(const wrong of [
 {scope:'unexpected',entity_key:first[5],expected_revision:8,mutation_id:'bad',changes:target},
 {scope:first[0],entity_key:first[5],expected_revision:0,mutation_id:'bad',changes:target},
 {scope:first[0],entity_key:first[5],expected_revision:8,mutation_id:'bad',changes:{sql_injection:'X'}},
 {scope:first[0],entity_key:first[5],expected_revision:8,mutation_id:'bad',changes:{}},
 {scope:first[0],entity_key:first[5],expected_revision:8,mutation_id:'bad',changes:{get foo(){throw Error('accessor')}}},
 {scope:first[0],entity_key:first[5],expected_revision:8,mutation_id:'bad\nline',changes:target}
 ]){
 const response=await planRes15ServiceSyntheticMutation(wrong,context);
 assert.equal(response.ok,false);negative++;
}
for(const opt of [{},{synthetic_only:true,environment:'production',remote:false},{synthetic_only:true,environment:'isolated_sqlite',remote:true}]){
 const blocked=await plan(first[0],first[5],8,'bad-context',target,opt);
 assert.equal(blocked.ok,false);negative++;
}
raw.close();
console.log('RES15_CRM_SERVICE_OWNER_SYNTHETIC_PLAN_CAS_ROLLBACK_ABA=PASS');
console.log('RES15_CRM_SYNTHETIC_NEGATIVE_CHECKS='+negative);
console.log('PRODUCTION_DEPLOY=0 SCHEMA_APPLY=0 REMOTE_D1=0 LINE_SEND=0 LIVE_CUSTOMER_WRITE=0');
