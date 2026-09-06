import assert from 'node:assert/strict';
import fs from 'node:fs';
import { handleCustomer360Request, customer360ReadOnlyStatus } from '../src/crm-customer360-runtime.mjs';

const sqlSeen=[];
let writeMethodTouches=0;
const DB={
  prepare(sql){
    sqlSeen.push(String(sql));
    if(/\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE)\b/i.test(String(sql)))throw new Error('STATUS_MUST_NOT_MUTATE_D1');
    return {
      async first(){return {ok:1}},
      async all(){throw new Error('STATUS_MUST_NOT_ENUMERATE_D1')},
      async run(){writeMethodTouches++;throw new Error('STATUS_MUST_NOT_RUN_D1_WRITE')}
    };
  }
};

const env={
  CRM_LOCAL_TEST_AUTH:'1',
  DB,
  RESERVATION_SERVICE:{},
  LINE_SERVICE:{}
};

const direct=await customer360ReadOnlyStatus(env);
assert.equal(direct.ok,true);
assert.equal(direct.read_only,true);
assert.equal(direct.bindings.DB,true);
assert.equal(direct.bindings.RESERVATION_SERVICE,true);
assert.equal(direct.bindings.LINE_SERVICE,true);
assert.equal(direct.customer360_status_read_only,true);
assert.equal(direct.d1_probe,'SELECT 1');
assert.equal(direct.d1_write,false);
assert.equal(direct.schema_repair,false);
assert.equal(direct.customer_write,false);
assert.equal(direct.customer_id_generation,false);
assert.equal(direct.line_send,false);
assert.deepEqual(sqlSeen,['SELECT 1 AS ok']);
assert.equal(writeMethodTouches,0);

sqlSeen.length=0;
const response=await handleCustomer360Request(new Request('https://example.test/api/customer360/status'),env);
assert.equal(response.status,200);
const body=await response.json();
assert.equal(body.ok,true);
assert.equal(body.read_only,true);
assert.equal(body.bindings.DB,true);
assert.equal(body.d1_write,false);
assert.equal(body.schema_repair,false);
assert.equal(body.customer_write,false);
assert.equal(body.customer_id_generation,false);
assert.equal(body.line_send,false);
assert.deepEqual(sqlSeen,['SELECT 1 AS ok']);
assert.equal(writeMethodTouches,0);

let unauthorizedDbTouched=0;
const unauthorizedEnv={
  DB:{
    prepare(){unauthorizedDbTouched++;throw new Error('UNAUTHORIZED_STATUS_MUST_NOT_TOUCH_DB')}
  }
};
const unauthorized=await handleCustomer360Request(new Request('https://example.test/api/customer360/status'),unauthorizedEnv);
assert.equal(unauthorized.status,401);
assert.equal((await unauthorized.json()).error,'authentication_required');
assert.equal(unauthorizedDbTouched,0);

const runtimeSrc=fs.readFileSync('src/crm-customer360-runtime.mjs','utf8');
const ownerSrc=fs.readFileSync('src/crm-mobile-owner-interaction-recovery.mjs','utf8');
const start=runtimeSrc.indexOf('export async function customer360ReadOnlyStatus');
const end=runtimeSrc.indexOf('export async function customerListData',start);
assert.ok(start>=0&&end>start,'read-only status function missing');
const statusSrc=runtimeSrc.slice(start,end);
assert.ok(statusSrc.includes("'SELECT 1 AS ok'"));
assert.doesNotMatch(statusSrc,/\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|REPLACE)\b/i);
assert.doesNotMatch(statusSrc,/\.run\s*\(/);
assert.ok(ownerSrc.includes("fetch('/api/customer360/status'"));
assert.ok(!ownerSrc.includes("fetch('/api/crm-health-check'"));

console.log('CUSTOMER360_READ_ONLY_STATUS=PASS');
console.log('OWNER_STATUS_D1_WRITE=0');
console.log('OWNER_STATUS_SCHEMA_REPAIR=0');
console.log('OWNER_STATUS_CUSTOMER_WRITE=0');
console.log('OWNER_STATUS_CUSTOMER_ID_GENERATION=0');
console.log('OWNER_STATUS_LINE_SEND=0');
console.log('LEGACY_MUTATING_HEALTH_REFERENCE=0');
