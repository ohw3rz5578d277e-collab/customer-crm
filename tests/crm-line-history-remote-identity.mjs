import assert from 'node:assert/strict';
import fs from 'node:fs';
import {DatabaseSync} from 'node:sqlite';
import {
  verifyRemoteLineIdentityOwnership,
  verifyRemoteLineHistoryAtDelivery,
  validateRemoteLineHistoryPayload
} from '../src/crm-line-history-exact-customer-read.mjs';

const source = fs.readFileSync('src/index.js', 'utf8');
const remoteStart = source.indexOf('async function fetchRemoteLineHistory(');
const remoteEnd = source.indexOf('async function getCustomerLineHistory(', remoteStart);
assert.ok(remoteStart >= 0 && remoteEnd > remoteStart, 'remote LINE history path missing');
const remote = source.slice(remoteStart, remoteEnd);
assert.match(remote, /verifyRemoteLineIdentityOwnership\(env\.DB, customer\)/, 'remote LINE lookup lacks unique ownership gate');
assert.match(remote, /if \(!ownership\.allowed\)/, 'remote LINE lookup does not reject ambiguous owners');
assert.equal((remote.match(/validateRemoteLineHistoryPayload\(data, lineUserId\)/g) || []).length, 2, 'both Worker routes must validate response identities');
assert.equal((remote.match(/normalizeLineHistoryMessages\(verified\.items\)/g) || []).length, 2, 'remote messages must come from verified payload');
assert.match(remote, /remote_line_identity_unverified/, 'identity mismatch must fail closed');
const readStart = source.indexOf('async function getCustomerLineHistory(');
const detailStart = source.indexOf('async function getCustomerDetail(',readStart);
assert.ok(readStart === remoteEnd && detailStart > readStart,'customer history delivery path missing');
const deliverySource = source.slice(readStart,detailStart);
const fetchIndex = deliverySource.indexOf('await fetchRemoteLineHistory(env, customer)');
const postCheckIndex = deliverySource.indexOf('await verifyRemoteLineHistoryAtDelivery(env.DB, customer, remote)');
const composeIndex = deliverySource.indexOf('const combined = normalizeLineHistoryMessages(');
assert.ok(fetchIndex >= 0 && postCheckIndex > fetchIndex && composeIndex > postCheckIndex,'remote identity must be rechecked after fetch and before returning history');
assert.match(remote,/attempted:\s*true/,'remote attempts must be tracked independently of connection success');
assert.match(deliverySource.slice(postCheckIndex,composeIndex),/if \(!delivery.allowed\)\s*\{[\s\S]*?ok:\s*false[\s\S]*?messages:\s*\[\]/,'failed ownership recheck must return zero messages');

const db = new DatabaseSync(':memory:');
db.exec('CREATE TABLE customers(customer_id TEXT PRIMARY KEY, line_user_id TEXT)');
const insert = db.prepare('INSERT INTO customers(customer_id,line_user_id) VALUES(?,?)');
const lineA = 'U' + 'a'.repeat(32);
const lineB = 'U' + 'b'.repeat(32);
const lineC = 'U' + 'c'.repeat(32);
insert.run('26000101', lineA);
insert.run('26000102', lineB);
insert.run('26000103', lineC);

let reads = 0;
const d1 = {prepare(sql) {
  reads++;
  assert.equal(sql, 'SELECT customer_id FROM customers WHERE line_user_id = ? LIMIT 2');
  return {bind(id) {
    assert.equal(typeof id, 'string');
    return {all:async()=>({results:db.prepare(sql).all(id)})};
  }};
}};
const ownerA = {customer_id:'26000101',line_user_id:lineA};
assert.equal((await verifyRemoteLineIdentityOwnership(d1, ownerA)).allowed, true);
assert.equal((await verifyRemoteLineIdentityOwnership(d1, {customer_id:'26000102',line_user_id:lineA})).allowed, false, 'different customer must not read LINE A');
assert.equal((await verifyRemoteLineIdentityOwnership(d1, {customer_id:'26000999',line_user_id:lineA})).allowed, false, 'unknown customer must not read LINE A');
const beforeInvalid = reads;
for (const bad of [
  {customer_id:'CRM-26000101',line_user_id:lineA},
  {customer_id:'26000101',line_user_id:'26000101'},
  {customer_id:'26000101',line_user_id:'Unothex'},
  {customer_id:26000101,line_user_id:lineA},
  null
]) assert.equal((await verifyRemoteLineIdentityOwnership(d1,bad)).allowed, false);
assert.equal(reads,beforeInvalid,'malformed identity must not query D1');
insert.run('26000104',lineA);
const ambiguous = await verifyRemoteLineIdentityOwnership(d1,ownerA);
assert.equal(ambiguous.allowed,false,'duplicate LINE identity must block remote history');
assert.equal(ambiguous.reason,'remote_line_identity_ambiguous');
assert.equal((await verifyRemoteLineIdentityOwnership({prepare(){throw Error('synthetic-db-error')}},ownerA)).allowed,false,'database errors must fail closed');
assert.equal((await verifyRemoteLineIdentityOwnership({prepare(){return{bind(){return{all:async()=>({})}}}}},ownerA)).allowed,false,'invalid database payload must fail closed');
assert.equal(db.prepare('SELECT COUNT(*) AS n FROM customers').get().n,4,'read checks mutated customers');

const inbound={line_user_id:lineB,user_id:lineB,message_text:'synthetic inbound'};
const outbound={line_user_id:lineB,user_id:lineB,direction:'outbound',message_text:'synthetic outbound'};
const valid={ok:true,line_user_id:lineB,contact:{user_id:lineB},items:[inbound,outbound]};
const result=validateRemoteLineHistoryPayload(valid,lineB);
assert.equal(result.valid,true);
assert.deepEqual(result.items,[inbound,outbound]);
assert.equal(validateRemoteLineHistoryPayload({ok:true,line_user_id:lineB,items:[]},lineB).valid,true,'empty valid roster must not fail');
assert.equal(validateRemoteLineHistoryPayload({...valid,line_user_id:lineA},lineB).valid,false,'other LINE user envelope leaked');
assert.equal(validateRemoteLineHistoryPayload({...valid,contact:{user_id:lineA}},lineB).valid,false,'other LINE user contact leaked');
assert.equal(validateRemoteLineHistoryPayload({...valid,items:[inbound,{...outbound,line_user_id:lineA}]},lineB).valid,false,'mixed remote conversation leaked');
assert.equal(validateRemoteLineHistoryPayload({...valid,items:[{...inbound,user_id:lineA}]},lineB).valid,false,'conflicting user_id leaked');
assert.equal(validateRemoteLineHistoryPayload({...valid,items:[{message_text:'missing identity'}]},lineB).valid,false,'unattributed remote message leaked');
assert.equal(validateRemoteLineHistoryPayload({...valid,items:{}},lineB).valid,false,'invalid remote array was accepted');
assert.equal(validateRemoteLineHistoryPayload({ok:true,line_user_id:lineB},lineB).valid,false,'missing messages array was accepted');
assert.equal(validateRemoteLineHistoryPayload({...valid,messages:[{...inbound,line_user_id:lineA}]},lineB).valid,false,'unselected messages field bypassed validation');
assert.equal(validateRemoteLineHistoryPayload({...valid,ok:false},lineB).valid,false);
assert.equal(validateRemoteLineHistoryPayload({...valid,contact:{name:'not an identity'}},lineB).valid,false);
assert.equal(validateRemoteLineHistoryPayload({...valid},'bad-line-id').valid,false);
// No live CRM reads: emulate a real linkage change while a remote response is delayed.
const stableBefore=await verifyRemoteLineIdentityOwnership(d1,ownerA);
assert.equal(stableBefore.allowed,false,'previous duplicate fixture must still be ambiguous');
const ownerB={customer_id:'26000102',line_user_id:lineB};
assert.equal((await verifyRemoteLineHistoryAtDelivery(d1,ownerB,{connected:true,messages:[inbound]})).allowed,true);
const beforeNoRemote=reads;
assert.deepEqual(await verifyRemoteLineHistoryAtDelivery(d1,ownerB,{connected:false,messages:[]}),{allowed:true,remoteHistoryAvailable:false});
assert.equal(reads,beforeNoRemote,'no remote attempt must not force extra DB reads');
assert.deepEqual(await verifyRemoteLineHistoryAtDelivery(d1,ownerB,{attempted:true,connected:false,messages:[]}),{allowed:true,remoteHistoryAvailable:false},'failed remote fetch must recheck a stable owner');
assert.equal(reads,beforeNoRemote+1,'failed attempted remote fetch must recheck ownership');

async function delayedResponseOwnershipCheck(customer,duringNetwork) {
  const initial=await verifyRemoteLineIdentityOwnership(d1,customer);
  assert.equal(initial.allowed,true,'the request must start with a valid identity');
  let release;
  const pending=new Promise(resolve=>{release=resolve});
  const request=(async()=>{
    const response=await pending;
    const delivery=await verifyRemoteLineHistoryAtDelivery(d1,customer,response);
    return {delivery,messages:delivery.allowed?response.messages:[]};
  })();
  duringNetwork();
  release({connected:true,messages:[{line_user_id:customer.line_user_id,message_text:'synthetic delayed history'}]});
  return request;
}

// A failed remote lookup must not expose local history after unlink during the request.
const beforeFailedAttempt=await verifyRemoteLineIdentityOwnership(d1,ownerB);
assert.equal(beforeFailedAttempt.allowed,true);
db.prepare('UPDATE customers SET line_user_id=NULL WHERE customer_id=?').run('26000102');
const failedAttempt=await verifyRemoteLineHistoryAtDelivery(d1,ownerB,{attempted:true,connected:false,messages:[]});
assert.equal(failedAttempt.allowed,false,'failed Worker fetch must not bypass delivery ownership check');
db.prepare('UPDATE customers SET line_user_id=? WHERE customer_id=?').run(lineB,'26000102');

// A formerly valid owner becomes unlinked before remote fetch completes.
const unlinked=await delayedResponseOwnershipCheck(ownerB,()=>{
  db.prepare('UPDATE customers SET line_user_id=NULL WHERE customer_id=?').run('26000102');
});
assert.equal(unlinked.delivery.allowed,false,'unlink during fetch must invalidate the response');
assert.equal(unlinked.delivery.reason,'remote_line_identity_changed_during_fetch');
assert.deepEqual(unlinked.messages,[],'stale remote messages must not be exposed');

// Reassignment and duplicate links must also invalidate an in-flight response.
insert.run('26000105',lineB);
const changed=await delayedResponseOwnershipCheck({customer_id:'26000105',line_user_id:lineB},()=>{
  db.prepare('UPDATE customers SET line_user_id=? WHERE customer_id=?').run(lineA,'26000105');
});
assert.equal(changed.delivery.allowed,false);
assert.deepEqual(changed.messages,[]);
db.prepare('UPDATE customers SET line_user_id=? WHERE customer_id=?').run(lineB,'26000105');
const duplicate=await delayedResponseOwnershipCheck({customer_id:'26000105',line_user_id:lineB},()=>{
  db.prepare('UPDATE customers SET line_user_id=? WHERE customer_id=?').run(lineB,'26000103');
});
assert.equal(duplicate.delivery.allowed,false,'second CRM customer linked during fetch must block');
assert.deepEqual(duplicate.messages,[]);
assert.equal((await verifyRemoteLineHistoryAtDelivery({prepare(){throw Error('synthetic read failure')}},ownerB,{connected:true,messages:[inbound]})).allowed,false);

// Exercise the actual source-level fetch + delivery functions, not only the
// delivery helper. All transports and local messages are synthetic.
const runtime = new Function(
  'getLocalCustomerLineHistory', 'verifyRemoteLineIdentityOwnership',
  'verifyRemoteLineHistoryAtDelivery', 'validateRemoteLineHistoryPayload',
  'normalizeLineHistoryMessages', 'text', 'getAdminToken',
  'DEFAULT_LINE_WORKER_BASE', 'fetch', 'Request', 'URL',
  remote + '\n' + deliverySource + '\nreturn getCustomerLineHistory;'
);
const syntheticLocal = [{customer_id:ownerB.customer_id,line_user_id:lineB,message_text:'synthetic local only'}];
function createHistoryRunner(transport) {
  return runtime(
    async()=>syntheticLocal,
    verifyRemoteLineIdentityOwnership,
    verifyRemoteLineHistoryAtDelivery,
    validateRemoteLineHistoryPayload,
    items=>items,
    value=>String(value??'').trim(),
    ()=>'synthetic-admin',
    'https://synthetic-worker.invalid',
    transport.https,
    Request, URL
  );
}
function resetOwnerB() {
  db.prepare('UPDATE customers SET line_user_id=? WHERE customer_id=?').run(lineB,ownerB.customer_id);
  db.prepare('UPDATE customers SET line_user_id=? WHERE customer_id=?').run(lineC,'26000103');
  db.prepare('UPDATE customers SET line_user_id=? WHERE customer_id=?').run(lineA,'26000105');
}
function deferred() {
  let release;
  const promise=new Promise(resolve=>{release=resolve});
  return {promise,release};
}
const failedResponse={ok:false,status:403,text:async()=>JSON.stringify({ok:false,message:'synthetic rejected'})};
resetOwnerB();
// Service Binding rejects after unlink; HTTPS also fails. Even the previously
// loaded local row must not be returned to the now-unlinked identity.
{
  const gate=deferred();
  const run=createHistoryRunner({https:async()=>failedResponse});
  const pending=run({
    DB:d1,LINE_SERVICE:{fetch:async()=>gate.promise},
    LINE_HISTORY_API_BASE:'https://synthetic-worker.invalid'
  },ownerB);
  db.prepare('UPDATE customers SET line_user_id=NULL WHERE customer_id=?').run(ownerB.customer_id);
  gate.release(failedResponse);
  const result=await pending;
  assert.equal(result.ok,false,'failed Service Binding + unlink must fail closed');
  assert.deepEqual(result.messages,[],'failed Service Binding leaked local history');
  assert.equal(result.line_user_id,'','failed Service Binding leaked stale LINE identity');
}
resetOwnerB();
// HTTPS fallback rejects after reassignment, with no Service Binding configured.
{
  const gate=deferred();
  const run=createHistoryRunner({https:async()=>gate.promise});
  const pending=run({DB:d1,LINE_HISTORY_API_BASE:'https://synthetic-worker.invalid'},ownerB);
  db.prepare('UPDATE customers SET line_user_id=NULL WHERE customer_id=?').run(ownerB.customer_id);
  gate.release(failedResponse);
  const result=await pending;
  assert.equal(result.ok,false,'failed HTTPS fetch + unlink must fail closed');
  assert.deepEqual(result.messages,[],'failed HTTPS fetch leaked local history');
}
resetOwnerB();
// With an unchanged owner, an unsuccessful remote fetch may still deliver only
// the exact-customer local messages.
{
  const run=createHistoryRunner({https:async()=>failedResponse});
  const result=await run({DB:d1,LINE_HISTORY_API_BASE:'https://synthetic-worker.invalid'},ownerB);
  assert.equal(result.ok,true);
  assert.equal(result.connected,false);
  assert.deepEqual(result.messages,syntheticLocal);
}
console.log('CRM_REMOTE_LINE_FAILED_SERVICE_DELIVERY=BLOCKED');
console.log('CRM_REMOTE_LINE_FAILED_HTTPS_DELIVERY=BLOCKED');
console.log('CRM_REMOTE_LINE_STABLE_OWNER_LOCAL_ONLY=PASS');

db.close();
console.log('CRM_REMOTE_LINE_IDENTITY_MID_FETCH_UNLINK=BLOCKED');
console.log('CRM_REMOTE_LINE_IDENTITY_MID_FETCH_RELINK=BLOCKED');
console.log('CRM_REMOTE_LINE_IDENTITY_MID_FETCH_DUPLICATE=BLOCKED');
console.log('CRM_REMOTE_LINE_IDENTITY_OWNER_GATE=PASS');
console.log('CRM_REMOTE_LINE_DUPLICATE_LINK=BLOCKED');
console.log('CRM_REMOTE_LINE_WORKER_ENVELOPE_AND_ROWS=VERIFIED');
console.log('CRM_REMOTE_LINE_UNATTRIBUTED_MESSAGE=BLOCKED');
console.log('CUSTOMER_MUTATIONS=0');
console.log('PRODUCTION_REQUESTS=0');
