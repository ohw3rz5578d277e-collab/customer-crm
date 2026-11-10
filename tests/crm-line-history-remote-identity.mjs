import assert from 'node:assert/strict';
import fs from 'node:fs';
import {DatabaseSync} from 'node:sqlite';
import {
  verifyRemoteLineIdentityOwnership,
  validateRemoteLineHistoryPayload
} from '../src/crm-line-history-remote-identity.mjs';

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
db.close();
console.log('CRM_REMOTE_LINE_IDENTITY_OWNER_GATE=PASS');
console.log('CRM_REMOTE_LINE_DUPLICATE_LINK=BLOCKED');
console.log('CRM_REMOTE_LINE_WORKER_ENVELOPE_AND_ROWS=VERIFIED');
console.log('CRM_REMOTE_LINE_UNATTRIBUTED_MESSAGE=BLOCKED');
console.log('CUSTOMER_MUTATIONS=0');
console.log('PRODUCTION_REQUESTS=0');
