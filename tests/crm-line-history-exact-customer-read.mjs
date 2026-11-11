import assert from 'node:assert/strict';
import fs from 'node:fs';
import {DatabaseSync} from 'node:sqlite';
import {exactCustomerLineHistoryQuery,readExactCustomerLocalLineMessages} from '../src/crm-line-history-exact-customer-read.mjs';

const source=fs.readFileSync('src/index.js','utf8');
const getLocal=source.slice(source.indexOf('async function getLocalCustomerLineHistory('),source.indexOf('async function fetchRemoteLineHistory('));
assert.ok(getLocal.startsWith('async function getLocalCustomerLineHistory('),'local history selector missing');
assert.match(source,/import \{\s*readExactCustomerLocalLineMessages(?:\s*,\s*[^}]*)?\s*\} from ['"]\.\/crm-line-history-exact-customer-read\.mjs['"]/);
assert.match(getLocal,/readExactCustomerLocalLineMessages\(env\.DB,\s*customer\?\.customer_id\)/);
assert.doesNotMatch(getLocal,/OR\s+line_user_id|line_user_id\s*=\s*\?/i,'unsafe LINE ID cross-customer fallback remains');

const db=new DatabaseSync(':memory:');
db.exec(`CREATE TABLE customer_line_messages (id INTEGER PRIMARY KEY, customer_id TEXT, line_user_id TEXT, message_text TEXT, sent_at TEXT, created_at TEXT);`);
const insert=db.prepare('INSERT INTO customer_line_messages(id,customer_id,line_user_id,message_text,sent_at) VALUES(?,?,?,?,?)');
const shared='U'+'a'.repeat(32);
insert.run(1,'26000101',shared,'A first','2026-11-01T01:00:00Z');
insert.run(2,'26000102',shared,'B private','2026-11-01T02:00:00Z');
insert.run(3,null,shared,'Unassigned legacy','2026-11-01T03:00:00Z');
insert.run(4,'26000101',shared,'A second','2026-11-01T04:00:00Z');
insert.run(5,'26000103','U'+'b'.repeat(32),'C private','2026-11-01T05:00:00Z');

let prepareCount=0;
const readOnlyDb={prepare(sql){prepareCount++;assert.match(sql,/^SELECT \*/);assert.match(sql,/WHERE customer_id = \?/);assert.doesNotMatch(sql,/\bOR\s+line_user_id\b/i);let binds=[];return{bind(...a){binds=a;assert.equal(a.length,1,'customer ID must be the only query binding');return this},async all(){return{results:db.prepare(sql).all(...binds)}}}}};
assert.deepEqual(exactCustomerLineHistoryQuery('26000101').params,['26000101']);
assert.equal(exactCustomerLineHistoryQuery('CRM-26000101'),null);
assert.equal(exactCustomerLineHistoryQuery(26000101),null);
const a=await readExactCustomerLocalLineMessages(readOnlyDb,'26000101');
assert.deepEqual(a.map(r=>r.message_text),['A first','A second']);
const b=await readExactCustomerLocalLineMessages(readOnlyDb,'26000102');
assert.deepEqual(b.map(r=>r.message_text),['B private']);
const empty=await readExactCustomerLocalLineMessages(readOnlyDb,'26000999');
assert.equal(empty.length,0);
const beforeInvalid=prepareCount;
for(const id of [null,'','CRM-26000101','26000101/../26000102',26000101,undefined]){
 assert.deepEqual(await readExactCustomerLocalLineMessages(readOnlyDb,id),[]);
}
assert.equal(prepareCount,beforeInvalid,'invalid customer ID must not query D1');
assert.equal(db.prepare('SELECT COUNT(*) AS n FROM customer_line_messages').get().n,5,'read-only regression changed source data');
db.close();
console.log('CRM_LINE_HISTORY_EXACT_CUSTOMER_READ=PASS');
console.log('CROSS_CUSTOMER_LINE_USER_ID_JOIN=BLOCKED');
console.log('UNASSIGNED_LEGACY_FALLBACK=BLOCKED');
console.log('INVALID_ID_D1_READ=0');
console.log('DATABASE_WRITES_DURING_READ=0');
