import fs from 'node:fs';
import { DatabaseSync } from 'node:sqlite';

const db = new DatabaseSync(':memory:');
const migration = fs.readFileSync('migrations/20260817_customer_id_reconciliation_reviews.sql','utf8');
db.exec(migration);

db.exec(`
INSERT INTO customer_id_reconciliation_reviews(reservation_customer_id,crm_candidate_customer_id) VALUES ('R-X1','C-Y1');
INSERT INTO customer_id_reconciliation_reviews(reservation_customer_id,crm_candidate_customer_id) VALUES ('R-X2','C-Y2');
`);

function current(id){ return db.prepare('SELECT * FROM customer_id_reconciliation_reviews WHERE id=?').get(id); }
function save(id, decision, reason, who='tester@example.invalid'){
  const before=current(id);
  if(!before) throw new Error('review missing');
  db.prepare(`UPDATE customer_id_reconciliation_reviews SET decision=?,reason=?,reviewed_by=?,reviewed_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE id=?`).run(decision,reason,who,id);
  db.prepare(`INSERT INTO customer_id_reconciliation_review_audit(review_id,reservation_customer_id,crm_candidate_customer_id,from_decision,to_decision,reason,changed_by,changed_at) VALUES(?,?,?,?,?,?,?,CURRENT_TIMESTAMP)`).run(id,before.reservation_customer_id,before.crm_candidate_customer_id,before.decision,decision,reason,who);
}

if(current(1).decision!=='UNREVIEWED' || current(2).decision!=='UNREVIEWED') throw new Error('initial state failed');
save(1,'SAME_PERSON','manual confirmation');
if(current(1).decision!=='SAME_PERSON') throw new Error('SAME_PERSON persistence failed');
if(current(2).decision!=='UNREVIEWED') throw new Error('pair isolation failed after SAME_PERSON');
save(1,'DEFERRED','need more evidence');
if(current(1).decision!=='DEFERRED') throw new Error('decision change/undo failed');
save(2,'DIFFERENT_PERSON','confirmed separate people');
if(current(2).decision!=='DIFFERENT_PERSON') throw new Error('DIFFERENT_PERSON persistence failed');
const audit=db.prepare('SELECT * FROM customer_id_reconciliation_review_audit ORDER BY id').all();
if(audit.length!==3) throw new Error('audit history count failed');
if(audit[0].from_decision!=='UNREVIEWED'||audit[0].to_decision!=='SAME_PERSON') throw new Error('audit transition 1 failed');
if(audit[1].from_decision!=='SAME_PERSON'||audit[1].to_decision!=='DEFERRED') throw new Error('audit transition 2 failed');
if(audit[2].reservation_customer_id!=='R-X2'||audit[2].crm_candidate_customer_id!=='C-Y2') throw new Error('audit pair isolation failed');
const customerTables=db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('customers','customer_reservations','app_reservations')").all();
if(customerTables.length) throw new Error('migration unexpectedly created customer/reservation tables');
console.log('Decision persistence/runtime: SAME_PERSON, DIFFERENT_PERSON, DEFERRED, change, audit, pair isolation PASS');
