import assert from 'node:assert/strict';
import fs from 'node:fs';
import { DatabaseSync } from 'node:sqlite';
import { handleCustomerProfileEnrichmentRequest } from '../src/crm-customer360-profile-enrichment.mjs';
import { guardCustomer360ProfileWrite } from '../src/crm-customer360-profile-write-guard.mjs';

class D1Statement{
  constructor(stmt){this.stmt=stmt;this.args=[]}
  bind(...args){this.args=args;return this}
  async first(){return this.stmt.get(...this.args)||null}
  async all(){return{results:this.stmt.all(...this.args)}}
  async run(){const r=this.stmt.run(...this.args);return{success:true,meta:{changes:Number(r.changes||0)}}}
}
class D1Database{
  constructor(db){this.db=db}
  prepare(sql){return new D1Statement(this.db.prepare(sql))}
}

const sqlite=new DatabaseSync(':memory:');
sqlite.exec(`
CREATE TABLE customers(
  customer_id TEXT PRIMARY KEY,
  name TEXT, furigana TEXT, line_display_name TEXT, line_user_id TEXT,
  phone TEXT, address TEXT, email TEXT, memo TEXT, anniversary TEXT,
  acquisition_source TEXT, updated_at TEXT, deleted_at TEXT
);
CREATE TABLE customer_family_members(
  id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, relation TEXT NOT NULL,
  name TEXT, furigana TEXT, birthdate TEXT, gender TEXT, school_stage TEXT,
  memo TEXT, created_at TEXT, updated_at TEXT, updated_by TEXT, deleted_at TEXT
);
CREATE TABLE customer_reservations(
  reservation_id TEXT PRIMARY KEY, customer_id TEXT, genre TEXT, shoot_date TEXT,
  total_amount REAL, status TEXT, deleted_at TEXT
);
INSERT INTO customers(customer_id,name,furigana,line_display_name,line_user_id,phone,address,email,memo,anniversary,acquisition_source,updated_at,deleted_at)
VALUES('26000123','山田 花子','やまだ はなこ','hanako','U-exact-line','','大阪府','','legacy memo','','instagram','2026-09-01T00:00:00Z',NULL);
INSERT INTO customers(customer_id,name,deleted_at) VALUES('26000999','紹介者',NULL);
INSERT INTO customer_reservations VALUES('R1','26000123','七五三','2026-01-01',10000,'撮影済み',NULL);
INSERT INTO customer_reservations VALUES('R2','26000123','お宮参り','2026-02-01',20000,'先行データ待ち',NULL);
INSERT INTO customer_reservations VALUES('R3','26000123','家族','2026-03-01',30000,'本納品まち',NULL);
INSERT INTO customer_reservations VALUES('R4','26000123','成人','2026-04-01',40000,'納品済み',NULL);
INSERT INTO customer_reservations VALUES('R5','26000123','予約','2026-05-01',50000,'予約確定',NULL);
INSERT INTO customer_reservations VALUES('R6','26000123','キャンセル','2026-06-01',60000,'キャンセル',NULL);
`);
sqlite.exec(fs.readFileSync('migrations_managed/20260903_customer360_profile_auto_enrichment.sql','utf8'));
const env={DB:new D1Database(sqlite),CRM_LOCAL_TEST_AUTH:'1',CRM_CUSTOMER360_WRITE_ENABLED:'1'};
const base='https://crm.test/api/customer360/profile/26000123';
const ownerHeaders={'content-type':'application/json','cf-access-authenticated-user-email':'owner@example.test'};

async function jsonResponse(response){const body=await response.json();return{status:response.status,body}}
async function getProfile(){return jsonResponse(await handleCustomerProfileEnrichmentRequest(new Request(base,{headers:ownerHeaders}),env))}
async function patch(changes){return jsonResponse(await handleCustomerProfileEnrichmentRequest(new Request(base,{method:'PATCH',headers:ownerHeaders,body:JSON.stringify({changes})}),env))}
async function child(payload){return jsonResponse(await handleCustomerProfileEnrichmentRequest(new Request(base+'/children',{method:'POST',headers:ownerHeaders,body:JSON.stringify(payload)}),env))}

{
  const r=await getProfile();assert.equal(r.status,200);const m=r.body.customer.metrics;
  assert.equal(m.reservation_count,6);assert.equal(m.completed_shoot_count,4);assert.equal(m.repeat_count,3);assert.equal(m.customer_repeat_share,0.75);assert.equal(m.lifetime_revenue,100000);assert.equal(m.average_shoot_value,25000);assert.deepEqual(m.shoot_genre_history,['七五三','お宮参り','家族','成人']);assert.equal(m.first_shoot_date,'2026-01-01');assert.equal(m.last_shoot_date,'2026-04-01');assert.equal(m.cancel_count,1);
  console.log('DERIVED_POST_SHOOT_STATUSES=PASS');
}

{
  const a=await patch({phone:'090-1234-5678'}),b=await patch({phone:'090-1234-5678'});assert.equal(a.status,200);assert.equal(b.status,200);assert.equal(sqlite.prepare("SELECT phone FROM customers WHERE customer_id='26000123'").get().phone,'090-1234-5678');assert.equal(sqlite.prepare("SELECT COUNT(*) AS n FROM customer_field_evidence WHERE customer_id='26000123' AND field_name='phone' AND candidate_value='090-1234-5678' AND source='manual'").get().n,1);
  console.log('OWNER_REPEAT_SAVE_IDEMPOTENT=PASS');
}

{
  assert.equal((await patch({nps_score:9,nps_answered_at:'2026-09-03T10:00',nps_comment:'満足'})).status,200);assert.equal((await patch({address:'大阪府大阪市'})).status,200);const p=(await getProfile()).body.customer;assert.equal(p.experience.nps_score,9);assert.equal(p.experience.nps_comment,'満足');
  console.log('UNRELATED_EDIT_PRESERVES_NPS=PASS');
}

{
  assert.equal((await getProfile()).body.customer.notes.current,'legacy memo');assert.equal((await patch({notes:''})).status,200);const p=(await getProfile()).body.customer;assert.equal(p.notes.current,'');assert.ok(p.notes.updated_at);const history=sqlite.prepare("SELECT body FROM customer_notes_history WHERE customer_id='26000123' ORDER BY created_at DESC, note_id DESC LIMIT 1").get();assert.equal(history.body,'');
  console.log('NOTE_EXPLICIT_CLEAR=PASS');
}

{
  for(const order of [1,2,3,4,5]){const r=await child({birth_order:order,name:`子${order}`,name_kana:`こ${order}`,birth_date:`202${order}-01-0${order}`});assert.equal(r.status,200,`child ${order}`)}
  const kids=(await getProfile()).body.customer.family.children;assert.equal(kids.length,5);assert.equal(kids.find(x=>x.birth_order===4)?.name,'子4');assert.equal(kids.find(x=>x.birth_order===5)?.name,'子5');assert.equal((await child({child_id:kids.find(x=>x.birth_order===4).child_id,birth_order:4,name:'子4',name_kana:'こ4',birth_date:'2024-01-04'})).status,200);
  console.log('CHILDREN_OVER_THREE=PASS');
}

{
  const self=await guardCustomer360ProfileWrite(new Request(base,{method:'PATCH',headers:ownerHeaders,body:JSON.stringify({changes:{referrer_customer_id:'26000123'}})}),env);assert.equal(self.status,409);const missing=await guardCustomer360ProfileWrite(new Request(base,{method:'PATCH',headers:ownerHeaders,body:JSON.stringify({changes:{referrer_customer_id:'26000888'}})}),env);assert.equal(missing.status,409);const exact=await guardCustomer360ProfileWrite(new Request(base,{method:'PATCH',headers:ownerHeaders,body:JSON.stringify({changes:{referrer_customer_id:'26000999'}})}),env);assert.equal(exact,null);
  console.log('REFERRER_EXACT_EXISTING_NON_SELF=PASS');
}

{
  const locked={...env,CRM_LOCAL_TEST_AUTH:'0'};const read=await handleCustomerProfileEnrichmentRequest(new Request(base),locked);const write=await handleCustomerProfileEnrichmentRequest(new Request(base,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({changes:{phone:'000'}})}),locked);assert.equal(read.status,401);assert.equal(write.status,401);
  console.log('UNAUTHENTICATED_READ_WRITE_DENIED=PASS');
}

{
  const ui=fs.readFileSync('src/crm-customer360-profile-ui-client.mjs','utf8');const runtime=fs.readFileSync('src/crm-customer360-profile-enrichment.mjs','utf8');const line=fs.readFileSync('src/crm-customer360-line-profile-extraction.mjs','utf8');
  assert.match(ui,/childOrders\(kids,true\)/);assert.ok(!ui.includes('[1,2,3].map'));
  assert.match(ui,/function changed\(el\)/);assert.match(ui,/if\(el&&changed\(el\)\)changes\[n\]=el\.value/);assert.match(ui,/if\(!changed\(nameEl\)&&!changed\(kanaEl\)&&!changed\(birthEl\)\)continue/);
  assert.match(ui,/async function load\(customerId\)[\s\S]*?api\('\/api\/customer360\/customer\/'\+id\)/);assert.doesNotMatch(ui,/async function load\(customerId\)[\s\S]*?api\('\/api\/customer360\/profile\/'\+id\)/);
  assert.match(runtime,/direction='incoming' AND send_status='received'/);assert.match(line,/direction='incoming' AND send_status='received'/);
  for(const secret of ['CRM_INTERNAL_TOKEN','ADMIN_TOKEN','CF_ACCESS_CLIENT_SECRET'])assert.ok(!ui.includes(secret),secret);
  console.log('MOBILE_UI_PERFORMANCE_SECURITY_CONTRACT=PASS');
}

{
  const entry=fs.readFileSync('src/production-index-crm-customer360-entry.js','utf8');
  const safeRead=fs.readFileSync('.github/workflows/customer360-production-safe-read.yml','utf8');
  const deploy=fs.readFileSync('.github/workflows/deploy-cloudflare.yml','utf8');
  const bridge=fs.readFileSync('.github/workflows/dispatch-production-deploy-from-issue.yml','utf8');
  for(const marker of ['customer360_profile_enrichment_schema_available','customer360_family_metadata_available','customer360_field_evidence_available','customer360_notes_history_available']){
    assert.ok(entry.includes(marker),`health schema marker missing: ${marker}`);
    assert.ok(safeRead.includes(marker),`Production verifier schema marker missing: ${marker}`);
  }
  for(const marker of ['customer360_family_marketing_foundation','customer360_profile_enrichment','customer360_line_profile_extraction','referrer_customer_id_exact_existing_customer_only','customer360_profile_composed_into_detail'])assert.ok(safeRead.includes(marker),`canonical health marker missing: ${marker}`);
  assert.ok(!safeRead.includes("'customer360_profile_write_guard'"));
  assert.ok(!safeRead.includes("'customer360_combined_detail'"));
  assert.ok(entry.includes('customer360_identity_fallback:false'));assert.ok(entry.includes('customer360_paid_ai_provider_active:false'));
  const classifyPos=deploy.indexOf('Classify remote pending migrations and read-only state');
  const familyCountPos=deploy.indexOf('Read Production existing family count before migration');
  const applyPos=deploy.indexOf('Apply Customer360 managed remote D1 migration');
  assert.ok(classifyPos>=0&&familyCountPos>classifyPos&&applyPos>familyCountPos,'family pre-count must be conditional after classification and before migration');
  assert.ok(deploy.includes("steps.classify.outputs.family_table == 'PRESENT'"));
  assert.ok(!/Read Production row counts before migration[\s\S]{0,500}COUNT\(\*\) AS family_count FROM customer_family_members/.test(deploy),'unconditional pre-migration family count must remain removed');
  for(const token of ["'mode':'deploy'","'expected_sha':os.environ['EXPECTED_SHA']",'CANONICAL_MODE=deploy'])assert.ok(bridge.includes(token),`deploy bridge exact input missing: ${token}`);
  console.log('PRODUCTION_HEALTH_RELEASE_BRIDGE_CONTRACT=PASS');
}

console.log('CUSTOMER360_PROFILE_PRODUCTION_CLOSE_REGRESSION=PASS');
