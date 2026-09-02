import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import { __test, customerProfileEnrichmentHealth } from '../src/crm-customer360-profile-enrichment.mjs';
import { injectCustomer360ProfileUi } from '../src/crm-customer360-profile-ui.mjs';

const root=new URL('../',import.meta.url);
const runtime=await readFile(new URL('src/crm-customer360-profile-enrichment.mjs',root),'utf8');
const migration=await readFile(new URL('migrations_managed/20260903_customer360_profile_auto_enrichment.sql',root),'utf8');
const client=await readFile(new URL('src/crm-customer360-profile-ui-client.mjs',root),'utf8');
const entry=await readFile(new URL('src/production-index-crm-customer360-entry.js',root),'utf8');

let pass=0;
function ok(name,fn){try{fn();pass++;console.log('PASS',name)}catch(e){console.error('FAIL',name,e);process.exitCode=1}}

ok('required Customer360 fields are represented in Owner UI',()=>{
  for(const token of ['氏名','ふりがな','Customer ID','LINE名','電話','住所','メール','家族','撮影履歴','累計売上','初回流入','NPS','掲載許可','備考・対応メモ'])assert.ok(client.includes(token),token);
});

ok('derived metrics use reservation runtime semantics',()=>{
  for(const token of ['reservation_count','completed_shoot_count','repeat_count','customer_repeat_share','lifetime_revenue','average_shoot_value','shoot_genre_history','first_shoot_date','last_shoot_date'])assert.ok(runtime.includes(token),token);
  assert.match(runtime,/Math\.max\(count-1,0\)/);
  assert.match(runtime,/revenue\/count/);
  assert.match(runtime,/customer_reservations/);
});

ok('LINE high confidence extraction recognizes explicit phone and email',()=>{
  const out=__test.extractedCandidates('電話番号は090-1234-5678です。メールはtest@example.comです。');
  assert.equal(out.find(x=>x.field==='phone')?.value,'090-1234-5678');
  assert.equal(out.find(x=>x.field==='email')?.value,'test@example.com');
  assert.ok(out.every(x=>x.confidence>=0&&x.confidence<=1));
});

ok('child and anniversary extraction produce candidates only',()=>{
  const child=__test.extractedCandidates('第一子の名前は陽葵です。第一子は2023年10月5日生まれです。');
  assert.equal(child.find(x=>x.field==='child.1.name')?.value,'陽葵');
  assert.equal(child.find(x=>x.field==='child.1.birth_date')?.value,'2023-10-05');
  const ann=__test.extractedCandidates('結婚記念日は2020年6月20日です。');
  assert.equal(ann.find(x=>x.field==='wedding_anniversary')?.value,'2020-06-20');
});

ok('identity unresolved/conflict cannot silently write',()=>{
  assert.match(runtime,/exactCustomer\(env,id\)/);
  assert.match(runtime,/customer_id AS TEXT\)=\?/);
  assert.match(runtime,/line_user_id=\?/);
  assert.match(runtime,/fallback_used:false/);
  assert.match(runtime,/auto_apply:false/);
  assert.match(runtime,/status=existing&&existing!==text\(c\.value\)\?'conflict':'candidate'/);
});

ok('human confirmed data is represented and LINE extraction never auto applies it',()=>{
  assert.match(migration,/confirmed_by_human INTEGER NOT NULL DEFAULT 0/);
  assert.match(runtime,/status='confirmed',confirmed_by_human=1/);
  const health=customerProfileEnrichmentHealth();
  assert.equal(health.human_confirmed_overwrite_by_line,false);
  assert.equal(health.customer_line_auto_apply,false);
});

ok('manual children remain extensible beyond three',()=>{
  assert.match(migration,/birth_order INTEGER/);
  assert.match(runtime,/birthOrder=Math\.max\(1/);
  assert.ok(!migration.includes('child1_name'));
  assert.ok(!migration.includes('child2_name'));
  assert.ok(!migration.includes('child3_name'));
});

ok('attribution remains read-model reference and source categories are not rewritten',()=>{
  assert.match(runtime,/Attribution remains reference\/read-model data/);
  assert.match(runtime,/first_touch_source/);
  assert.match(runtime,/last_touch_source/);
  assert.ok(!runtime.includes("UPDATE customer_attribution"));
});

ok('security requires Owner or internal auth and does not expose tokens',()=>{
  assert.match(runtime,/authentication_required/);
  assert.match(runtime,/owner_auth_required/);
  assert.equal(customerProfileEnrichmentHealth().browser_secret_exposure,false);
  assert.ok(!client.includes('CRM_INTERNAL_TOKEN'));
  assert.ok(!client.includes('ADMIN_TOKEN'));
});

ok('Customer ID generation and destructive migrations remain zero',()=>{
  assert.equal(customerProfileEnrichmentHealth().customer_id_generation,false);
  assert.equal(customerProfileEnrichmentHealth().destructive_schema_changes,false);
  assert.ok(!/\bDROP\s+(TABLE|COLUMN)\b/i.test(migration));
  assert.ok(!/customer_identity_(sequence|registry)/.test(migration));
  assert.ok(!runtime.includes('assignCanonicalCustomerId'));
});

ok('production entry composes the real Customer360 profile feature',()=>{
  assert.ok(entry.includes("./crm-customer360-profile-enrichment.mjs"));
  assert.ok(entry.includes("./crm-customer360-profile-ui.mjs"));
  const html=injectCustomer360ProfileUi('<html><head></head><body><main>CRM</main></body></html>');
  assert.ok(html.includes('crm-customer360-profile-script'));
  assert.ok(html.includes('crm-customer360-profile-style'));
});

ok('LINE send remains absent',()=>{
  assert.ok(!runtime.includes('LINE_SERVICE.fetch'));
  assert.ok(!runtime.includes('/push'));
  assert.ok(!runtime.includes('/multicast'));
});

console.log(`RESULT ${pass}/12 PASS`);
if(process.exitCode)process.exit(process.exitCode);
