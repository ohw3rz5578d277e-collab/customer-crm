import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-schema-apply.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-schema-apply-from-issue.yml','utf8');

const lifecycleMigrations=[
  '20261007_member_identity_prospect_foundation.sql',
  '20261009_member_registration_consent_event_foundation.sql'
];
const lifecycleTables=[
  'member_identities','member_prospects','member_customer_invitations','member_profile_change_review_queue','member_registration_events','member_consent_evidence'
];
const lifecycleIndexContracts=new Map([
  ['idx_member_identity_customer',{target:'member_identities',unique:true}],
  ['idx_member_identity_prospect',{target:'member_identities',unique:true}],
  ['idx_member_customer_invitation_customer',{target:'member_customer_invitations',unique:false}],
  ['idx_member_registration_events_member_time',{target:'member_registration_events',unique:false}],
  ['idx_member_registration_events_prospect_time',{target:'member_registration_events',unique:false}],
  ['idx_member_consent_evidence_member_time',{target:'member_consent_evidence',unique:false}],
  ['idx_member_consent_evidence_prospect_time',{target:'member_consent_evidence',unique:false}]
]);
const lifecycleTriggerContracts=new Map([
  ['trg_member_registration_events_no_update',{target:'member_registration_events',operation:'UPDATE'}],
  ['trg_member_registration_events_no_delete',{target:'member_registration_events',operation:'DELETE'}],
  ['trg_member_consent_evidence_no_update',{target:'member_consent_evidence',operation:'UPDATE'}],
  ['trg_member_consent_evidence_no_delete',{target:'member_consent_evidence',operation:'DELETE'}]
]);
const lifecycleAuxObjects=[...lifecycleIndexContracts.keys(),...lifecycleTriggerContracts.keys()];
const lifecycleTableSet=new Set(lifecycleTables);
const expectedLifecycleObjectsByMigration=new Map([
  ['20261007_member_identity_prospect_foundation.sql',{
    tables:['member_identities','member_prospects','member_customer_invitations','member_profile_change_review_queue'],
    indexes:['idx_member_identity_customer','idx_member_identity_prospect','idx_member_customer_invitation_customer'],
    triggers:[]
  }],
  ['20261009_member_registration_consent_event_foundation.sql',{
    tables:['member_registration_events','member_consent_evidence'],
    indexes:['idx_member_registration_events_member_time','idx_member_registration_events_prospect_time','idx_member_consent_evidence_member_time','idx_member_consent_evidence_prospect_time'],
    triggers:['trg_member_registration_events_no_update','trg_member_registration_events_no_delete','trg_member_consent_evidence_no_update','trg_member_consent_evidence_no_delete']
  }]
]);

function openQuote(ch){
  if(ch==="'" || ch==='"' || ch==='`') return {open:ch,close:ch,doubled:true};
  if(ch==='[') return {open:'[',close:']',doubled:false};
  return null;
}

function stripSqlComments(raw) {
  let out='';
  let quote=null;
  for(let i=0;i<raw.length;i+=1){
    const ch=raw[i];
    const next=raw[i+1];
    if(quote){
      out+=ch;
      if(ch===quote.close){
        if(quote.doubled && next===quote.close){
          out+=next;
          i+=1;
        }else{
          quote=null;
        }
      }
      continue;
    }
    const opened=openQuote(ch);
    if(opened){
      quote=opened;
      out+=ch;
      continue;
    }
    if(ch==='-' && next==='-'){
      i+=2;
      while(i<raw.length && raw[i] !== '\n' && raw[i] !== '\r') i+=1;
      out+=' ';
      if(i<raw.length) out+=raw[i];
      continue;
    }
    if(ch==='/' && next==='*'){
      i+=2;
      while(i<raw.length && !(raw[i]==='*' && raw[i+1]==='/')) i+=1;
      if(i>=raw.length) throw new Error('unterminated SQL block comment');
      i+=1;
      out+=' ';
      continue;
    }
    out+=ch;
  }
  if(quote) throw new Error('unterminated SQL quoted literal or identifier');
  return out;
}

function splitSqlStatements(raw) {
  const statements=[];
  let current='';
  let quote=null;
  for(let i=0;i<raw.length;i+=1){
    const ch=raw[i];
    const next=raw[i+1];
    current+=ch;
    if(quote){
      if(ch===quote.close){
        if(quote.doubled && next===quote.close){
          current+=next;
          i+=1;
        }else{
          quote=null;
        }
      }
      continue;
    }
    const opened=openQuote(ch);
    if(opened){
      quote=opened;
      continue;
    }
    if(ch===';'){
      const statement=current.slice(0,-1).trim();
      if(statement) statements.push(statement);
      current='';
    }
  }
  if(quote) throw new Error('unterminated SQL quoted literal or identifier');
  if(current.trim()) statements.push(current.trim());
  return statements;
}

function assertExactObjectSet(actual, expected, kind, migration){
  const actualSet=new Set(actual);
  const expectedSet=new Set(expected);
  assert.equal(actual.length,expected.length,`${migration} ${kind} exact object count mismatch`);
  assert.equal(actualSet.size,expectedSet.size,`${migration} ${kind} contains duplicate or missing canonical objects`);
  assert.deepEqual([...actualSet].sort(),[...expectedSet].sort(),`${migration} ${kind} exact object set mismatch`);
}

function auditSchemaOnlyMigration(raw, migration) {
  const sql=stripSqlComments(raw);
  const createdTables=[];
  const createdIndexes=[];
  const createdTriggers=[];
  const triggerBlocks=[...sql.matchAll(/CREATE\s+TRIGGER\s+IF\s+NOT\s+EXISTS[\s\S]*?\bEND\s*;/gi)].map(match=>match[0]);
  for(const block of triggerBlocks){
    const match=block.match(/^CREATE\s+TRIGGER\s+IF\s+NOT\s+EXISTS\s+([A-Za-z0-9_]+)\s+BEFORE\s+(UPDATE|DELETE)\s+ON\s+([A-Za-z0-9_]+)\s+BEGIN\s+SELECT\s+RAISE\(ABORT,\s*'[^']+'\)\s*;\s*END\s*;$/i);
    assert.ok(match,`${migration} contains an unexpected trigger body`);
    const [,triggerName,operationRaw,targetTable]=match;
    const contract=lifecycleTriggerContracts.get(triggerName);
    assert.ok(contract,`${migration} contains unauthorized trigger name: ${triggerName}`);
    assert.equal(targetTable,contract.target,`${migration} contains unauthorized trigger target: ${triggerName}`);
    assert.equal(operationRaw.toUpperCase(),contract.operation,`${migration} contains unauthorized trigger operation: ${triggerName}`);
    createdTriggers.push(triggerName);
  }
  const topLevel=sql.replace(/CREATE\s+TRIGGER\s+IF\s+NOT\s+EXISTS[\s\S]*?\bEND\s*;/gi,' ');
  assert.doesNotMatch(topLevel,/(^|;)\s*(?:INSERT|UPDATE|DELETE|REPLACE|TRUNCATE)\b/im,`${migration} contains top-level DML`);
  assert.doesNotMatch(topLevel,/(^|;)\s*(?:ALTER|DROP)\b/im,`${migration} contains destructive top-level DDL`);
  const statements=splitSqlStatements(topLevel);
  assert.ok(statements.length>0,`${migration} is empty`);
  for(const statement of statements){
    assert.doesNotMatch(statement,/\bAS\s+SELECT\b/i,`${migration} contains data-populating CREATE TABLE AS SELECT`);
    const table=statement.match(/^CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+([A-Za-z0-9_]+)\s*\(/i);
    if(table){
      assert.ok(lifecycleTableSet.has(table[1]),`${migration} creates unauthorized table: ${table[1]}`);
      createdTables.push(table[1]);
      continue;
    }
    const index=statement.match(/^CREATE\s+(UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS\s+([A-Za-z0-9_]+)\s+ON\s+([A-Za-z0-9_]+)\s*\(/i);
    if(index){
      const [,uniqueRaw,indexName,targetTable]=index;
      const contract=lifecycleIndexContracts.get(indexName);
      assert.ok(contract,`${migration} creates unauthorized index name: ${indexName}`);
      assert.equal(targetTable,contract.target,`${migration} creates unauthorized index target: ${indexName}`);
      assert.equal(Boolean(uniqueRaw),contract.unique,`${migration} creates index with wrong uniqueness: ${indexName}`);
      createdIndexes.push(indexName);
      continue;
    }
    assert.fail(`${migration} contains non-schema or unauthorized top-level statement`);
  }

  const expected=expectedLifecycleObjectsByMigration.get(migration);
  if(expected){
    assertExactObjectSet(createdTables,expected.tables,'tables',migration);
    assertExactObjectSet(createdIndexes,expected.indexes,'indexes',migration);
    assertExactObjectSet(createdTriggers,expected.triggers,'triggers',migration);
  }
}

assert.throws(
  ()=>auditSchemaOnlyMigration("CREATE TABLE IF NOT EXISTS harmless (note TEXT DEFAULT '--'); DELETE FROM member_registration_events;",'quote-aware-regression.sql'),
  /top-level DML|non-schema|unauthorized/,
  'SQL comment markers inside quoted literals must not hide following top-level DML'
);
assert.throws(
  ()=>auditSchemaOnlyMigration("CREATE TABLE IF NOT EXISTS harmless (note TEXT DEFAULT '/* not a comment */'); DROP TABLE member_identities;",'quote-aware-block-comment-regression.sql'),
  /destructive top-level DDL|non-schema|unauthorized/,
  'SQL block-comment markers inside quoted literals must not hide following destructive DDL'
);
assert.throws(
  ()=>auditSchemaOnlyMigration("CREATE TABLE IF NOT EXISTS [harmless--name] (id TEXT); DELETE FROM member_registration_events;",'bracket-quoted-line-comment-regression.sql'),
  /top-level DML|non-schema|unauthorized/,
  'SQLite bracket-quoted identifiers containing -- must not hide following top-level DML'
);
assert.throws(
  ()=>auditSchemaOnlyMigration("CREATE TABLE IF NOT EXISTS [harmless/*name*/] (id TEXT); DROP TABLE member_identities;",'bracket-quoted-block-comment-regression.sql'),
  /destructive top-level DDL|non-schema|unauthorized/,
  'SQLite bracket-quoted identifiers containing block-comment markers must not hide following destructive DDL'
);
assert.throws(
  ()=>auditSchemaOnlyMigration("CREATE TABLE IF NOT EXISTS [unterminated--name (id TEXT);",'unterminated-bracket-identifier.sql'),
  /unterminated SQL quoted literal or identifier/,
  'unterminated bracket-quoted identifiers must fail closed'
);
assert.throws(
  ()=>auditSchemaOnlyMigration('CREATE TABLE IF NOT EXISTS copied_customers AS SELECT * FROM customers;','ctas-data-copy-regression.sql'),
  /data-populating CREATE TABLE AS SELECT|non-schema|unauthorized/,
  'CREATE TABLE AS SELECT must be rejected because it materializes Production data'
);
assert.throws(
  ()=>auditSchemaOnlyMigration('CREATE TABLE IF NOT EXISTS member_identities_copy (id TEXT);','unexpected-table-regression.sql'),
  /unauthorized table/,
  'only exact lifecycle table names may be created'
);
assert.throws(
  ()=>auditSchemaOnlyMigration('CREATE INDEX IF NOT EXISTS idx_member_identity_customer ON customers(id);','wrong-index-target-regression.sql'),
  /unauthorized index target|wrong uniqueness/,
  'lifecycle indexes may target only their exact lifecycle tables'
);
assert.throws(
  ()=>auditSchemaOnlyMigration("CREATE TRIGGER IF NOT EXISTS trg_member_registration_events_no_update BEFORE UPDATE ON customers BEGIN SELECT RAISE(ABORT, 'x'); END;",'wrong-trigger-target-regression.sql'),
  /unauthorized trigger target/,
  'lifecycle triggers may target only their exact lifecycle tables'
);
assert.throws(
  ()=>auditSchemaOnlyMigration("CREATE TRIGGER IF NOT EXISTS trg_member_registration_events_no_update BEFORE DELETE ON member_registration_events BEGIN SELECT RAISE(ABORT, 'x'); END;",'wrong-trigger-operation-regression.sql'),
  /unauthorized trigger operation/,
  'a no_update trigger name must remain bound to BEFORE UPDATE'
);
assert.throws(
  ()=>auditSchemaOnlyMigration('CREATE INDEX IF NOT EXISTS idx_member_identity_prospect ON member_identities(prospect_id);','identity-index-uniqueness-regression.sql'),
  /wrong uniqueness/,
  'identity prospect index must remain UNIQUE'
);
assert.throws(
  ()=>auditSchemaOnlyMigration('CREATE UNIQUE INDEX IF NOT EXISTS idx_member_customer_invitation_customer ON member_customer_invitations(canonical_customer_id);','nonunique-index-regression.sql'),
  /wrong uniqueness/,
  'non-unique lifecycle indexes must not silently become unique'
);

const identityMigrationRaw=fs.readFileSync('migrations_managed/20261007_member_identity_prospect_foundation.sql','utf8');
const identityWithoutRequiredIndex=identityMigrationRaw.replace(/CREATE\s+UNIQUE\s+INDEX\s+IF\s+NOT\s+EXISTS\s+idx_member_identity_customer[\s\S]*?;\s*/i,'');
assert.throws(
  ()=>auditSchemaOnlyMigration(identityWithoutRequiredIndex,'20261007_member_identity_prospect_foundation.sql'),
  /indexes exact object count mismatch|indexes exact object set mismatch|duplicate or missing canonical objects/,
  'omitting a required lifecycle index must fail before apply'
);
const consentMigrationRaw=fs.readFileSync('migrations_managed/20261009_member_registration_consent_event_foundation.sql','utf8');
const consentWithoutRequiredTrigger=consentMigrationRaw.replace(/CREATE\s+TRIGGER\s+IF\s+NOT\s+EXISTS\s+trg_member_consent_evidence_no_delete[\s\S]*?\bEND\s*;\s*/i,'');
assert.throws(
  ()=>auditSchemaOnlyMigration(consentWithoutRequiredTrigger,'20261009_member_registration_consent_event_foundation.sql'),
  /triggers exact object count mismatch|triggers exact object set mismatch|duplicate or missing canonical objects/,
  'omitting a required append-only lifecycle trigger must fail before apply'
);

assert.ok(workflow.includes('workflow_dispatch:'),'workflow_dispatch missing');
for(const input of ['expected_sha:','preflight_run_id:','owner_comment_id:','confirmation:']) assert.ok(workflow.includes(input),`workflow input missing: ${input}`);
assert.ok(workflow.includes("test \"$CONFIRMATION_RAW\" = 'APPLY_MEMBER_LIFECYCLE_SCHEMA'"),'lifecycle confirmation gate missing');
assert.ok(!workflow.includes("test \"$CONFIRMATION_RAW\" = 'APPLY_MEMBER_SCHEMA'"),'legacy broad confirmation token must be disabled');
assert.ok(workflow.includes('git ls-remote origin refs/heads/main'),'exact current-main gate missing');
assert.ok(workflow.includes('/issues/comments/$OWNER_COMMENT_ID'),'Owner comment receipt lookup missing');
assert.ok(workflow.includes("confirm=APPLY_MEMBER_LIFECYCLE_SCHEMA"),'exact Owner lifecycle confirmation missing');
assert.ok(workflow.includes('/actions/runs/$PREFLIGHT_RUN_ID'),'preflight receipt lookup missing');
assert.ok(workflow.includes("r.get('conclusion')=='success'"),'successful preflight receipt check missing');
assert.ok(workflow.includes("r.get('head_sha')==os.environ['EXPECTED_SHA']"),'exact-SHA preflight receipt check missing');
assert.ok(workflow.includes("r.get('path')=='.github/workflows/member-production-schema-preflight.yml'"),'canonical preflight receipt check missing');
assert.ok(workflow.includes('requireMemberLifecyclePreApplyState'),'pre-apply lifecycle state gate missing');
assert.ok(workflow.includes('requireMemberLifecyclePostApplyState'),'post-apply lifecycle state gate missing');
assert.ok(workflow.includes('SELECT name,type,tbl_name FROM sqlite_master'),'schema object type/table binding read missing');

const applyMatches=workflow.match(/d1 migrations apply customer-crm-db --remote/g)||[];
assert.equal(applyMatches.length,1,'schema apply workflow must contain exactly one D1 migration apply command');
const preGateIndex=workflow.indexOf('requireMemberLifecyclePreApplyState');
const applyIndex=workflow.indexOf('d1 migrations apply customer-crm-db --remote');
const postGateIndex=workflow.indexOf('requireMemberLifecyclePostApplyState');
assert.ok(preGateIndex>=0 && preGateIndex<applyIndex,'pre-apply gate must run before D1 migration apply');
assert.ok(postGateIndex>applyIndex,'post-apply gate must run after D1 migration apply');
assert.ok(!/\bwrangler(?:@[^\s]+)?\s+deploy\b/.test(workflow),'schema apply workflow must not deploy Worker');
assert.ok(workflow.includes('PRODUCTION_DEPLOY=0'),'deploy zero declaration missing');
assert.ok(workflow.includes('MEMBER_ROUTE_ACTIVATION=0'),'route activation zero declaration missing');
assert.ok(workflow.includes('CRM_WRITE=0'),'CRM write zero declaration missing');
assert.ok(workflow.includes('LINE_SEND=0'),'LINE send zero declaration missing');

for(const migration of lifecycleMigrations){
  assert.ok(workflow.includes(migration),`workflow exact lifecycle migration missing: ${migration}`);
  const raw=fs.readFileSync('migrations_managed/'+migration,'utf8');
  assert.match(raw,/DO NOT APPLY TO PRODUCTION without a separate exact-SHA Owner schema authorization/);
  auditSchemaOnlyMigration(raw,migration);
}
for(const name of [...lifecycleTables,...lifecycleAuxObjects]) assert.ok(workflow.includes(name),`expected lifecycle schema object missing: ${name}`);

assert.ok(bridge.includes("github.event.issue.number == 26"),'bridge must remain pinned to issue #26');
assert.ok(bridge.includes("github.actor == 'ohw3rz5578d277e-collab'"),'bridge Owner actor gate missing');
assert.ok(bridge.includes("^/member-schema-apply sha=([0-9a-f]{40}) preflight_run=([0-9]+) confirm=APPLY_MEMBER_LIFECYCLE_SCHEMA$"),'bridge exact lifecycle apply command missing');
assert.ok(bridge.includes('member-production-schema-apply.yml/dispatches'),'bridge canonical apply workflow target missing');
assert.ok(bridge.includes("'owner_comment_id':os.environ['OWNER_COMMENT_ID']"),'bridge Owner comment receipt forwarding missing');
assert.ok(bridge.includes("'confirmation':'APPLY_MEMBER_LIFECYCLE_SCHEMA'"),'bridge lifecycle confirmation forwarding missing');
assert.ok(!bridge.includes("'confirmation':'APPLY_MEMBER_SCHEMA'"),'bridge legacy broad confirmation must be disabled');
assert.ok(bridge.includes('PRODUCTION_MUTATION_DISPATCH_GATE=FREE'),'Production mutation concurrency gate missing');
assert.ok(bridge.includes('PRODUCTION_DEPLOY=0'),'bridge deploy zero declaration missing');
assert.ok(bridge.includes('MEMBER_ROUTE_ACTIVATION=0'),'bridge route zero declaration missing');

console.log('MEMBER_PRODUCTION_SCHEMA_APPLY_CONTRACT=PASS');
