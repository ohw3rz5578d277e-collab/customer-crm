import assert from 'node:assert/strict';
import fs from 'node:fs';

const workflow=fs.readFileSync('.github/workflows/member-production-schema-preflight.yml','utf8');
const bridge=fs.readFileSync('.github/workflows/dispatch-member-schema-preflight-from-issue.yml','utf8');

const lifecycleMigrations=[
  '20261007_member_identity_prospect_foundation.sql',
  '20261009_member_registration_consent_event_foundation.sql'
];
const lifecycleObjects=[
  'member_identities','member_prospects','member_customer_invitations','member_profile_change_review_queue','member_registration_events','member_consent_evidence',
  'idx_member_identity_customer','idx_member_identity_prospect','idx_member_customer_invitation_customer',
  'idx_member_registration_events_member_time','idx_member_registration_events_prospect_time','idx_member_consent_evidence_member_time','idx_member_consent_evidence_prospect_time',
  'trg_member_registration_events_no_update','trg_member_registration_events_no_delete','trg_member_consent_evidence_no_update','trg_member_consent_evidence_no_delete'
];

assert.match(workflow,/workflow_dispatch:/);
assert.ok(workflow.includes('expected_sha:'),'exact SHA input missing');
assert.ok(workflow.includes('owner_comment_id:'),'Owner receipt input missing');
assert.ok(workflow.includes('persist-credentials: false'),'checkout credentials must stay disabled');
assert.ok(workflow.includes('git ls-remote origin refs/heads/main'),'fresh current-main gate missing');
assert.ok(workflow.includes('/issues/comments/$OWNER_COMMENT_ID'),'Owner comment receipt lookup missing');
assert.ok(workflow.includes("endswith('/issues/26')"),'Owner comment issue #26 check missing');
assert.ok(workflow.includes('d1 migrations list customer-crm-db --remote'),'pending migration read missing');
assert.ok(workflow.includes('SELECT name,type,tbl_name FROM sqlite_master WHERE name COLLATE NOCASE IN ('),'schema object type/table binding read missing');
assert.ok(workflow.includes('requireMemberLifecyclePreApplyState'),'lifecycle pre-apply classifier missing');
assert.ok(workflow.includes('MEMBER_SCHEMA_PREFLIGHT_CLASSIFICATION=${result.status}'),'classification evidence missing');
assert.ok(workflow.includes('MEMBER_SCHEMA_APPLY=0'),'schema apply zero declaration missing');
assert.ok(workflow.includes('PRODUCTION_D1_WRITE=0'),'D1 write zero declaration missing');
assert.ok(workflow.includes('PRODUCTION_DEPLOY=0'),'deploy zero declaration missing');
assert.ok(workflow.includes('MEMBER_ROUTE_ACTIVATION=0'),'route activation zero declaration missing');
assert.ok(!workflow.includes('d1 migrations apply customer-crm-db --remote'),'preflight must never apply migrations');
assert.ok(!/\bwrangler(?:@[^\s]+)?\s+deploy\b/.test(workflow),'preflight must never deploy Worker');

for(const migration of lifecycleMigrations){
  assert.ok(workflow.includes(migration),`workflow lifecycle migration missing: ${migration}`);
  const raw=fs.readFileSync('migrations_managed/'+migration,'utf8');
  assert.match(raw,/DO NOT APPLY TO PRODUCTION without a separate exact-SHA Owner schema authorization/);
}
for(const name of lifecycleObjects) assert.ok(workflow.includes(name),`lifecycle schema receipt object missing: ${name}`);

assert.ok(bridge.includes("github.event.issue.number == 26"),'bridge must remain pinned to release-gate issue #26');
assert.ok(bridge.includes("github.actor == 'ohw3rz5578d277e-collab'"),'bridge Owner actor gate missing');
assert.ok(bridge.includes("^/member-schema-preflight sha=([0-9a-f]{40})$"),'bridge exact command format missing');
assert.ok(bridge.includes('OWNER_COMMENT_ID: ${{ github.event.comment.id }}'),'bridge Owner receipt capture missing');
assert.ok(bridge.includes("'owner_comment_id':os.environ['OWNER_COMMENT_ID']"),'bridge Owner receipt forwarding missing');
assert.ok(bridge.includes('member-production-schema-preflight.yml/dispatches'),'bridge canonical workflow target missing');
assert.ok(bridge.includes('BRIDGE_PRODUCTION_WRITE=0'),'bridge zero-write declaration missing');
assert.ok(!bridge.includes('/member-schema-apply '),'preflight bridge must not expose schema apply command');

console.log('MEMBER_PRODUCTION_SCHEMA_PREFLIGHT_CONTRACT=PASS');
