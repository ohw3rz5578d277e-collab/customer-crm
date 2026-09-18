import fs from 'node:fs';
import assert from 'node:assert/strict';
import { buildLineHistoryOwnerAuthorizationPacket } from '../src/crm-line-history-owner-authorization-packet.mjs';

const privatePlan={
  planner:'line_history_owner_decision_plan_v1',
  submitted_decisions:3,
  undecided_groups:1,
  decision_summary:{
    SAME_PERSON:1,
    DIFFERENT_PERSON:1,
    DEFERRED:1,
    NEEDS_MORE_EVIDENCE:0
  },
  proposed_write_actions:1,
  accepted_no_write_decisions:2,
  validation_error_count:0,
  proposed_actions:[
    {
      queue_id:'safe-queue-id',
      action:'LINK_LINE_ID_TO_EXISTING_CUSTOMER',
      target_customer_id:'26001234',
      line_user_id:'Uabcdefabcdefabcdefabcdefabcdef12',
      source:'owner_review_same_person'
    }
  ],
  no_write_decisions:[
    {queue_id:'safe-no-write',decision:'DIFFERENT_PERSON'}
  ],
  validation_errors:[],
  ready_for_separate_write_authorization:true,
  authorization_granted:false,
  safety:{
    production_d1_read:0,
    production_d1_write:0,
    generated_sql:false,
    executed_sql:false,
    customer_id_generation:0,
    customer_update:0,
    customer_delete:0,
    customer_merge:0,
    line_send:0,
    worker_deploy:0,
    production_deploy:0
  }
};

const raw=Buffer.from(JSON.stringify(privatePlan,null,2)+'\n');
const mainSha='2df62bf3682ae0e0444e024605118098fc43e15f';
const packet=buildLineHistoryOwnerAuthorizationPacket({
  plan:privatePlan,
  sourcePlanBytes:raw,
  mainSha
});

assert.equal(packet.planner,'line_history_owner_write_authorization_packet_v1');
assert.match(packet.source_plan_sha256,/^[0-9a-f]{64}$/);
assert.equal(packet.source_main_sha,mainSha);
assert.equal(packet.proposed_write_actions,1);
assert.equal(packet.validation_error_count,0);
assert.equal(packet.authorization_scope,'LINE_HISTORY_EXISTING_CUSTOMER_LINE_LINK_ONLY');
assert.equal(packet.authorization_required,true);
assert.equal(packet.packet_ready,true);
assert.equal(packet.authorization_granted,false);
assert.equal(packet.blockers.length,0);
assert.match(packet.approval_text,/^PLAN_SHA [0-9a-f]{64} \/ MAIN_SHA [0-9a-f]{40} のLINE history Production D1 writeを承認します$/);

const serialized=JSON.stringify(packet);
for(const secret of [
  '26001234',
  'Uabcdefabcdefabcdefabcdefabcdef12',
  'owner_review_same_person',
  'LINK_LINE_ID_TO_EXISTING_CUSTOMER'
]){
  assert.ok(!serialized.includes(secret),'private action leaked: '+secret);
}

assert.equal(packet.safety.production_d1_read,0);
assert.equal(packet.safety.production_d1_write,0);
assert.equal(packet.safety.sql_generated,false);
assert.equal(packet.safety.sql_executed,false);
assert.equal(packet.safety.customer_id_generation,0);
assert.equal(packet.safety.customer_update,0);
assert.equal(packet.safety.customer_delete,0);
assert.equal(packet.safety.customer_merge,0);
assert.equal(packet.safety.line_send,0);
assert.equal(packet.safety.worker_deploy,0);
assert.equal(packet.safety.production_deploy,0);
assert.equal(packet.safety.private_action_values_output,false);

const badValidation=buildLineHistoryOwnerAuthorizationPacket({
  plan:{...privatePlan,validation_error_count:1},
  sourcePlanBytes:raw,
  mainSha
});
assert.equal(badValidation.packet_ready,false);
assert.ok(badValidation.blockers.includes('PLAN_VALIDATION_ERRORS_PRESENT'));
assert.equal(badValidation.approval_text,'');

const badMain=buildLineHistoryOwnerAuthorizationPacket({
  plan:privatePlan,
  sourcePlanBytes:raw,
  mainSha:'bad-sha'
});
assert.equal(badMain.packet_ready,false);
assert.ok(badMain.blockers.includes('MAIN_SHA_INVALID'));

const preAuthorized=buildLineHistoryOwnerAuthorizationPacket({
  plan:{...privatePlan,authorization_granted:true},
  sourcePlanBytes:raw,
  mainSha
});
assert.equal(preAuthorized.packet_ready,false);
assert.ok(preAuthorized.blockers.includes('PLAN_ALREADY_MARKED_AUTHORIZED'));

const unsafePlan=buildLineHistoryOwnerAuthorizationPacket({
  plan:{
    ...privatePlan,
    safety:{...privatePlan.safety,production_d1_write:1,generated_sql:true}
  },
  sourcePlanBytes:raw,
  mainSha
});
assert.equal(unsafePlan.packet_ready,false);
assert.ok(unsafePlan.blockers.includes('PLAN_PRODUCTION_WRITE_NOT_ZERO'));
assert.ok(unsafePlan.blockers.includes('PLAN_ALREADY_GENERATED_SQL'));

const noWritePlan={
  ...privatePlan,
  proposed_write_actions:0,
  ready_for_separate_write_authorization:false
};
const noWritePacket=buildLineHistoryOwnerAuthorizationPacket({
  plan:noWritePlan,
  sourcePlanBytes:Buffer.from(JSON.stringify(noWritePlan)),
  mainSha
});
assert.equal(noWritePacket.packet_ready,true);
assert.equal(noWritePacket.authorization_required,false);
assert.equal(noWritePacket.approval_text,'');

const cli=fs.readFileSync('scripts/build-line-history-owner-authorization-packet.mjs','utf8');
assert.doesNotMatch(cli,/\bwrangler\b/i);
assert.doesNotMatch(cli,/child_process/i);
assert.doesNotMatch(cli,/INSERT\s+INTO/i);
assert.doesNotMatch(cli,/UPDATE\s+customers/i);
assert.match(cli,/AUTHORIZATION_GRANTED=NO/);
assert.match(cli,/SQL_GENERATED=0/);
assert.match(cli,/SQL_EXECUTED=0/);
assert.match(cli,/PRIVATE_ACTION_VALUES_PRINTED_TO_TERMINAL=0/);

const runner=fs.readFileSync('scripts/run-line-history-owner-authorization-prep.sh','utf8');
assert.match(runner,/MAIN_SHA_GUARD=PASS/);
assert.match(runner,/STOP_MAIN_DRIFT/);
assert.match(runner,/STOP_OWNER_DECISION_PLAN_VALIDATION/);
assert.match(runner,/STOP_AUTHORIZATION_PACKET_NOT_READY/);
assert.match(runner,/AUTHORIZATION_GRANTED=NO/);
assert.match(runner,/SQL_GENERATED=0/);
assert.match(runner,/SQL_EXECUTED=0/);
assert.match(runner,/PRODUCTION_D1_WRITE=0/);
assert.doesNotMatch(runner,/\bwrangler\b/i);
assert.doesNotMatch(runner,/d1\s+execute/i);
assert.doesNotMatch(runner,/INSERT\s+INTO/i);
assert.doesNotMatch(runner,/UPDATE\s+customers/i);
assert.doesNotMatch(runner,/DELETE\s+FROM/i);
assert.doesNotMatch(runner,/deploy/i);

console.log('LINE_HISTORY_OWNER_AUTH_PACKET_PRIVATE_VALUES_HIDDEN=PASS');
console.log('LINE_HISTORY_OWNER_AUTH_PACKET_EXACT_PLAN_MAIN_BINDING=PASS');
console.log('LINE_HISTORY_OWNER_AUTH_PACKET_INVALID_PLAN_BLOCKED=PASS');
console.log('LINE_HISTORY_OWNER_AUTH_PACKET_AUTH_NOT_GRANTED=PASS');
console.log('LINE_HISTORY_OWNER_AUTH_PREP_MAIN_GUARD=PASS');
console.log('SQL_GENERATED=0');
console.log('SQL_EXECUTED=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('LINE_SEND=0');
