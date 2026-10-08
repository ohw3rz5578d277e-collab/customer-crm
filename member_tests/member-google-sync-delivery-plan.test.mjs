import assert from 'node:assert/strict';
import {deriveSyncEventId,planGoogleSyncDispatch,planGoogleSyncAttemptResult,planGoogleSyncReconciliation,profilePayloadDigest,__test} from '../src/member-google-sync-delivery-plan.mjs';

const member='MID_abcdefghijklmnopqrstuvwxyz123456';
const customer='12345678';
const profile={name:'Test',email:'a@example.test',address:{city:'Osaka'}};
const digest=profilePayloadDigest(profile);
const decision='RVD_'+'a'.repeat(64);
const nextVersion=4;
const event=deriveSyncEventId({
 decision_event_id:decision,member_identity_id:member,subject_type:'customer',subject_id:customer,
 profile_version:nextVersion,payload_digest_sha256:digest
});
assert.match(event,/^SE_[0-9a-f]{64}$/);
assert.equal(event,deriveSyncEventId({decision_event_id:decision,member_identity_id:member,subject_type:'customer',subject_id:customer,profile_version:nextVersion,payload_digest_sha256:digest}));

const base={
 server_execution_context_verified:true,
 destination_contract:__test.DESTINATION_CONTRACT,
 review_decision_status:'approve_ready',
 review_decision_verified:true,
 decision_event_id:decision,
 persisted_decision_event_id:decision,
 member_identity_id:member,
 persisted_decision_member_identity_id:member,
 subject_type:'customer',subject_id:customer,
 persisted_decision_subject_type:'customer',persisted_decision_subject_id:customer,
 payload_digest_sha256:digest,persisted_decision_payload_digest_sha256:digest,
 verified_current_profile_version:3,persisted_decision_profile_version:3,next_profile_version:4,
 profile,
 existing_sync_event_count:0,
 existing_sync_event_count_member_identity_id:member,
 existing_sync_event_count_subject_type:'customer',
 existing_sync_event_count_subject_id:customer,
 existing_sync_event_count_profile_version:4,
 existing_sync_event_count_payload_digest_sha256:digest
};

const dispatch=planGoogleSyncDispatch(base);
assert.equal(dispatch.status,'dispatch_ready');
assert.equal(dispatch.ready,true);
assert.equal(dispatch.sync_event_id,event);
assert.equal(dispatch.profile_version,4);
assert.equal(dispatch.max_attempts,4);
assert.deepEqual(dispatch.retry_delays_ms,[60000,300000,1800000]);
assert.equal(dispatch.send_allowed,false);
assert.equal(dispatch.google_write_allowed,false);
assert.equal(dispatch.browser_direct_google_access,false);
assert.equal(dispatch.execution_requires_separate_gate,true);

assert.equal(planGoogleSyncDispatch({...base,server_execution_context_verified:'true'}).status,'server_context_not_verified');
assert.equal(planGoogleSyncDispatch({...base,destination_contract:'wrong'}).status,'invalid_destination_contract');
assert.equal(planGoogleSyncDispatch({...base,review_decision_status:'reject_ready'}).status,'review_not_approved');
assert.equal(planGoogleSyncDispatch({...base,member_identity_id:['bad']}).status,'invalid_dispatch_identity');
assert.equal(planGoogleSyncDispatch({...base,next_profile_version:5,existing_sync_event_count_profile_version:5}).status,'profile_version_not_next');
assert.equal(planGoogleSyncDispatch({...base,persisted_decision_subject_id:'87654321'}).status,'review_decision_binding_mismatch');
assert.equal(planGoogleSyncDispatch({...base,profile:{...profile,name:'Changed'}}).status,'profile_digest_mismatch');
assert.equal(planGoogleSyncDispatch({...base,existing_sync_event_count:'0'}).status,'invalid_sync_event_count');
assert.equal(planGoogleSyncDispatch({...base,existing_sync_event_count_subject_id:'87654321'}).status,'sync_event_count_scope_mismatch');
assert.equal(planGoogleSyncDispatch({...base,profile:{name:undefined}}).status,'invalid_profile_payload');

const replay=planGoogleSyncDispatch({...base,existing_sync_event_count:1,persisted_sync_event_verified:true,persisted_sync_event_id:event,persisted_sync_member_identity_id:member,persisted_sync_subject_type:'customer',persisted_sync_subject_id:customer,persisted_sync_profile_version:4,persisted_sync_payload_digest_sha256:digest});
assert.equal(replay.status,'sync_event_already_planned');
assert.equal(replay.idempotent_replay,true);
assert.equal(planGoogleSyncDispatch({...base,existing_sync_event_count:1,persisted_sync_event_verified:true,persisted_sync_event_id:event,persisted_sync_member_identity_id:member,persisted_sync_subject_type:'customer',persisted_sync_subject_id:'87654321',persisted_sync_profile_version:4,persisted_sync_payload_digest_sha256:digest}).status,'sync_event_replay_evidence_mismatch');
assert.equal(planGoogleSyncDispatch({...base,existing_sync_event_count:2}).status,'sync_event_state_conflict');

const attemptBase={sync_event_id:event,persisted_sync_event_verified:true,persisted_sync_event_id:event,attempt_number:1,persisted_attempt_number:1};
let attempt=planGoogleSyncAttemptResult({...attemptBase,result:'network_error'});
assert.equal(attempt.status,'retry_scheduled');
assert.equal(attempt.next_attempt_number,2);
assert.equal(attempt.retry_delay_ms,60000);
assert.equal(attempt.send_allowed,false);

attempt=planGoogleSyncAttemptResult({...attemptBase,attempt_number:2,persisted_attempt_number:2,result:'google_429'});
assert.equal(attempt.retry_delay_ms,300000);
attempt=planGoogleSyncAttemptResult({...attemptBase,attempt_number:3,persisted_attempt_number:3,result:'google_5xx'});
assert.equal(attempt.retry_delay_ms,1800000);
attempt=planGoogleSyncAttemptResult({...attemptBase,attempt_number:4,persisted_attempt_number:4,result:'timeout'});
assert.equal(attempt.status,'retry_budget_exhausted');
assert.equal(attempt.review_required,true);

attempt=planGoogleSyncAttemptResult({...attemptBase,result:'success',remote_commit_verified:true});
assert.equal(attempt.status,'sync_confirmed');
assert.equal(attempt.ready,true);
assert.equal(planGoogleSyncAttemptResult({...attemptBase,result:'success',remote_commit_verified:false}).status,'remote_commit_not_verified');
assert.equal(planGoogleSyncAttemptResult({...attemptBase,result:'auth_failure'}).status,'permanent_sync_failure');
assert.equal(planGoogleSyncAttemptResult({...attemptBase,result:'unknown'}).status,'invalid_attempt_result');
assert.equal(planGoogleSyncAttemptResult({...attemptBase,persisted_attempt_number:2,result:'network_error'}).status,'attempt_binding_mismatch');
assert.equal(planGoogleSyncAttemptResult({...attemptBase,attempt_number:'1',persisted_attempt_number:'1',result:'network_error'}).status,'invalid_attempt_identity');

const reconciliationBase={
 sync_event_id:event,
 expected_subject_type:'customer',expected_subject_id:customer,expected_profile_version:4,expected_payload_digest_sha256:digest,
 completed_attempt_count:1,
 persisted_sync_event_verified:true,persisted_sync_event_id:event,persisted_sync_subject_type:'customer',persisted_sync_subject_id:customer,persisted_sync_profile_version:4,persisted_sync_payload_digest_sha256:digest,
 remote_observation_available:true,remote_subject_type:'customer',remote_subject_id:customer,remote_profile_version:4,remote_payload_digest_sha256:digest
};
let rec=planGoogleSyncReconciliation(reconciliationBase);
assert.equal(rec.status,'in_sync');
assert.equal(rec.reconciliation_complete,true);
assert.equal(rec.send_allowed,false);

rec=planGoogleSyncReconciliation({...reconciliationBase,remote_observation_available:false});
assert.equal(rec.status,'reconciliation_retry_required');
assert.equal(rec.next_attempt_number,2);
assert.equal(rec.execute,false);
rec=planGoogleSyncReconciliation({...reconciliationBase,remote_profile_version:3});
assert.equal(rec.status,'reconciliation_retry_required');
rec=planGoogleSyncReconciliation({...reconciliationBase,completed_attempt_count:4,remote_profile_version:3});
assert.equal(rec.status,'reconciliation_retry_budget_exhausted');
assert.equal(rec.review_required,true);
rec=planGoogleSyncReconciliation({...reconciliationBase,remote_profile_version:5});
assert.equal(rec.status,'remote_version_ahead');
assert.equal(rec.review_required,true);
rec=planGoogleSyncReconciliation({...reconciliationBase,remote_payload_digest_sha256:'b'.repeat(64)});
assert.equal(rec.status,'remote_digest_conflict');
rec=planGoogleSyncReconciliation({...reconciliationBase,remote_subject_id:'87654321'});
assert.equal(rec.status,'remote_subject_mismatch');
rec=planGoogleSyncReconciliation({...reconciliationBase,persisted_sync_subject_id:'87654321'});
assert.equal(rec.status,'reconciliation_binding_mismatch');
assert.equal(planGoogleSyncReconciliation({...reconciliationBase,completed_attempt_count:'1'}).status,'invalid_reconciliation_identity');

console.log('MEMBER_GOOGLE_SYNC_DELIVERY_PLAN=PASS');
console.log('DETERMINISTIC_SYNC_EVENT_ID=PASS');
console.log('EXACT_DECISION_BINDING=PASS');
console.log('BOUNDED_RETRY_ATTEMPTS=4');
console.log('RECONCILIATION_EXACT_BINDING=PASS');
console.log('BROWSER_DIRECT_GOOGLE_ACCESS=0');
console.log('GOOGLE_NETWORK_SEND=0');
console.log('PRODUCTION_WRITE=0');
