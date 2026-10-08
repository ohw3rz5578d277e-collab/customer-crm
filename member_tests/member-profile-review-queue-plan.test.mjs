import assert from 'node:assert/strict';
import {planProfileReview,planReviewDecision,reviewPayloadDigest} from '../src/member-profile-review-queue-plan.mjs';

const member='MID_abcdefghijklmnopqrstuvwxyz123456';
const prospect='PID_abcdefghijklmnopqrstuvwxyz123456';
const profile={name:'Test',address:{city:'Osaka',zip:'000'}};
const digest=reviewPayloadDigest(profile);
const key='review-20261009-0001';

const base={
 reason_code:'identity_mismatch',
 member_identity_id:member,
 member_identity_authenticated:true,
 authenticated_member_identity_id:member,
 claimed_customer_id:'12345678',
 prospect_id:'',
 submitted_profile:profile,
 queue_idempotency_key:key,
 existing_review_count:0,
 existing_review_count_member_identity_id:member,
 existing_review_count_idempotency_key:key,
 existing_review_count_payload_digest_sha256:digest
};

const r=planProfileReview(base);
assert.equal(r.status,'review_required');
assert.equal(r.ready,true);
assert.equal(r.customer_message,'変更内容を受け付けました');
assert.equal(r.member_identity_id,member);
assert.equal(r.subject_type,'customer');
assert.equal(r.subject_id,'12345678');
assert.equal(r.claimed_customer_id,'12345678');
assert.equal(r.prospect_id,null);
assert.equal(r.master_write_allowed,false);
assert.equal(r.queue_write_allowed,false);
assert.equal(r.google_send_allowed,false);
assert.equal(r.admin_decision_required,true);
assert.equal(r.queue_persistence_requires_separate_gate,true);
assert.equal(r.master_update_blocked_until_review_decision,true);
assert.equal(r.raw_profile_durable_storage_authorized,false);
assert.match(r.review_id,/^RV_[0-9a-f]{64}$/);
assert.equal(r.payload_digest_sha256,digest);
assert.equal(
 reviewPayloadDigest({address:{city:'Osaka',zip:'000'},name:'A'}),
 reviewPayloadDigest({name:'A',address:{zip:'000',city:'Osaka'}})
);

const deterministic=planProfileReview({...base,submitted_profile:{address:{zip:'000',city:'Osaka'},name:'Test'}});
assert.equal(deterministic.review_id,r.review_id);
assert.equal(deterministic.payload_digest_sha256,r.payload_digest_sha256);
const changed=planProfileReview({...base,submitted_profile:{...profile,name:'Changed'},existing_review_count_payload_digest_sha256:reviewPayloadDigest({...profile,name:'Changed'})});
assert.notEqual(changed.review_id,r.review_id);

assert.equal(planProfileReview({...base,reason_code:'unknown'}).status,'invalid_reason');
assert.equal(planProfileReview({...base,member_identity_id:['x']}).status,'invalid_member_identity');
assert.equal(planProfileReview({...base,member_identity_authenticated:'true'}).status,'member_identity_not_authenticated');
assert.equal(planProfileReview({...base,authenticated_member_identity_id:'MID_zyxwvutsrqponmlkjihgfedcba654321'}).status,'member_identity_not_authenticated');
assert.equal(planProfileReview({...base,prospect_id:prospect}).status,'invalid_review_subject');
assert.equal(planProfileReview({...base,claimed_customer_id:'',prospect_id:''}).status,'invalid_review_subject');
assert.equal(planProfileReview({...base,claimed_customer_id:'123',prospect_id:''}).status,'invalid_customer_id');
assert.equal(planProfileReview({...base,claimed_customer_id:'',prospect_id:'PID_bad'}).status,'invalid_prospect_id');
assert.equal(planProfileReview({...base,submitted_profile:[]}).status,'invalid_submitted_profile');
assert.equal(planProfileReview({...base,queue_idempotency_key:'short'}).status,'invalid_queue_idempotency_key');
assert.equal(planProfileReview({...base,existing_review_count:'01'}).status,'invalid_existing_review_count');
assert.equal(planProfileReview({...base,existing_review_count_member_identity_id:'MID_zyxwvutsrqponmlkjihgfedcba654321'}).status,'review_count_scope_mismatch');
assert.equal(planProfileReview({...base,existing_review_count_idempotency_key:'review-20261009-wrong'}).status,'review_count_scope_mismatch');
assert.equal(planProfileReview({...base,existing_review_count_payload_digest_sha256:'0'.repeat(64)}).status,'review_count_scope_mismatch');

const prospectBase={...base,claimed_customer_id:'',prospect_id:prospect};
const prospectReview=planProfileReview(prospectBase);
assert.equal(prospectReview.status,'review_required');
assert.equal(prospectReview.subject_type,'prospect');
assert.equal(prospectReview.subject_id,prospect);

const replayEvidence={
 ...base,
 existing_review_count:1,
 persisted_review_verified:true,
 persisted_review_id:r.review_id,
 persisted_review_member_identity_id:member,
 persisted_review_reason_code:'identity_mismatch',
 persisted_review_subject_type:'customer',
 persisted_review_subject_id:'12345678',
 persisted_review_payload_digest_sha256:digest,
 persisted_review_status:'pending'
};
const replay=planProfileReview(replayEvidence);
assert.equal(replay.status,'review_already_pending');
assert.equal(replay.idempotent_replay,true);
assert.equal(replay.master_write_allowed,false);
assert.equal(planProfileReview({...replayEvidence,persisted_review_subject_id:'87654321'}).status,'review_replay_evidence_mismatch');
assert.equal(planProfileReview({...replayEvidence,persisted_review_status:'approved'}).status,'review_replay_evidence_mismatch');
assert.equal(planProfileReview({...base,existing_review_count:2}).status,'review_state_conflict');

const decisionBase={
 review_id:r.review_id,
 member_identity_id:member,
 reason_code:'identity_mismatch',
 subject_type:'customer',
 subject_id:'12345678',
 payload_digest_sha256:digest,
 persisted_review_verified:true,
 persisted_review_id:r.review_id,
 persisted_review_member_identity_id:member,
 persisted_review_reason_code:'identity_mismatch',
 persisted_review_subject_type:'customer',
 persisted_review_subject_id:'12345678',
 persisted_review_payload_digest_sha256:digest,
 persisted_review_status:'pending',
 admin_actor_verified:true,
 admin_actor_id:'owner-admin',
 decision_idempotency_key:'decision-20261009-0001'
};

let d=planReviewDecision({...decisionBase,decision:'approve'});
assert.equal(d.status,'reverification_required');
assert.equal(d.master_write_allowed,false);

d=planReviewDecision({...decisionBase,decision:'approve',identity_reverified:'true',identity_reverified_member_identity_id:member});
assert.equal(d.status,'reverification_required');
assert.equal(d.master_write_allowed,false);

d=planReviewDecision({...decisionBase,decision:'approve',identity_reverified:true,identity_reverified_member_identity_id:member,latest_version_verified:true,latest_version_verified_subject_type:'customer',latest_version_verified_subject_id:'12345678',verified_current_profile_version:3});
assert.equal(d.status,'approve_ready');
assert.equal(d.master_write_allowed,false);
assert.equal(d.google_send_allowed,false);
assert.equal(d.new_sync_event_required,true);
assert.equal(d.step5_google_sync_required,true);
assert.equal(d.verified_current_profile_version,3);
assert.match(d.decision_event_id,/^RVD_[0-9a-f]{64}$/);
assert.equal(d.execution_requires_separate_gate,true);

const d2=planReviewDecision({...decisionBase,decision:'approve',identity_reverified:true,identity_reverified_member_identity_id:member,latest_version_verified:true,latest_version_verified_subject_type:'customer',latest_version_verified_subject_id:'12345678',verified_current_profile_version:3});
assert.equal(d2.decision_event_id,d.decision_event_id);

const reject=planReviewDecision({...decisionBase,decision:'reject'});
assert.equal(reject.status,'reject_ready');
assert.equal(reject.master_write_allowed,false);
assert.equal(reject.google_send_allowed,false);
assert.equal(reject.audit_required,true);
assert.equal(reject.execution_requires_separate_gate,true);

assert.equal(planReviewDecision({...decisionBase,decision:'reject',persisted_review_subject_id:'87654321'}).status,'persisted_review_binding_mismatch');
assert.equal(planReviewDecision({...decisionBase,decision:'reject',admin_actor_verified:'true'}).status,'admin_actor_not_verified');
assert.equal(planReviewDecision({...decisionBase,decision:'reject',admin_actor_id:['owner-admin']}).status,'invalid_admin_actor');
assert.equal(planReviewDecision({...decisionBase,decision:'approve',identity_reverified:true,identity_reverified_member_identity_id:member,latest_version_verified:true,latest_version_verified_subject_type:'prospect',latest_version_verified_subject_id:'12345678',verified_current_profile_version:3}).status,'latest_version_reverification_required');
assert.equal(planReviewDecision({...decisionBase,decision:'approve',identity_reverified:true,identity_reverified_member_identity_id:member,latest_version_verified:true,latest_version_verified_subject_type:'customer',latest_version_verified_subject_id:'12345678',verified_current_profile_version:'03'}).status,'invalid_verified_profile_version');

console.log('MEMBER_PROFILE_REVIEW_QUEUE_PLAN=PASS');
console.log('DETERMINISTIC_REVIEW_ID=PASS');
console.log('EXACT_REVIEW_BINDING=PASS');
console.log('AMBIGUOUS_MASTER_WRITE=0');
console.log('ADMIN_DECISION_REQUIRED=YES');
console.log('APPROVAL_REVERIFY_REQUIRED=YES');
console.log('STEP5_GOOGLE_SYNC_REQUIRED=YES');
console.log('PRODUCTION_QUEUE_WRITE=0');
