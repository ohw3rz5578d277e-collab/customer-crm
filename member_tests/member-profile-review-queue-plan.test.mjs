import assert from 'node:assert/strict';
import {planProfileReview,planReviewDecision} from '../src/member-profile-review-queue-plan.mjs';

const r=planProfileReview({reason_code:'identity_mismatch',member_identity_id:'MID_example',claimed_customer_id:'12345678',submitted_profile:{name:'Test'}});
assert.equal(r.status,'review_required');
assert.equal(r.customer_message,'変更内容を受け付けました');
assert.equal(r.master_write_allowed,false);
assert.equal(r.queue_write_allowed,false);
assert.equal(r.admin_decision_required,true);
assert.match(r.payload_digest_sha256,/^[0-9a-f]{64}$/);

assert.equal(planProfileReview({reason_code:'unknown',member_identity_id:'MID_example'}).status,'invalid_reason');

let d=planReviewDecision({review_status:'pending',decision:'approve'});
assert.equal(d.status,'reverification_required');
assert.equal(d.master_write_allowed,false);

d=planReviewDecision({review_status:'pending',decision:'approve',identity_reverified:true,latest_version_verified:true});
assert.equal(d.status,'approve_ready');
assert.equal(d.master_write_allowed,false);
assert.equal(d.new_sync_event_required,true);
assert.equal(d.execution_requires_separate_gate,true);

d=planReviewDecision({review_status:'pending',decision:'reject'});
assert.equal(d.status,'reject_ready');
assert.equal(d.master_write_allowed,false);
assert.equal(d.audit_required,true);

console.log('MEMBER_PROFILE_REVIEW_QUEUE_PLAN=PASS');
console.log('AMBIGUOUS_MASTER_WRITE=0');
console.log('ADMIN_DECISION_REQUIRED=YES');
console.log('APPROVAL_REVERIFY_REQUIRED=YES');
console.log('PRODUCTION_QUEUE_WRITE=0');
