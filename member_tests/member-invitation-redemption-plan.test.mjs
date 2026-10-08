import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {planOneTimeInvitationRedemption} from '../src/member-invitation-redemption-plan.mjs';

const token='abcdefghijklmnopqrstuvwxyzABCDEFGH_1234567890';
const digest=crypto.createHash('sha256').update(token).digest('hex');
const base={
  raw_token:token,
  server_now_verified:true,
  now:'2026-10-09T00:00:00.000Z',
  invitation_record_verified:true,
  persisted_invitation_id:'INV_abcdefghijklmnopqrstuvwx',
  persisted_canonical_customer_id:'12345678',
  persisted_token_sha256:digest,
  persisted_expires_at:'2026-10-10T00:00:00.000Z',
  persisted_consumed_at:null,
  persisted_invalidated_at:null,
  matching_invitation_count:1,
  matching_invitation_count_token_sha256:digest,
  active_invitation_count_for_customer:1,
  active_invitation_count_customer_id:'12345678'
};

const ready=planOneTimeInvitationRedemption(base);
assert.equal(ready.status,'ready');
assert.equal(ready.ready,true);
assert.equal(ready.canonical_customer_id,'12345678');
assert.equal(ready.token_digest_verified,true);
assert.equal(ready.server_now_verified,true);
assert.equal(ready.single_use,true);
assert.equal(ready.customer_id_from_server_record,true);
assert.equal(ready.customer_id_from_client,false);
assert.equal(ready.write_allowed,false);
assert.equal(ready.execute,false);
assert.equal(ready.production_write_authorized,false);
assert.equal(ready.execution_requires_separate_gate,true);
assert.equal(ready.atomic_redemption.mode,'single_statement_compare_and_set');
assert.equal(ready.atomic_redemption.database_clock_required,true);
assert.equal(ready.atomic_redemption.expiry_rechecked_at_execution,true);
assert.equal(ready.atomic_redemption.active_cardinality_rechecked_at_execution,true);
assert.equal(ready.atomic_redemption.success_requires_affected_rows,1);
assert.equal(ready.atomic_redemption.retry_without_revalidation,false);
assert.match(ready.atomic_redemption.statement,/SET consumed_at = strftime/);
assert.match(ready.atomic_redemption.statement,/consumed_at IS NULL/);
assert.match(ready.atomic_redemption.statement,/invalidated_at IS NULL/);
assert.match(ready.atomic_redemption.statement,/expires_at > strftime/);
assert.match(ready.atomic_redemption.statement,/NOT EXISTS/);
assert.match(ready.atomic_redemption.statement,/other\.canonical_customer_id = \?/);
assert.equal(ready.atomic_redemption.binds.length,6);
assert.ok(!JSON.stringify(ready).includes(token));

for(const raw_token of [undefined,null,'short',` ${token}`,`${token} `,[token],{},Object.create(null)]){
  assert.doesNotThrow(()=>planOneTimeInvitationRedemption({...base,raw_token}));
  assert.equal(planOneTimeInvitationRedemption({...base,raw_token}).status,'invalid_invitation_token');
}

assert.equal(planOneTimeInvitationRedemption({...base,server_now_verified:'true'}).status,'server_now_not_verified');
assert.equal(planOneTimeInvitationRedemption({...base,server_now_verified:false}).status,'server_now_not_verified');
assert.equal(planOneTimeInvitationRedemption({...base,invitation_record_verified:'true'}).status,'invitation_record_not_verified');
assert.equal(planOneTimeInvitationRedemption({...base,invitation_record_verified:false}).status,'invitation_record_not_verified');

const noConsumed={...base};
delete noConsumed.persisted_consumed_at;
assert.equal(planOneTimeInvitationRedemption(noConsumed).status,'missing_persisted_consumed_at_evidence');
const noInvalidated={...base};
delete noInvalidated.persisted_invalidated_at;
assert.equal(planOneTimeInvitationRedemption(noInvalidated).status,'missing_persisted_invalidated_at_evidence');

assert.equal(planOneTimeInvitationRedemption({...base,persisted_consumed_at:'2026-10-08T00:00:00.000Z'}).status,'invitation_already_consumed');
assert.equal(planOneTimeInvitationRedemption({...base,persisted_invalidated_at:'2026-10-08T00:00:00.000Z'}).status,'invitation_revoked');
assert.equal(planOneTimeInvitationRedemption({...base,now:'2026-10-10T00:00:00.000Z'}).status,'invitation_expired');
assert.equal(planOneTimeInvitationRedemption({...base,persisted_expires_at:'2026-02-30T00:00:00.000Z'}).status,'invalid_persisted_expiry');
assert.equal(planOneTimeInvitationRedemption({...base,now:'2026-10-09T00:00:00Z'}).status,'invalid_now_evidence');

const otherToken='ZYXWVUTSRQPONMLKJIHGFEDCBA9876543210_abcdefghijk';
const otherDigest=crypto.createHash('sha256').update(otherToken).digest('hex');
assert.equal(planOneTimeInvitationRedemption({...base,persisted_token_sha256:otherDigest}).status,'persisted_invitation_digest_mismatch');
assert.equal(planOneTimeInvitationRedemption({...base,matching_invitation_count_token_sha256:otherDigest}).status,'matching_invitation_count_scope_mismatch');
assert.equal(planOneTimeInvitationRedemption({...base,matching_invitation_count:0}).status,'invitation_not_found');
assert.equal(planOneTimeInvitationRedemption({...base,matching_invitation_count:2}).status,'ambiguous_invitation_digest');
assert.equal(planOneTimeInvitationRedemption({...base,active_invitation_count_for_customer:0}).status,'active_invitation_cardinality_violation');
assert.equal(planOneTimeInvitationRedemption({...base,active_invitation_count_for_customer:2}).status,'active_invitation_cardinality_violation');
assert.equal(planOneTimeInvitationRedemption({...base,active_invitation_count_customer_id:'87654321'}).status,'active_invitation_count_scope_mismatch');

for(const key of ['matching_invitation_count','active_invitation_count_for_customer']){
  for(const malformed of [[],{},Object.create(null),-1,1.5,Number.MAX_SAFE_INTEGER+1,'01','1.0','-1',' 1']){
    assert.doesNotThrow(()=>planOneTimeInvitationRedemption({...base,[key]:malformed}));
    assert.match(planOneTimeInvitationRedemption({...base,[key]:malformed}).status,/^invalid_/);
  }
}

assert.equal(planOneTimeInvitationRedemption({...base,persisted_invitation_id:['INV_abcdefghijklmnopqrstuvwx']}).status,'invalid_persisted_invitation_id');
assert.equal(planOneTimeInvitationRedemption({...base,persisted_invitation_id:` ${base.persisted_invitation_id}`}).status,'invalid_persisted_invitation_id');
assert.equal(planOneTimeInvitationRedemption({...base,persisted_canonical_customer_id:['12345678']}).status,'invalid_persisted_customer_id');
assert.equal(planOneTimeInvitationRedemption({...base,persisted_canonical_customer_id:'12345678 '}).status,'invalid_persisted_customer_id');
assert.equal(planOneTimeInvitationRedemption({...base,persisted_token_sha256:[digest]}).status,'invalid_persisted_token_digest');
assert.equal(planOneTimeInvitationRedemption({...base,persisted_token_sha256:`${digest} `}).status,'invalid_persisted_token_digest');
assert.equal(planOneTimeInvitationRedemption({...base,matching_invitation_count_token_sha256:[digest]}).status,'invalid_matching_invitation_count_scope');
assert.equal(planOneTimeInvitationRedemption({...base,active_invitation_count_customer_id:['12345678']}).status,'invalid_active_invitation_count_scope');

console.log('MEMBER_INVITATION_REDEMPTION_PLAN=PASS');
console.log('RAW_INVITATION_TOKEN_OUTPUT=0');
console.log('ATOMIC_COMPARE_AND_SET=PASS');
console.log('EXECUTION_TIME_EXPIRY_RECHECK=PASS');
console.log('EXECUTION_TIME_ACTIVE_CARDINALITY_RECHECK=PASS');
console.log('CUSTOMER_ID_FROM_CLIENT=0');
console.log('PRODUCTION_WRITE=0');
