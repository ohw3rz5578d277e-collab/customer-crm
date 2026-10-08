import assert from 'node:assert/strict';
import {buildProspectRegistrationPlan} from '../src/member-identity-prospect-foundation.mjs';
import {planProspectCustomerPromotion} from '../src/member-prospect-customer-promotion-plan.mjs';
import {planProfileSync,createSyncEventId} from '../src/member-google-profile-sync-plan.mjs';
import {planProfileReview} from '../src/member-profile-review-queue-plan.mjs';
import {planPiiRetention,__test as retentionTest} from '../src/member-d1-pii-retention-plan.mjs';
import {planSignupBenefitIssue,planSignupBenefitTransition} from '../src/member-signup-benefit-foundation.mjs';
import {buildProspectMemberIntegrationReadModel,buildMemberFamilyPassIntegrationReadModel} from '../src/member-family-pass-integration-read-model.mjs';

const prospect=buildProspectRegistrationPlan();
assert.equal(prospect.status,'ready');
assert.equal(prospect.customer_id,null);
assert.equal(prospect.customer_id_generation,false);

const isolated=buildProspectMemberIntegrationReadModel({
 member_identity_id:prospect.member_identity_id,
 prospect_id:prospect.prospect_id,
 persisted_prospect_id:prospect.prospect_id,
 persisted_member_identity_id:prospect.member_identity_id,
 member_prospect_binding_verified:true,
 consent_current:true,
 signup_benefit_state:'available',
 acquisition_source:'instagram',
 review_pending_count:0
});
assert.equal(isolated.family_pass,null);
assert.equal(isolated.memories,null);
assert.equal(isolated.passport,null);
assert.equal(isolated.todays_memory,null);
assert.equal(isolated.next_memory,null);
assert.equal(isolated.prospect_access_to_customer_memories,false);

const badPromotion=planProspectCustomerPromotion({
 member_identity_id:prospect.member_identity_id,
 prospect_id:prospect.prospect_id,
 canonical_customer_id:'12345678',
 customer_id_source:'customer_crm',
 customer_crm_record_verified:true,
 persisted_crm_customer_id:'12345678',
 persisted_crm_customer_id_source:'customer_crm',
 member_prospect_binding_verified:true,
 persisted_member_identity_id:'MID_zyxwvutsrqponmlkjihgfedcba654321',
 persisted_prospect_id:prospect.prospect_id,
 crm_promotion_trigger_verified:true,
 persisted_trigger_prospect_id:prospect.prospect_id,
 persisted_trigger_member_identity_id:prospect.member_identity_id,
 persisted_trigger_customer_id:'12345678',
 prospect_status:'prospect',
 prospect_status_binding_verified:true,
 persisted_status_prospect_id:prospect.prospect_id,
 persisted_status_member_identity_id:prospect.member_identity_id,
 persisted_prospect_status:'prospect',
 existing_customer_member_binding_count:0,
 persisted_binding_count_customer_id:'12345678',
 existing_promotion_event_count:0,
 persisted_promotion_event_count_prospect_id:prospect.prospect_id,
 persisted_promotion_event_count_member_identity_id:prospect.member_identity_id,
 persisted_promotion_event_count_customer_id:'12345678',
 consent_history_verified:true,
 persisted_consent_member_identity_id:prospect.member_identity_id,
 persisted_consent_prospect_id:prospect.prospect_id,
 acquisition_history_verified:true,
 persisted_acquisition_member_identity_id:prospect.member_identity_id,
 persisted_acquisition_prospect_id:prospect.prospect_id,
 existing_signup_benefit_count:0,
 persisted_benefit_count_member_identity_id:prospect.member_identity_id,
 persisted_benefit_count_prospect_id:prospect.prospect_id
});
assert.notEqual(badPromotion.status,'ready');
assert.equal(badPromotion.review_required,true);
assert.equal(badPromotion.promotion_allowed,false);

const sync=planProfileSync({
 subject_type:'prospect',prospect_id:prospect.prospect_id,member_identity_verified:true,
 profile_version:1,previous_profile_version:0,sync_event_id:createSyncEventId(),profile:{name:'Prospect'}
});
assert.equal(sync.status,'ready');
assert.equal(sync.send_allowed,false);
assert.equal(sync.browser_direct_google_access,false);

const review=planProfileReview({
 reason_code:'identity_mismatch',
 member_identity_id:prospect.member_identity_id,
 claimed_customer_id:'12345678',
 submitted_profile:{name:'Wrong target attempt'}
});
assert.equal(review.master_write_allowed,false);
assert.equal(review.queue_write_allowed,false);

const t0=Date.UTC(2026,0,1);
const expired=planPiiRetention({now_ms:t0+30*retentionTest.dayMs,pii_written_at_ms:t0});
assert.equal(expired.pii_access_allowed,false);
assert.equal(expired.purge_required,true);

const benefit=planSignupBenefitIssue({
 member_identity_id:prospect.member_identity_id,prospect_id:prospect.prospect_id,
 registration_completed:true,consent_current:true,existing_signup_benefit_count:0,
 member_prospect_binding_verified:true,persisted_member_identity_id:prospect.member_identity_id,persisted_prospect_id:prospect.prospect_id,
 persisted_benefit_count_member_identity_id:prospect.member_identity_id,persisted_benefit_count_prospect_id:prospect.prospect_id
});
assert.equal(benefit.status,'ready');
assert.equal(benefit.issue_allowed,false);
const doubleUse=planSignupBenefitTransition({
 current_state:'reserved',target_state:'used',
 entitlement_member_identity_id:prospect.member_identity_id,
 authenticated_member_identity_id:prospect.member_identity_id,
 canonical_customer_id:'12345678',reservation_id:'R-1',prior_redemption_count:1
});
assert.equal(doubleUse.status,'already_redeemed');
assert.equal(doubleUse.transition_allowed,false);

const familyRead=(overrides={})=>({
 member_identity_id:prospect.member_identity_id,
 canonical_customer_id:'12345678',
 family_id:'FAM-1',
 member_customer_binding_verified:true,
 persisted_member_identity_id:prospect.member_identity_id,
 persisted_member_customer_id:'12345678',
 member_status:'active',
 family_record_verified:true,
 persisted_family_record_id:'FAM-1',
 family_status:'active',
 family_link_verified:true,
 family_link_active_verified:true,
 persisted_verified_family_id:'FAM-1',
 persisted_family_customer_id:'12345678',
 memory_count_verified:true,
 published_non_deleted_memory_count:10,
 persisted_memory_count_family_id:'FAM-1',
 memory_count_published_only_verified:true,
 memory_count_deleted_excluded_verified:true,
 entitlement_schema_applied:true,
 durable_black_entitlement:false,
 durable_black_record_count:0,
 persisted_durable_black_record_count_family_id:'FAM-1',
 review_pending_count:0,
 ...overrides
});

const customer=buildMemberFamilyPassIntegrationReadModel(familyRead());
assert.equal(customer.status,'ready');
assert.equal(customer.family_pass.current_tier,'BLACK');
assert.equal(customer.black_contract.threshold,10);
assert.equal(customer.black_contract.photo_goods_discount_percent,10);
assert.equal(customer.black_contract.automatic_award,false);
assert.equal(customer.write_allowed,false);
assert.equal(customer.exact_read_evidence_verified,true);

const wrongMemberBinding=buildMemberFamilyPassIntegrationReadModel(familyRead({persisted_member_customer_id:'87654321'}));
assert.equal(wrongMemberBinding.status,'member_customer_binding_not_verified');
assert.equal(wrongMemberBinding.read_ready,false);
const wrongPersistedMember=buildMemberFamilyPassIntegrationReadModel(familyRead({persisted_member_identity_id:'MID_zyxwvutsrqponmlkjihgfedcba654321'}));
assert.equal(wrongPersistedMember.status,'member_customer_binding_not_verified');
assert.equal(wrongPersistedMember.read_ready,false);
const wrongFamilyCustomer=buildMemberFamilyPassIntegrationReadModel(familyRead({persisted_family_customer_id:'87654321'}));
assert.equal(wrongFamilyCustomer.status,'family_link_not_verified');
assert.equal(wrongFamilyCustomer.read_ready,false);
const wrongMemoryFamily=buildMemberFamilyPassIntegrationReadModel(familyRead({persisted_memory_count_family_id:'FAM-2'}));
assert.equal(wrongMemoryFamily.status,'memory_count_family_not_verified');
assert.equal(wrongMemoryFamily.read_ready,false);
const unverifiedFilters=buildMemberFamilyPassIntegrationReadModel(familyRead({memory_count_deleted_excluded_verified:false}));
assert.equal(unverifiedFilters.status,'memory_count_filter_not_verified');
assert.equal(unverifiedFilters.read_ready,false);
const crossedBlack=buildMemberFamilyPassIntegrationReadModel(familyRead({
 published_non_deleted_memory_count:4,
 durable_black_entitlement:true,
 durable_black_record_count:1,
 durable_black_record_verified:true,
 persisted_durable_black_family_id:'FAM-2',
 persisted_durable_black_lifetime:1,
 black_achieved_at:'2026-09-01T00:00:00.000Z',
 durable_black_qualifying_memory_count:10,
 persisted_durable_black_achievement_source:'published-member-memories'
}));
assert.equal(crossedBlack.status,'durable_black_family_not_verified');
assert.equal(crossedBlack.read_ready,false);

console.log('MEMBER_LIFECYCLE_CROSS_CONTRACT_SECURITY=PASS');
console.log('PROSPECT_CUSTOMER_DATA_LEAK=0');
console.log('MEMBER_CUSTOMER_ID_GENERATION=0');
console.log('AMBIGUOUS_MASTER_WRITE=0');
console.log('GOOGLE_NETWORK_SEND=0');
console.log('PII_ACCESS_AFTER_30_DAYS=0');
console.log('DOUBLE_BENEFIT_REDEMPTION=0');
console.log('FAMILY_SCOPED_MEMORY_COUNT=EXACT');
console.log('DURABLE_BLACK_CROSS_FAMILY_READ=0');
console.log('BLACK_THRESHOLD=10');
console.log('BLACK_AUTOMATIC_AWARD=0');
console.log('PRODUCTION_WRITE=0');
