import assert from 'node:assert/strict';
import {buildMemberFamilyPassIntegrationReadModel,buildProspectMemberIntegrationReadModel} from '../src/member-family-pass-integration-read-model.mjs';

const mid='MID_abcdefghijklmnopqrstuvwxyz123456';
const pid='PID_abcdefghijklmnopqrstuvwxyz123456';
const family='FAM-1';
const customer='12345678';

const base={
 member_identity_id:mid,
 canonical_customer_id:customer,
 family_id:family,
 member_customer_binding_verified:true,
 persisted_member_identity_id:mid,
 persisted_member_customer_id:customer,
 member_status:'active',
 family_record_verified:true,
 persisted_family_record_id:family,
 family_status:'active',
 family_link_verified:true,
 family_link_active_verified:true,
 persisted_verified_family_id:family,
 persisted_family_customer_id:customer,
 memory_count_verified:true,
 published_non_deleted_memory_count:10,
 persisted_memory_count_family_id:family,
 memory_count_published_only_verified:true,
 memory_count_deleted_excluded_verified:true,
 entitlement_schema_applied:true,
 durable_black_entitlement:false,
 durable_black_record_count:0,
 persisted_durable_black_record_count_family_id:family,
 review_pending_count:0
};

let r=buildMemberFamilyPassIntegrationReadModel(base);
assert.equal(r.status,'ready');
assert.equal(r.read_ready,true);
assert.equal(r.write_allowed,false);
assert.equal(r.production_write_authorized,false);
assert.equal(r.exact_read_evidence_verified,true);
assert.equal(r.family_pass.current_tier,'BLACK');
assert.equal(r.family_pass.black_currently_qualified,true);
assert.equal(r.family_pass.black_lifetime_entitled,false);
assert.equal(r.memory_scope.family_id,family);
assert.equal(r.memory_scope.exact_count,10);
assert.equal(r.durable_black_evidence.record_count,0);
assert.equal(r.black_contract.threshold,10);
assert.equal(r.black_contract.photo_goods_discount_percent,10);
assert.equal(r.black_contract.shooting_fee_discount,false);
assert.equal(r.black_contract.automatic_award,false);

assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,persisted_member_customer_id:'87654321'}).status,'member_customer_binding_not_verified');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,family_record_verified:false}).status,'family_record_not_verified');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,persisted_family_record_id:'FAM-2'}).status,'family_record_not_verified');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,family_status:'disabled'}).status,'family_record_not_verified');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,family_link_active_verified:false}).status,'family_link_not_verified');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,persisted_family_customer_id:'87654321'}).status,'family_link_not_verified');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,memory_count_verified:false}).status,'memory_count_not_verified');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,persisted_memory_count_family_id:'FAM-2'}).status,'memory_count_family_not_verified');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,memory_count_published_only_verified:false}).status,'memory_count_filter_not_verified');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,memory_count_deleted_excluded_verified:false}).status,'memory_count_filter_not_verified');
for(const malformed of ['10',' 10 ',['10'],{value:10},10n,Number.MAX_SAFE_INTEGER+1,-1]){
 assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,published_non_deleted_memory_count:malformed}).status,'invalid_memory_count');
}
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,entitlement_schema_applied:'true'}).status,'entitlement_schema_evidence_required');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,durable_black_entitlement:1}).status,'durable_black_evidence_required');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,durable_black_record_count:'0'}).status,'invalid_durable_black_record_count');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,persisted_durable_black_record_count_family_id:'FAM-2'}).status,'durable_black_count_family_not_verified');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,durable_black_record_count:2}).status,'durable_black_record_collision');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,durable_black_record_count:1,durable_black_entitlement:false}).status,'durable_black_state_mismatch');

const durable={
 ...base,
 published_non_deleted_memory_count:4,
 durable_black_entitlement:true,
 durable_black_record_count:1,
 durable_black_record_verified:true,
 persisted_durable_black_family_id:family,
 persisted_durable_black_lifetime:1,
 black_achieved_at:'2026-10-07T00:00:00Z',
 durable_black_qualifying_memory_count:10,
 persisted_durable_black_achievement_source:'published-member-memories'
};
r=buildMemberFamilyPassIntegrationReadModel(durable);
assert.equal(r.status,'ready');
assert.equal(r.family_pass.current_tier,'BLACK');
assert.equal(r.family_pass.black_currently_qualified,false);
assert.equal(r.family_pass.black_lifetime_entitled,true);
assert.equal(r.family_pass.tier_basis,'durable_black_entitlement');
assert.equal(r.durable_black_evidence.record_verified,true);
assert.equal(r.durable_black_evidence.qualifying_memory_count,10);
assert.equal(r.durable_black_evidence.achievement_source,'published-member-memories');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...durable,durable_black_record_verified:false}).status,'durable_black_family_not_verified');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...durable,persisted_durable_black_family_id:'FAM-2'}).status,'durable_black_family_not_verified');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...durable,persisted_durable_black_lifetime:'1'}).status,'invalid_durable_black_evidence');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...durable,black_achieved_at:'2026-10-07'}).status,'invalid_durable_black_evidence');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...durable,durable_black_qualifying_memory_count:'10'}).status,'invalid_durable_black_evidence');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...durable,durable_black_qualifying_memory_count:9}).status,'invalid_durable_black_evidence');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...durable,persisted_durable_black_achievement_source:'manual'}).status,'invalid_durable_black_evidence');

r=buildMemberFamilyPassIntegrationReadModel({...base,entitlement_schema_applied:false,durable_black_entitlement:false,durable_black_record_count:undefined,persisted_durable_black_record_count_family_id:''});
assert.equal(r.status,'ready');
assert.equal(r.family_pass.black_lifetime_persistence_supported,false);
assert.equal(r.durable_black_evidence.record_count,null);
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,entitlement_schema_applied:false,durable_black_entitlement:true}).status,'durable_black_schema_conflict');

assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,review_pending_count:'0'}).status,'invalid_review_count');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,member_identity_id:[mid]}).status,'invalid_identity');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,canonical_customer_id:12345678}).status,'invalid_identity');
assert.equal(buildMemberFamilyPassIntegrationReadModel({...base,family_id:' FAM-1 '}).status,'invalid_identity');

const prospect=buildProspectMemberIntegrationReadModel({
 member_identity_id:mid,
 prospect_id:pid,
 persisted_member_identity_id:mid,
 persisted_prospect_id:pid,
 member_prospect_binding_verified:true,
 review_pending_count:0
});
assert.equal(prospect.status,'ready');
assert.equal(prospect.family_pass,null);
assert.equal(prospect.memories,null);
assert.equal(prospect.prospect_access_to_customer_memories,false);
assert.equal(prospect.customer_id_generation,false);
assert.equal(prospect.write_allowed,false);
assert.equal(buildProspectMemberIntegrationReadModel({...prospect,member_identity_id:mid,prospect_id:pid,persisted_member_identity_id:mid,persisted_prospect_id:pid,member_prospect_binding_verified:true,review_pending_count:'0'}).status,'invalid_review_count');

console.log('MEMBER_FAMILY_PASS_STEP8_READ_EVIDENCE=PASS');
console.log('FAMILY_SCOPED_MEMORY_COUNT=EXACT');
console.log('DURABLE_BLACK_ROW_CARDINALITY=EXACT');
console.log('NUMERIC_STRING_COERCION=0');
console.log('BLACK_AUTOMATIC_AWARD=0');
console.log('PRODUCTION_WRITE=0');
