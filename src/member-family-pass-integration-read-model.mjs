import {computeCurrentFamilyPass} from './member-family-pass-read-model.mjs';

const MID=/^MID_[A-Za-z0-9_-]{22,}$/;
const PID=/^PID_[A-Za-z0-9_-]{22,}$/;
const CID=/^\d{8}$/;
const BLACK_ACHIEVEMENT_SOURCE='published-member-memories';
const UTC_INSTANT_RE=/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
const strictText=v=>typeof v==='string'?v.trim():'';
const scalarNonNegativeInteger=v=>typeof v==='number'&&Number.isSafeInteger(v)&&v>=0?v:null;
const canonicalUtcInstant=value=>{
 if(typeof value!=='string'||!UTC_INSTANT_RE.test(value)) return false;
 const ms=Date.parse(value);
 return Number.isFinite(ms)&&new Date(ms).toISOString()===value;
};

export function buildMemberFamilyPassIntegrationReadModel({
 member_identity_id,
 canonical_customer_id,
 family_id,
 family_link_verified=false,
 persisted_verified_family_id='',
 persisted_family_customer_id='',
 member_customer_binding_verified=false,
 persisted_member_identity_id='',
 persisted_member_customer_id='',
 member_status,
 memory_count_verified=false,
 published_non_deleted_memory_count,
 persisted_memory_count_family_id='',
 entitlement_schema_verified=false,
 entitlement_schema_applied,
 durable_black_record_verified=false,
 persisted_durable_black_query_family_id='',
 durable_black_record_count,
 durable_black_entitlement,
 persisted_durable_black_family_id='',
 black_achieved_at='',
 durable_black_qualifying_memory_count,
 durable_black_achievement_source='',
 consent_current=false,
 signup_benefit_state='none',
 acquisition_source='unknown',
 review_pending_count=0
}={}){
 const member=strictText(member_identity_id),customer=strictText(canonical_customer_id),family=strictText(family_id);
 if(!MID.test(member)||!CID.test(customer)||!family) return {status:'invalid_identity',read_ready:false};
 if(member_customer_binding_verified!==true||strictText(persisted_member_identity_id)!==member||strictText(persisted_member_customer_id)!==customer) return {status:'member_customer_binding_not_verified',read_ready:false};
 if(!['active','customer'].includes(strictText(member_status))) return {status:'member_status_not_eligible',read_ready:false};
 if(family_link_verified!==true||strictText(persisted_verified_family_id)!==family||strictText(persisted_family_customer_id)!==customer) return {status:'family_link_not_verified',read_ready:false};
 if(memory_count_verified!==true) return {status:'memory_count_not_verified',read_ready:false};
 if(strictText(persisted_memory_count_family_id)!==family) return {status:'memory_count_family_not_verified',read_ready:false};
 if(published_non_deleted_memory_count===undefined||published_non_deleted_memory_count===null||published_non_deleted_memory_count==='') return {status:'missing_memory_count_evidence',read_ready:false};
 const count=scalarNonNegativeInteger(published_non_deleted_memory_count);
 if(count===null) return {status:'invalid_memory_count',read_ready:false};

 if(entitlement_schema_verified!==true) return {status:'entitlement_schema_not_verified',read_ready:false};
 if(typeof entitlement_schema_applied!=='boolean') return {status:'entitlement_schema_evidence_required',read_ready:false};
 if(typeof durable_black_entitlement!=='boolean') return {status:'durable_black_evidence_required',read_ready:false};
 if(entitlement_schema_applied===false&&durable_black_entitlement===true) return {status:'durable_black_schema_conflict',read_ready:false};

 let durableBlack=false;
 let durableRecordCount=null;
 let durableAchievedAt='';
 let durableQualifyingCount=null;
 let durableSource=null;
 if(entitlement_schema_applied===true){
  if(durable_black_record_verified!==true) return {status:'durable_black_record_not_verified',read_ready:false};
  if(strictText(persisted_durable_black_query_family_id)!==family) return {status:'durable_black_query_family_not_verified',read_ready:false};
  durableRecordCount=scalarNonNegativeInteger(durable_black_record_count);
  if(durableRecordCount===null||durableRecordCount>1) return {status:'invalid_durable_black_record_count',read_ready:false};
  if(durable_black_entitlement===true&&durableRecordCount!==1) return {status:'durable_black_record_count_mismatch',read_ready:false};
  if(durable_black_entitlement===false&&durableRecordCount!==0) return {status:'durable_black_record_count_mismatch',read_ready:false};
 }
 if(durable_black_entitlement===true){
  if(strictText(persisted_durable_black_family_id)!==family) return {status:'durable_black_family_not_verified',read_ready:false};
  durableAchievedAt=strictText(black_achieved_at);
  durableQualifyingCount=scalarNonNegativeInteger(durable_black_qualifying_memory_count);
  durableSource=strictText(durable_black_achievement_source);
  if(
   entitlement_schema_applied!==true
   ||!canonicalUtcInstant(durableAchievedAt)
   ||durableQualifyingCount===null
   ||durableQualifyingCount<10
   ||durableSource!==BLACK_ACHIEVEMENT_SOURCE
  ) return {status:'invalid_durable_black_evidence',read_ready:false};
  durableBlack=true;
 }
 const pending=scalarNonNegativeInteger(review_pending_count);
 if(pending===null) return {status:'invalid_review_count',read_ready:false};

 const pass=computeCurrentFamilyPass(count,{
  durable_black_entitlement:durableBlack,
  entitlement_schema_applied,
  black_achieved_at:durableAchievedAt
 });

 return {
  status:'ready',
  member_identity_id:member,
  canonical_customer_id:customer,
  family_id:family,
  family_pass:pass,
  family_pass_source:'existing_member_family_pass_read_model',
  memory_scope:{
    verified:true,
    family_id:family,
    memory_count:count,
    family_scoped:true,
    published_only:true,
    deleted_hidden:true,
    one_memory_equals_one_shoot:true
  },
  durable_black_evidence:{
    schema_verified:true,
    schema_applied:entitlement_schema_applied,
    query_family_id:entitlement_schema_applied?family:null,
    record_verified:entitlement_schema_applied?true:false,
    record_count:entitlement_schema_applied?durableRecordCount:null,
    durable_black:durableBlack,
    family_id:durableBlack?family:null,
    black_achieved_at:durableBlack?durableAchievedAt:null,
    qualifying_memory_count:durableBlack?durableQualifyingCount:null,
    achievement_source:durableBlack?durableSource:null
  },
  black_contract:{
    threshold:10,
    lifetime_entitlement:pass.black_lifetime_entitled,
    photo_goods_discount_percent:10,
    shooting_fee_discount:false,
    automatic_award:false,
    automatic_enforcement:false
  },
  consent_current:consent_current===true,
  signup_benefit_state:strictText(signup_benefit_state)||'none',
  acquisition_source:strictText(acquisition_source)||'unknown',
  review_pending_count:pending,
  review_required:pending>0,
  passport_source:'existing_member_family_passport_read_model',
  todays_memory_source:'existing_member_todays_memory_read_model',
  next_memory_source:'existing_member_next_memory_read_model',
  prospect_access_to_customer_memories:false,
  write_allowed:false,
  read_ready:true
 };
}

export function buildProspectMemberIntegrationReadModel({
 member_identity_id,
 prospect_id,
 persisted_prospect_id='',
 persisted_member_identity_id='',
 member_prospect_binding_verified=false,
 consent_current=false,
 signup_benefit_state='none',
 acquisition_source='unknown',
 review_pending_count=0
}={}){
 const member=strictText(member_identity_id),prospect=strictText(prospect_id);
 if(!MID.test(member)||!PID.test(prospect)) return {status:'invalid_identity',read_ready:false};
 if(member_prospect_binding_verified!==true||strictText(persisted_prospect_id)!==prospect||strictText(persisted_member_identity_id)!==member) return {status:'member_prospect_binding_not_verified',read_ready:false};
 const pending=scalarNonNegativeInteger(review_pending_count);
 if(pending===null) return {status:'invalid_review_count',read_ready:false};
 return {
  status:'ready',
  member_identity_id:member,
  prospect_id:prospect,
  canonical_customer_id:null,
  family_id:null,
  family_pass:null,
  memories:null,
  passport:null,
  todays_memory:null,
  next_memory:null,
  consent_current:consent_current===true,
  signup_benefit_state:strictText(signup_benefit_state)||'none',
  acquisition_source:strictText(acquisition_source)||'unknown',
  review_pending_count:pending,
  prospect_access_to_customer_memories:false,
  customer_id_generation:false,
  write_allowed:false,
  read_ready:true
 };
}

export const __test={
 BLACK_ACHIEVEMENT_SOURCE,
 scalarNonNegativeInteger,
 canonicalUtcInstant
};
