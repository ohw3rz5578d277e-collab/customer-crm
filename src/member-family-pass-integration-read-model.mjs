import {computeCurrentFamilyPass} from './member-family-pass-read-model.mjs';

const MID=/^MID_[A-Za-z0-9_-]{22,}$/;
const PID=/^PID_[A-Za-z0-9_-]{22,}$/;
const CID=/^\d{8}$/;
const MAX_FAMILY_ID=128;
const BLACK_SOURCE='published-member-memories';
const strictText=value=>typeof value==='string'&&value.trim()===value?value:null;
const strictNonNegativeInteger=value=>typeof value==='number'&&Number.isSafeInteger(value)&&value>=0?value:null;
const exact=(value,expected)=>strictText(value)!==null&&value===expected;
const validFamilyId=value=>{
 const family=strictText(value);
 return family!==null&&family.length>0&&family.length<=MAX_FAMILY_ID?family:null;
};
const validUtcTimestamp=value=>{
 const timestamp=strictText(value);
 if(timestamp===null||!/^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{3})?Z$/.test(timestamp)) return null;
 return Number.isFinite(Date.parse(timestamp))?timestamp:null;
};
const blocked=status=>({status,read_ready:false,write_allowed:false,production_write_authorized:false});

export function buildMemberFamilyPassIntegrationReadModel({
 member_identity_id,
 canonical_customer_id,
 family_id,
 member_customer_binding_verified=false,
 persisted_member_identity_id='',
 persisted_member_customer_id='',
 member_status,
 family_record_verified=false,
 persisted_family_record_id='',
 family_status='',
 family_link_verified=false,
 family_link_active_verified=false,
 persisted_verified_family_id='',
 persisted_family_customer_id='',
 memory_count_verified=false,
 published_non_deleted_memory_count,
 persisted_memory_count_family_id='',
 memory_count_published_only_verified=false,
 memory_count_deleted_excluded_verified=false,
 entitlement_schema_applied,
 durable_black_entitlement,
 durable_black_record_count,
 persisted_durable_black_record_count_family_id='',
 durable_black_record_verified=false,
 persisted_durable_black_family_id='',
 persisted_durable_black_lifetime,
 black_achieved_at='',
 durable_black_qualifying_memory_count,
 persisted_durable_black_achievement_source='',
 consent_current=false,
 signup_benefit_state='none',
 acquisition_source='unknown',
 review_pending_count=0
}={}){
 const member=strictText(member_identity_id),customer=strictText(canonical_customer_id),family=validFamilyId(family_id);
 if(member===null||customer===null||!MID.test(member)||!CID.test(customer)||family===null) return blocked('invalid_identity');

 if(
  member_customer_binding_verified!==true||
  !exact(persisted_member_identity_id,member)||
  !exact(persisted_member_customer_id,customer)
 ) return blocked('member_customer_binding_not_verified');
 const normalizedMemberStatus=strictText(member_status);
 if(normalizedMemberStatus===null||!['active','customer'].includes(normalizedMemberStatus)) return blocked('member_status_not_eligible');

 if(
  family_record_verified!==true||
  !exact(persisted_family_record_id,family)||
  strictText(family_status)!=='active'
 ) return blocked('family_record_not_verified');
 if(
  family_link_verified!==true||
  family_link_active_verified!==true||
  !exact(persisted_verified_family_id,family)||
  !exact(persisted_family_customer_id,customer)
 ) return blocked('family_link_not_verified');

 if(memory_count_verified!==true) return blocked('memory_count_not_verified');
 if(!exact(persisted_memory_count_family_id,family)) return blocked('memory_count_family_not_verified');
 if(memory_count_published_only_verified!==true||memory_count_deleted_excluded_verified!==true){
  return blocked('memory_count_filter_not_verified');
 }
 const count=strictNonNegativeInteger(published_non_deleted_memory_count);
 if(count===null) return blocked('invalid_memory_count');

 if(typeof entitlement_schema_applied!=='boolean') return blocked('entitlement_schema_evidence_required');
 if(typeof durable_black_entitlement!=='boolean') return blocked('durable_black_evidence_required');

 let durableBlack=false;
 let durableRecordCount=null;
 let achievedAt='';
 let qualifyingCount=null;
 if(entitlement_schema_applied===false){
  if(durable_black_entitlement===true) return blocked('durable_black_schema_conflict');
 }else{
  durableRecordCount=strictNonNegativeInteger(durable_black_record_count);
  if(durableRecordCount===null) return blocked('invalid_durable_black_record_count');
  if(!exact(persisted_durable_black_record_count_family_id,family)) return blocked('durable_black_count_family_not_verified');
  if(durableRecordCount>1) return blocked('durable_black_record_collision');
  if(durableRecordCount===0){
   if(durable_black_entitlement!==false) return blocked('durable_black_state_mismatch');
  }else{
   if(durable_black_entitlement!==true) return blocked('durable_black_state_mismatch');
   if(durable_black_record_verified!==true||!exact(persisted_durable_black_family_id,family)) return blocked('durable_black_family_not_verified');
   if(persisted_durable_black_lifetime!==1) return blocked('invalid_durable_black_evidence');
   achievedAt=validUtcTimestamp(black_achieved_at)||'';
   qualifyingCount=strictNonNegativeInteger(durable_black_qualifying_memory_count);
   if(
    !achievedAt||
    qualifyingCount===null||qualifyingCount<10||
    strictText(persisted_durable_black_achievement_source)!==BLACK_SOURCE
   ) return blocked('invalid_durable_black_evidence');
   durableBlack=true;
  }
 }

 const pending=strictNonNegativeInteger(review_pending_count);
 if(pending===null) return blocked('invalid_review_count');

 const pass=computeCurrentFamilyPass(count,{
  durable_black_entitlement:durableBlack,
  entitlement_schema_applied,
  black_achieved_at:achievedAt
 });
 const signupState=strictText(signup_benefit_state);
 const acquisition=strictText(acquisition_source);

 return {
  status:'ready',
  member_identity_id:member,
  canonical_customer_id:customer,
  family_id:family,
  family_pass:pass,
  family_pass_source:'existing_member_family_pass_read_model',
  exact_read_evidence_verified:true,
  memory_scope:{
   family_scoped:true,
   family_id:family,
   published_only:true,
   deleted_hidden:true,
   one_memory_equals_one_shoot:true,
   exact_count:count
  },
  durable_black_evidence:{
   schema_applied:entitlement_schema_applied,
   record_count:durableRecordCount,
   family_scoped:entitlement_schema_applied===true,
   record_verified:durableBlack,
   lifetime_entitled:durableBlack,
   black_achieved_at:durableBlack?achievedAt:null,
   qualifying_memory_count:durableBlack?qualifyingCount:null,
   achievement_source:durableBlack?BLACK_SOURCE:null
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
  signup_benefit_state:signupState||'none',
  acquisition_source:acquisition||'unknown',
  review_pending_count:pending,
  review_required:pending>0,
  passport_source:'existing_member_family_passport_read_model',
  todays_memory_source:'existing_member_todays_memory_read_model',
  next_memory_source:'existing_member_next_memory_read_model',
  prospect_access_to_customer_memories:false,
  write_allowed:false,
  production_write_authorized:false,
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
 if(member===null||prospect===null||!MID.test(member)||!PID.test(prospect)) return blocked('invalid_identity');
 if(member_prospect_binding_verified!==true||!exact(persisted_prospect_id,prospect)||!exact(persisted_member_identity_id,member)) return blocked('member_prospect_binding_not_verified');
 const pending=strictNonNegativeInteger(review_pending_count);
 if(pending===null) return blocked('invalid_review_count');
 const signupState=strictText(signup_benefit_state);
 const acquisition=strictText(acquisition_source);
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
  signup_benefit_state:signupState||'none',
  acquisition_source:acquisition||'unknown',
  review_pending_count:pending,
  prospect_access_to_customer_memories:false,
  customer_id_generation:false,
  write_allowed:false,
  production_write_authorized:false,
  read_ready:true
 };
}

export const __test={MID,PID,CID,BLACK_SOURCE,strictText,strictNonNegativeInteger,validFamilyId,validUtcTimestamp};
