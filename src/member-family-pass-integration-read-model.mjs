import {computeCurrentFamilyPass} from './member-family-pass-read-model.mjs';

const MID=/^MID_[A-Za-z0-9_-]{22,}$/;
const CID=/^\d{8}$/;
const text=v=>v==null?'':String(v).trim();

export function buildMemberFamilyPassIntegrationReadModel({
 member_identity_id,
 canonical_customer_id,
 family_id,
 family_link_verified=false,
 member_customer_binding_verified=false,
 persisted_member_customer_id='',
 member_status,
 published_non_deleted_memory_count,
 durable_black_entitlement=false,
 entitlement_schema_applied,
 black_achieved_at='',
 consent_current=false,
 signup_benefit_state='none',
 acquisition_source='unknown',
 review_pending_count=0
}={}){
 const member=text(member_identity_id),customer=text(canonical_customer_id),family=text(family_id);
 if(!MID.test(member)||!CID.test(customer)||!family) return {status:'invalid_identity',read_ready:false};
 if(member_customer_binding_verified!==true||text(persisted_member_customer_id)!==customer) return {status:'member_customer_binding_not_verified',read_ready:false};
 if(!['active','customer'].includes(text(member_status))) return {status:'member_status_not_eligible',read_ready:false};
 if(family_link_verified!==true) return {status:'family_link_not_verified',read_ready:false};
 if(published_non_deleted_memory_count===undefined||published_non_deleted_memory_count===null||typeof published_non_deleted_memory_count==='boolean'||String(published_non_deleted_memory_count).trim()==='') return {status:'missing_memory_count_evidence',read_ready:false};
 const count=Number(published_non_deleted_memory_count);
 if(!Number.isInteger(count)||count<0) return {status:'invalid_memory_count',read_ready:false};
 if(typeof entitlement_schema_applied!=='boolean') return {status:'entitlement_schema_evidence_required',read_ready:false};
 const pending=Number(review_pending_count);
 if(!Number.isInteger(pending)||pending<0) return {status:'invalid_review_count',read_ready:false};

 const pass=computeCurrentFamilyPass(count,{
  durable_black_entitlement:durable_black_entitlement===true,
  entitlement_schema_applied,
  black_achieved_at:text(black_achieved_at)
 });

 return {
  status:'ready',
  member_identity_id:member,
  canonical_customer_id:customer,
  family_id:family,
  family_pass:pass,
  family_pass_source:'existing_member_family_pass_read_model',
  memory_scope:{
    family_scoped:true,
    published_only:true,
    deleted_hidden:true,
    one_memory_equals_one_shoot:true
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
  signup_benefit_state:text(signup_benefit_state)||'none',
  acquisition_source:text(acquisition_source)||'unknown',
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
 consent_current=false,
 signup_benefit_state='none',
 acquisition_source='unknown',
 review_pending_count=0
}={}){
 const PID=/^PID_[A-Za-z0-9_-]{22,}$/;
 const member=text(member_identity_id),prospect=text(prospect_id);
 if(!MID.test(member)||!PID.test(prospect)) return {status:'invalid_identity',read_ready:false};
 const pending=Number(review_pending_count);
 if(!Number.isInteger(pending)||pending<0) return {status:'invalid_review_count',read_ready:false};
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
  signup_benefit_state:text(signup_benefit_state)||'none',
  acquisition_source:text(acquisition_source)||'unknown',
  review_pending_count:pending,
  prospect_access_to_customer_memories:false,
  customer_id_generation:false,
  write_allowed:false,
  read_ready:true
 };
}
