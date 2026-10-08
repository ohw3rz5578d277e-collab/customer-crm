const text=v=>v==null?'':String(v).trim();
const MID=/^MID_[A-Za-z0-9_-]{22,}$/;
const PID=/^PID_[A-Za-z0-9_-]{22,}$/;
const CID=/^\d{8}$/;

const scalarNonNegativeInteger=v=>{
  if(typeof v==='number') return Number.isSafeInteger(v)&&v>=0?v:null;
  if(typeof v==='string'&&/^(?:0|[1-9]\d*)$/.test(v.trim())){
    const n=Number(v.trim());
    return Number.isSafeInteger(n)&&n>=0?n:null;
  }
  return null;
};

function baseBlocked(status,extra={}){
  return {
    status,
    exact_binding_verified:false,
    review_required:false,
    customer_id_generation:false,
    family_id_generation:false,
    fuzzy_identity_linking:false,
    write_allowed:false,
    execution_requires_separate_gate:true,
    ...extra
  };
}

function verifyActiveMemberIdentity({member,verified,persisted_id,status}){
  if(verified!==true||text(persisted_id)!==member) return 'member_identity_record_not_verified';
  if(text(status)!=='active') return 'member_identity_not_active';
  return null;
}

export function planExactCustomerMemberFamilyBinding({
  member_identity_id,
  canonical_customer_id,
  family_id,
  member_identity_record_verified=false,
  persisted_member_identity_record_id='',
  member_identity_status='',
  customer_id_source,
  customer_record_verified=false,
  persisted_customer_record_id='',
  member_customer_binding_verified=false,
  persisted_member_identity_id='',
  persisted_member_customer_id='',
  active_member_customer_binding_count,
  persisted_member_binding_count_customer_id='',
  family_record_verified=false,
  persisted_family_record_id='',
  family_status='',
  family_link_verified=false,
  persisted_family_id='',
  persisted_family_customer_id='',
  active_family_link_count,
  persisted_family_link_count_customer_id=''
}={}){
  const member=text(member_identity_id);
  const customer=text(canonical_customer_id);
  const family=text(family_id);

  if(!MID.test(member)||!CID.test(customer)||!family){
    return baseBlocked('invalid_identity');
  }
  const memberRecordFailure=verifyActiveMemberIdentity({
    member,
    verified:member_identity_record_verified,
    persisted_id:persisted_member_identity_record_id,
    status:member_identity_status
  });
  if(memberRecordFailure) return baseBlocked(memberRecordFailure,{review_required:true});
  if(text(customer_id_source)!=='customer_crm'){
    return baseBlocked('invalid_customer_id_source');
  }
  if(customer_record_verified!==true||text(persisted_customer_record_id)!==customer){
    return baseBlocked('customer_record_not_verified');
  }
  if(
    member_customer_binding_verified!==true||
    text(persisted_member_identity_id)!==member||
    text(persisted_member_customer_id)!==customer
  ){
    return baseBlocked('member_customer_binding_not_verified',{review_required:true});
  }

  if(
    active_member_customer_binding_count===undefined||
    active_member_customer_binding_count===null||
    typeof active_member_customer_binding_count==='boolean'||
    String(active_member_customer_binding_count).trim()===''
  ){
    return baseBlocked('missing_member_binding_count_evidence');
  }
  if(text(persisted_member_binding_count_customer_id)!==customer){
    return baseBlocked('member_binding_count_customer_not_verified',{review_required:true});
  }
  const memberBindingCount=scalarNonNegativeInteger(active_member_customer_binding_count);
  if(memberBindingCount===null){
    return baseBlocked('invalid_member_binding_count');
  }
  if(memberBindingCount!==1){
    return baseBlocked('ambiguous_member_customer_binding',{review_required:true});
  }

  if(family_record_verified!==true||text(persisted_family_record_id)!==family){
    return baseBlocked('family_record_not_verified',{review_required:true});
  }
  if(text(family_status)!=='active'){
    return baseBlocked('family_not_active',{review_required:true});
  }
  if(
    family_link_verified!==true||
    text(persisted_family_id)!==family||
    text(persisted_family_customer_id)!==customer
  ){
    return baseBlocked('family_link_not_verified',{review_required:true});
  }
  if(
    active_family_link_count===undefined||
    active_family_link_count===null||
    typeof active_family_link_count==='boolean'||
    String(active_family_link_count).trim()===''
  ){
    return baseBlocked('missing_family_link_count_evidence');
  }
  if(text(persisted_family_link_count_customer_id)!==customer){
    return baseBlocked('family_link_count_customer_not_verified',{review_required:true});
  }
  const familyLinkCount=scalarNonNegativeInteger(active_family_link_count);
  if(familyLinkCount===null){
    return baseBlocked('invalid_family_link_count');
  }
  if(familyLinkCount!==1){
    return baseBlocked('ambiguous_family_link',{review_required:true});
  }

  return {
    status:'ready',
    subject_type:'customer',
    member_identity_id:member,
    canonical_customer_id:customer,
    family_id:family,
    member_identity_status:'active',
    family_status:'active',
    customer_id_source:'customer_crm',
    customer_record_verified:true,
    member_customer_binding_verified:true,
    family_link_verified:true,
    active_member_customer_binding_count:1,
    active_family_link_count:1,
    exact_binding_verified:true,
    review_required:false,
    customer_id_generation:false,
    family_id_generation:false,
    fuzzy_identity_linking:false,
    automatic_merge:false,
    write_allowed:false,
    execution_requires_separate_gate:true
  };
}

export function planExactProspectMemberBinding({
  member_identity_id,
  prospect_id,
  canonical_customer_id,
  family_id,
  persisted_promoted_customer_id,
  member_identity_record_verified=false,
  persisted_member_identity_record_id='',
  member_identity_status='',
  prospect_status,
  member_prospect_binding_verified=false,
  persisted_member_identity_id='',
  persisted_prospect_id='',
  active_member_prospect_binding_count,
  persisted_member_binding_count_prospect_id=''
}={}){
  const member=text(member_identity_id);
  const prospect=text(prospect_id);

  if(!MID.test(member)||!PID.test(prospect)){
    return baseBlocked('invalid_identity');
  }
  if(
    canonical_customer_id===undefined||
    family_id===undefined||
    persisted_promoted_customer_id===undefined
  ){
    return baseBlocked('missing_prospect_scope_evidence');
  }
  if(
    canonical_customer_id!==null||
    family_id!==null||
    persisted_promoted_customer_id!==null
  ){
    return baseBlocked('prospect_scope_violation',{review_required:true});
  }
  const memberRecordFailure=verifyActiveMemberIdentity({
    member,
    verified:member_identity_record_verified,
    persisted_id:persisted_member_identity_record_id,
    status:member_identity_status
  });
  if(memberRecordFailure) return baseBlocked(memberRecordFailure,{review_required:true});
  if(text(prospect_status)!=='prospect'){
    return baseBlocked('invalid_prospect_state');
  }
  if(
    member_prospect_binding_verified!==true||
    text(persisted_member_identity_id)!==member||
    text(persisted_prospect_id)!==prospect
  ){
    return baseBlocked('member_prospect_binding_not_verified',{review_required:true});
  }
  if(
    active_member_prospect_binding_count===undefined||
    active_member_prospect_binding_count===null||
    typeof active_member_prospect_binding_count==='boolean'||
    String(active_member_prospect_binding_count).trim()===''
  ){
    return baseBlocked('missing_prospect_binding_count_evidence');
  }
  if(text(persisted_member_binding_count_prospect_id)!==prospect){
    return baseBlocked('prospect_binding_count_subject_not_verified',{review_required:true});
  }
  const bindingCount=scalarNonNegativeInteger(active_member_prospect_binding_count);
  if(bindingCount===null){
    return baseBlocked('invalid_prospect_binding_count');
  }
  if(bindingCount!==1){
    return baseBlocked('ambiguous_member_prospect_binding',{review_required:true});
  }

  return {
    status:'ready',
    subject_type:'prospect',
    member_identity_id:member,
    prospect_id:prospect,
    canonical_customer_id:null,
    family_id:null,
    promoted_customer_id:null,
    member_identity_status:'active',
    member_prospect_binding_verified:true,
    active_member_prospect_binding_count:1,
    exact_binding_verified:true,
    review_required:false,
    prospect_access_to_customer_data:false,
    customer_id_generation:false,
    family_id_generation:false,
    fuzzy_identity_linking:false,
    automatic_merge:false,
    write_allowed:false,
    execution_requires_separate_gate:true
  };
}

export const __test={MID,PID,CID,scalarNonNegativeInteger};
