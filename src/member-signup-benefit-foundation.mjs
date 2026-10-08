import crypto from 'node:crypto';
const MID=/^MID_[A-Za-z0-9_-]{22,}$/;
const PID=/^PID_[A-Za-z0-9_-]{22,}$/;
const CID=/^\d{8}$/;
const BEN=/^BEN_[A-Za-z0-9_-]{22,}$/;
const text=v=>v==null?'':String(v).trim();
const scalarNonNegativeInteger=v=>{
 if(typeof v==='number') return Number.isSafeInteger(v)&&v>=0?v:null;
 if(typeof v==='string'&&/^(?:0|[1-9]\d*)$/.test(v.trim())){
  const n=Number(v.trim());
  return Number.isSafeInteger(n)&&n>=0?n:null;
 }
 return null;
};

export function createBenefitEntitlementId(){
 return 'BEN_'+crypto.randomBytes(24).toString('base64url');
}

export function planSignupBenefitIssue({
 member_identity_id,
 prospect_id,
 registration_completed=false,
 persisted_prospect_id='',
 persisted_member_identity_id='',
 member_prospect_binding_verified=false,
 consent_current=false,
 existing_signup_benefit_count,
 persisted_benefit_count_member_identity_id='',
 persisted_benefit_count_prospect_id=''
}={}){
 const member=text(member_identity_id),prospect=text(prospect_id);
 if(!MID.test(member)||!PID.test(prospect)) return {status:'invalid_identity',issue_allowed:false};
 if(member_prospect_binding_verified!==true||text(persisted_prospect_id)!==prospect||text(persisted_member_identity_id)!==member) return {status:'member_prospect_binding_not_verified',issue_allowed:false};
 if(registration_completed!==true||consent_current!==true) return {status:'registration_not_complete',issue_allowed:false};
 if(existing_signup_benefit_count===undefined||existing_signup_benefit_count===null||typeof existing_signup_benefit_count==='boolean'||String(existing_signup_benefit_count).trim()==='') return {status:'missing_existing_benefit_evidence',issue_allowed:false};
 if(text(persisted_benefit_count_member_identity_id)!==member||text(persisted_benefit_count_prospect_id)!==prospect) return {status:'benefit_count_binding_not_verified',issue_allowed:false};
 const count=scalarNonNegativeInteger(existing_signup_benefit_count);
 if(count===null) return {status:'invalid_existing_benefit_count',issue_allowed:false};
 if(count!==0) return {status:'already_issued',issue_allowed:false};
 return {
  status:'ready',
  entitlement_id:createBenefitEntitlementId(),
  member_identity_id:member,
  prospect_id:prospect,
  canonical_customer_id:null,
  state:'issued',
  commercial_definition:null,
  automatic_discount:false,
  issue_allowed:false,
  execution_requires_separate_gate:true
 };
}

export function planSignupBenefitTransition({
 current_state,
 target_state,
 entitlement_id='',
 entitlement_member_identity_id,
 authenticated_member_identity_id,
 canonical_customer_id='',
 reservation_id='',
 prior_redemption_count,
 persisted_redemption_entitlement_id='',
 entitlement_member_binding_verified=false,
 persisted_entitlement_id='',
 persisted_entitlement_member_identity_id='',
 entitlement_state_binding_verified=false,
 persisted_entitlement_state='',
 reservation_context_binding_verified=false,
 persisted_reservation_context_member_identity_id='',
 persisted_reservation_context_customer_id='',
 persisted_reservation_context_reservation_id='',
 redemption_context_binding_verified=false,
 persisted_redemption_member_identity_id='',
 persisted_redemption_customer_id='',
 persisted_redemption_reservation_id=''
}={}){
 const current=text(current_state),target=text(target_state);
 const entitlement=text(entitlement_id),member=text(entitlement_member_identity_id),auth=text(authenticated_member_identity_id);
 if(!MID.test(member)) return {status:'invalid_entitlement_member_identity',transition_allowed:false};
 if(!BEN.test(entitlement)) return {status:'transition_entitlement_required',transition_allowed:false};
 if(entitlement_member_binding_verified!==true||text(persisted_entitlement_id)!==entitlement||text(persisted_entitlement_member_identity_id)!==member) return {status:'entitlement_member_binding_not_verified',transition_allowed:false};
 if(entitlement_state_binding_verified!==true||text(persisted_entitlement_id)!==entitlement||text(persisted_entitlement_state)!==current) return {status:'entitlement_state_binding_not_verified',transition_allowed:false};

 const allowed={
  issued:new Set(['available','expired','revoked']),
  available:new Set(['reserved','expired','revoked']),
  reserved:new Set(['available','used','expired','revoked']),
  used:new Set([]),
  expired:new Set([]),
  revoked:new Set([])
 };
 if(!allowed[current]||!allowed[current].has(target)) return {status:'invalid_transition',transition_allowed:false};
 if(target==='reserved'){
   if(member!==auth) return {status:'member_identity_mismatch',transition_allowed:false};
   if(!CID.test(text(canonical_customer_id))||!text(reservation_id)) return {status:'reservation_context_required',transition_allowed:false};
   if(reservation_context_binding_verified!==true||text(persisted_reservation_context_member_identity_id)!==auth||text(persisted_reservation_context_customer_id)!==text(canonical_customer_id)||text(persisted_reservation_context_reservation_id)!==text(reservation_id)) return {status:'reservation_context_binding_not_verified',transition_allowed:false};
 }
 if(target==='used'){
   if(member!==auth) return {status:'member_identity_mismatch',transition_allowed:false};
   if(text(persisted_entitlement_member_identity_id)!==auth) return {status:'entitlement_member_binding_not_verified',transition_allowed:false};
   if(prior_redemption_count===undefined||prior_redemption_count===null||typeof prior_redemption_count==='boolean'||String(prior_redemption_count).trim()==='') return {status:'missing_redemption_evidence',transition_allowed:false};
   if(text(persisted_redemption_entitlement_id)!==entitlement) return {status:'redemption_count_binding_not_verified',transition_allowed:false};
   const redemptions=scalarNonNegativeInteger(prior_redemption_count);
   if(redemptions===null) return {status:'invalid_redemption_count',transition_allowed:false};
   if(redemptions!==0) return {status:'already_redeemed',transition_allowed:false};
   if(!CID.test(text(canonical_customer_id))||!text(reservation_id)) return {status:'redemption_context_required',transition_allowed:false};
   if(redemption_context_binding_verified!==true||text(persisted_redemption_member_identity_id)!==auth||text(persisted_redemption_customer_id)!==text(canonical_customer_id)||text(persisted_redemption_reservation_id)!==text(reservation_id)) return {status:'redemption_context_binding_not_verified',transition_allowed:false};
 }
 return {
  status:'ready',
  from:current,
  to:target,
  entitlement_id:entitlement,
  preserve_member_identity:true,
  canonical_customer_id:CID.test(text(canonical_customer_id))?text(canonical_customer_id):null,
  reservation_id:text(reservation_id)||null,
  automatic_discount:false,
  commerce_write_allowed:false,
  transition_allowed:false,
  execution_requires_separate_gate:true
 };
}

export function planBenefitPromotionCarryForward({
 member_identity_id,
 prospect_id,
 canonical_customer_id,
 current_state,
 entitlement_id='',
 persisted_prospect_id='',
 persisted_member_identity_id='',
 persisted_canonical_customer_id='',
 promotion_verified=false,
 entitlement_binding_verified=false,
 persisted_entitlement_id='',
 persisted_entitlement_member_identity_id='',
 persisted_entitlement_state=''
}={}){
 const member=text(member_identity_id),prospect=text(prospect_id),customer=text(canonical_customer_id),current=text(current_state),entitlement=text(entitlement_id);
 if(!MID.test(member)||!PID.test(prospect)||!CID.test(customer)) return {status:'invalid_identity',carry_forward_allowed:false};
 if(!['issued','available','reserved','used','expired','revoked'].includes(current)) return {status:'invalid_state',carry_forward_allowed:false};
 if(!BEN.test(entitlement)) return {status:'carry_forward_entitlement_required',carry_forward_allowed:false};
 if(entitlement_binding_verified!==true||text(persisted_entitlement_id)!==entitlement||text(persisted_entitlement_member_identity_id)!==member||text(persisted_entitlement_state)!==current) return {status:'entitlement_carry_forward_binding_not_verified',carry_forward_allowed:false};
 if(promotion_verified!==true||text(persisted_prospect_id)!==prospect||text(persisted_member_identity_id)!==member||text(persisted_canonical_customer_id)!==customer) return {status:'promotion_binding_not_verified',carry_forward_allowed:false};
 return {
  status:'ready',
  entitlement_id:entitlement,
  member_identity_id:member,
  canonical_customer_id:customer,
  state:current,
  state_reset:false,
  reissue:false,
  carry_forward_allowed:false,
  execution_requires_separate_gate:true
 };
}
