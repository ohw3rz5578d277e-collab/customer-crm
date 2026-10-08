import crypto from 'node:crypto';

const CUSTOMER_RE=/^\d{8}$/;
const PROSPECT_RE=/^PID_[A-Za-z0-9_-]{22,}$/;
const MEMBER_RE=/^MID_[A-Za-z0-9_-]{22,}$/;
const BENEFIT_RE=/^BEN_[A-Za-z0-9_-]{22,}$/;
const BENEFIT_STATES=new Set(['issued','available','reserved','used','expired','revoked']);

const strictText=value=>typeof value==='string'&&value.trim()===value?value:null;
const scalarNonNegativeInteger=value=>{
  if(typeof value==='number') return Number.isSafeInteger(value)&&value>=0?value:null;
  if(typeof value==='string'&&/^(?:0|[1-9]\d*)$/.test(value)){
    const parsed=Number(value);
    return Number.isSafeInteger(parsed)?parsed:null;
  }
  return null;
};
const sha256=value=>crypto.createHash('sha256').update(value).digest('hex');
const blocked=(status,{review_required=false,customer_id_generation=false}={})=>({
  status,ready:false,review_required,promotion_allowed:false,execute:false,production_write_authorized:false,
  customer_id_generation,automatic_merge:false,fuzzy_match_used:false
});
const exact=(value,expected)=>strictText(value)!==null&&value===expected;

function inspectScopedCount(value,expectedScope,actualScope,status){
  const count=scalarNonNegativeInteger(value);
  if(count===null) return {ok:false,status:`invalid_${status}`};
  if(!exact(actualScope,expectedScope)) return {ok:false,status:`${status}_scope_mismatch`};
  return {ok:true,count};
}

export function createProspectPromotionEventId({prospect_id,member_identity_id,canonical_customer_id}={}){
  const prospect=strictText(prospect_id),member=strictText(member_identity_id),customer=strictText(canonical_customer_id);
  if(prospect===null||member===null||customer===null) return null;
  return `PROM_${sha256(JSON.stringify(['member_prospect_promotion_v1',prospect,member,customer]))}`;
}

export function planProspectCustomerPromotion(evidence={}){
  if(!evidence||typeof evidence!=='object'||Array.isArray(evidence)) return blocked('invalid_promotion_evidence');

  const prospect=strictText(evidence.prospect_id),member=strictText(evidence.member_identity_id),customer=strictText(evidence.canonical_customer_id);
  if(prospect===null||member===null||customer===null||!PROSPECT_RE.test(prospect)||!MEMBER_RE.test(member)||!CUSTOMER_RE.test(customer)) return blocked('invalid_identity');
  if(evidence.customer_id_source!=='customer_crm') return blocked('invalid_customer_id_source');

  if(evidence.customer_crm_record_verified!==true||!exact(evidence.persisted_crm_customer_id,customer)||evidence.persisted_crm_customer_id_source!=='customer_crm'){
    return blocked('crm_customer_not_verified');
  }
  if(evidence.member_prospect_binding_verified!==true||!exact(evidence.persisted_prospect_id,prospect)||!exact(evidence.persisted_member_identity_id,member)){
    return blocked('member_prospect_binding_not_verified',{review_required:true});
  }
  if(evidence.crm_promotion_trigger_verified!==true||!exact(evidence.persisted_trigger_prospect_id,prospect)||!exact(evidence.persisted_trigger_member_identity_id,member)||!exact(evidence.persisted_trigger_customer_id,customer)){
    return blocked('crm_promotion_trigger_not_verified',{review_required:true});
  }

  const status=strictText(evidence.prospect_status);
  if(status===null) return blocked('missing_prospect_status');
  if(evidence.prospect_status_binding_verified!==true||!exact(evidence.persisted_status_prospect_id,prospect)||!exact(evidence.persisted_status_member_identity_id,member)||!exact(evidence.persisted_prospect_status,status)){
    return blocked('prospect_status_binding_not_verified',{review_required:true});
  }

  const customerBinding=inspectScopedCount(evidence.existing_customer_member_binding_count,customer,evidence.persisted_binding_count_customer_id,'customer_member_binding_count');
  if(!customerBinding.ok) return blocked(customerBinding.status,{review_required:true});

  const promotionEventId=createProspectPromotionEventId({prospect_id:prospect,member_identity_id:member,canonical_customer_id:customer});
  const promotionCount=scalarNonNegativeInteger(evidence.existing_promotion_event_count);
  if(promotionCount===null) return blocked('invalid_existing_promotion_event_count');
  if(!exact(evidence.persisted_promotion_event_count_prospect_id,prospect)||!exact(evidence.persisted_promotion_event_count_member_identity_id,member)||!exact(evidence.persisted_promotion_event_count_customer_id,customer)){
    return blocked('promotion_event_count_scope_mismatch',{review_required:true});
  }

  // Completed exact replay: one promotion event, the Prospect is promoted, and the target
  // Customer is bound to this same Member. No second mutation is planned.
  if(promotionCount===1){
    const replayBindingVerified=customerBinding.count===1&&evidence.customer_member_binding_verified===true&&exact(evidence.persisted_bound_customer_id,customer)&&exact(evidence.persisted_bound_member_identity_id,member);
    const replayEventVerified=evidence.existing_promotion_event_verified===true&&exact(evidence.persisted_promotion_event_id,promotionEventId)&&exact(evidence.persisted_promotion_event_prospect_id,prospect)&&exact(evidence.persisted_promotion_event_member_identity_id,member)&&exact(evidence.persisted_promotion_event_customer_id,customer);
    if(status==='promoted'&&replayBindingVerified&&replayEventVerified){
      return {
        ...blocked('promotion_already_recorded'),
        prospect_id:prospect,member_identity_id:member,canonical_customer_id:customer,promotion_event_id:promotionEventId,
        idempotent_replay:true,preserve_member_identity:true,customer_id_generation:false
      };
    }
    return blocked('promotion_replay_evidence_mismatch',{review_required:true});
  }
  if(promotionCount!==0) return blocked('promotion_event_collision',{review_required:true});

  if(status!=='prospect') return blocked('invalid_prospect_state',{review_required:true});
  if(customerBinding.count!==0) return blocked('customer_binding_collision',{review_required:true});

  if(evidence.consent_history_verified!==true||!exact(evidence.persisted_consent_member_identity_id,member)||!exact(evidence.persisted_consent_prospect_id,prospect)){
    return blocked('consent_history_not_verified',{review_required:true});
  }
  if(evidence.acquisition_history_verified!==true||!exact(evidence.persisted_acquisition_member_identity_id,member)||!exact(evidence.persisted_acquisition_prospect_id,prospect)){
    return blocked('acquisition_history_not_verified',{review_required:true});
  }

  const benefitCount=scalarNonNegativeInteger(evidence.existing_signup_benefit_count);
  if(benefitCount===null) return blocked('invalid_signup_benefit_count');
  if(!exact(evidence.persisted_benefit_count_member_identity_id,member)||!exact(evidence.persisted_benefit_count_prospect_id,prospect)){
    return blocked('signup_benefit_count_scope_mismatch',{review_required:true});
  }
  if(benefitCount>1) return blocked('signup_benefit_collision',{review_required:true});

  let benefitCarryForward=null;
  if(benefitCount===1){
    const entitlementId=strictText(evidence.persisted_benefit_entitlement_id),benefitState=strictText(evidence.persisted_benefit_state);
    if(evidence.benefit_state_verified!==true||entitlementId===null||!BENEFIT_RE.test(entitlementId)||benefitState===null||!BENEFIT_STATES.has(benefitState)||!exact(evidence.persisted_benefit_member_identity_id,member)||!exact(evidence.persisted_benefit_prospect_id,prospect)){
      return blocked('signup_benefit_state_not_verified',{review_required:true});
    }
    benefitCarryForward={entitlement_id:entitlementId,state:benefitState,member_identity_id:member,prospect_id:prospect,canonical_customer_id:customer,state_reset:false,reissue:false};
  }

  return {
    status:'ready',ready:true,review_required:false,
    prospect_id:prospect,member_identity_id:member,canonical_customer_id:customer,promotion_event_id:promotionEventId,
    preserve_member_identity:true,preserve_consent_history:true,preserve_acquisition_history:true,preserve_benefit_state:true,
    benefit_carry_forward:benefitCarryForward,
    transaction_required:true,
    transaction_operations:['bind_member_to_existing_crm_customer','mark_prospect_promoted','append_promotion_event','preserve_history_links'],
    partial_commit_allowed:false,
    fuzzy_match_used:false,customer_id_generation:false,automatic_merge:false,
    promotion_allowed:false,execute:false,production_write_authorized:false,execution_requires_separate_gate:true
  };
}

export const __test={CUSTOMER_RE,PROSPECT_RE,MEMBER_RE,BENEFIT_RE,BENEFIT_STATES,scalarNonNegativeInteger};
