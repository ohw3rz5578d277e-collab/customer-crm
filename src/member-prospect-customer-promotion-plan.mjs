const text=v=>v==null?'':String(v).trim();
const CUSTOMER_RE=/^\d{8}$/;
const PROSPECT_RE=/^PID_[A-Za-z0-9_-]{22,}$/;
const MEMBER_RE=/^MID_[A-Za-z0-9_-]{22,}$/;
const scalarNonNegativeInteger=v=>{
 if(typeof v==='number') return Number.isInteger(v)&&v>=0?v:null;
 if(typeof v==='string'&&/^(?:0|[1-9]\d*)$/.test(v.trim())) return Number(v.trim());
 return null;
};

export function planProspectCustomerPromotion({
 prospect_id,
 member_identity_id,
 persisted_prospect_id,
 persisted_member_identity_id,
 canonical_customer_id,
 customer_id_source,
 existing_customer_member_binding_count,
 prospect_status,
 consent_history_preserved=false,
 acquisition_history_preserved=false,
 benefit_state_preserved=false
}={}){
 const prospect=text(prospect_id),member=text(member_identity_id),customer=text(canonical_customer_id);
 if(!PROSPECT_RE.test(prospect)||!MEMBER_RE.test(member)||!CUSTOMER_RE.test(customer)) return {status:'invalid_identity',promotion_allowed:false};
 if(text(customer_id_source)!=='customer_crm') return {status:'invalid_customer_id_source',promotion_allowed:false,customer_id_generation:false};
 if(text(persisted_prospect_id)!==prospect||text(persisted_member_identity_id)!==member) return {status:'binding_mismatch',review_required:true,promotion_allowed:false};
 if(prospect_status===undefined||prospect_status===null||text(prospect_status)===''||existing_customer_member_binding_count===undefined||existing_customer_member_binding_count===null||typeof existing_customer_member_binding_count==='boolean'||String(existing_customer_member_binding_count).trim()==='') return {status:'missing_persisted_evidence',promotion_allowed:false};
 if(text(prospect_status)!=='prospect') return {status:'invalid_prospect_state',promotion_allowed:false};
 const bindingCount=scalarNonNegativeInteger(existing_customer_member_binding_count);
 if(bindingCount===null) return {status:'invalid_binding_count',promotion_allowed:false};
 if(bindingCount!==0) return {status:'customer_binding_collision',review_required:true,promotion_allowed:false};
 if(consent_history_preserved!==true||acquisition_history_preserved!==true||benefit_state_preserved!==true){
   return {status:'continuity_not_verified',review_required:true,promotion_allowed:false};
 }
 return {
  status:'ready',
  prospect_id:prospect,
  member_identity_id:member,
  canonical_customer_id:customer,
  preserve_member_identity:true,
  preserve_consent_history:true,
  preserve_acquisition_history:true,
  preserve_benefit_state:true,
  fuzzy_match_used:false,
  customer_id_generation:false,
  automatic_merge:false,
  promotion_allowed:false,
  execution_requires_separate_gate:true
 };
}
