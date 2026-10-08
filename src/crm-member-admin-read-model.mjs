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

export function buildMemberAdminReadModel({
 subject_type,
 canonical_customer_id='',
 prospect_id='',
 member_identity_id='',
 subject_member_binding_verified=false,
 persisted_member_identity_id='',
 persisted_subject_customer_id='',
 persisted_subject_prospect_id='',
 member_status='unregistered',
 consent_status='missing',
 invitation_status='unissued',
 review_pending_count=0,
 benefit_status='none',
 acquisition_source='',
 reservation_status='none',
 promotion_status='prospect'
}={}){
 const type=text(subject_type),customer=text(canonical_customer_id),prospect=text(prospect_id),member=text(member_identity_id);
 if(!['customer','prospect'].includes(type)) return {status:'invalid_subject_type',read_ready:false};
 if(type==='customer'&&!CID.test(customer)) return {status:'invalid_customer_id',read_ready:false};
 if(type==='prospect'&&!PID.test(prospect)) return {status:'invalid_prospect_id',read_ready:false};
 if(!MID.test(member)) return {status:'invalid_member_identity',read_ready:false};
 if(type==='customer'&&(subject_member_binding_verified!==true||text(persisted_member_identity_id)!==member||text(persisted_subject_customer_id)!==customer)) return {status:'subject_member_binding_not_verified',read_ready:false};
 if(type==='prospect'&&(subject_member_binding_verified!==true||text(persisted_member_identity_id)!==member||text(persisted_subject_prospect_id)!==prospect)) return {status:'subject_member_binding_not_verified',read_ready:false};
 const pending=scalarNonNegativeInteger(review_pending_count);
 if(pending===null) return {status:'invalid_review_count',read_ready:false};
 return {
  status:'ready',
  subject_type:type,
  canonical_customer_id:type==='customer'?customer:null,
  prospect_id:type==='prospect'?prospect:null,
  member_identity_id:member,
  member_status:text(member_status),
  consent_status:text(consent_status),
  invitation_status:type==='customer'?text(invitation_status):'not_applicable',
  review_pending_count:pending,
  needs_review:pending>0,
  benefit_status:text(benefit_status),
  acquisition_source:text(acquisition_source),
  reservation_status:text(reservation_status),
  promotion_status:type==='prospect'?text(promotion_status):'not_applicable',
  customer_id_generation:false,
  mutation_allowed:false,
  line_send_allowed:false,
  read_ready:true
 };
}
