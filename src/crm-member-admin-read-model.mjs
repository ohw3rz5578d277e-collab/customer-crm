const text=v=>v==null?'':String(v).trim();

export function buildMemberAdminReadModel({
 subject_type,
 canonical_customer_id='',
 prospect_id='',
 member_identity_id='',
 member_status='unregistered',
 consent_status='missing',
 invitation_status='unissued',
 review_pending_count=0,
 benefit_status='none',
 acquisition_source='',
 reservation_status='none',
 promotion_status='prospect'
}={}){
 const type=text(subject_type);
 if(!['customer','prospect'].includes(type)) return {status:'invalid_subject_type',read_ready:false};
 if(type==='customer'&&!/^\d{8}$/.test(text(canonical_customer_id))) return {status:'invalid_customer_id',read_ready:false};
 if(type==='prospect'&&!/^PID_[A-Za-z0-9_-]{22,}$/.test(text(prospect_id))) return {status:'invalid_prospect_id',read_ready:false};
 if(type==='prospect'&&!/^MID_[A-Za-z0-9_-]{22,}$/.test(text(member_identity_id))) return {status:'invalid_member_identity',read_ready:false};
 const pending=Number(review_pending_count);
 if(!Number.isInteger(pending)||pending<0) return {status:'invalid_review_count',read_ready:false};
 return {
  status:'ready',
  subject_type:type,
  canonical_customer_id:type==='customer'?text(canonical_customer_id):null,
  prospect_id:type==='prospect'?text(prospect_id):null,
  member_identity_id:text(member_identity_id)||null,
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
