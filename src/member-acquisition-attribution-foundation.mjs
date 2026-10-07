import crypto from 'node:crypto';
const MID=/^MID_[A-Za-z0-9_-]{22,}$/;
const PID=/^PID_[A-Za-z0-9_-]{22,}$/;
const CID=/^\d{8}$/;
const text=v=>v==null?'':String(v).trim();
const allowedSource=new Set(['instagram','website','meta_paid','instagram_profile','organic_search','direct','referral','unknown']);

export function createAcquisitionEventId(){return 'AQ_'+crypto.randomBytes(24).toString('base64url');}

export function planAcquisitionRegistration({
 member_identity_id,prospect_id,source='unknown',campaign='',referrer='',utm_source='',utm_medium='',utm_campaign=''
}={}){
 if(!MID.test(text(member_identity_id))||!PID.test(text(prospect_id))) return {status:'invalid_identity',record_allowed:false};
 const normalized=allowedSource.has(text(source))?text(source):'unknown';
 return {
  status:'ready',event_id:createAcquisitionEventId(),member_identity_id:text(member_identity_id),prospect_id:text(prospect_id),
  source:normalized,campaign:text(campaign),referrer:text(referrer),utm_source:text(utm_source),utm_medium:text(utm_medium),utm_campaign:text(utm_campaign),
  identity_authority:false,append_only:true,record_allowed:false
 };
}

export function planFunnelEvent({stage,member_identity_id,prospect_id='',canonical_customer_id='',reservation_id='',occurred_at}={}){
 const stages=new Set(['registered','inquiry','reserved','shot','repeat']);
 if(!stages.has(text(stage))||!MID.test(text(member_identity_id))) return {status:'invalid_event',record_allowed:false};
 if(text(canonical_customer_id)&&!CID.test(text(canonical_customer_id))) return {status:'invalid_customer_id',record_allowed:false};
 if(text(prospect_id)&&!PID.test(text(prospect_id))) return {status:'invalid_prospect_id',record_allowed:false};
 if(!text(occurred_at)||Number.isNaN(Date.parse(text(occurred_at)))) return {status:'invalid_time',record_allowed:false};
 if(['reserved','shot','repeat'].includes(text(stage))&&!text(reservation_id)) return {status:'reservation_context_required',record_allowed:false};
 return {status:'ready',event_id:createAcquisitionEventId(),stage:text(stage),member_identity_id:text(member_identity_id),prospect_id:text(prospect_id)||null,canonical_customer_id:text(canonical_customer_id)||null,reservation_id:text(reservation_id)||null,occurred_at:text(occurred_at),append_only:true,identity_authority:false,record_allowed:false};
}
