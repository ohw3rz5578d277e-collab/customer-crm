import crypto from 'node:crypto';
const MID=/^MID_[A-Za-z0-9_-]{22,}$/;
const PID=/^PID_[A-Za-z0-9_-]{22,}$/;
const CID=/^\d{8}$/;
const text=v=>v==null?'':String(v).trim();
const ISO_INSTANT_RE=/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/;
function validInstant(value){
 const at=text(value),local=at.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?(Z|([+-])(\d{2}):(\d{2}))$/);
 if(!ISO_INSTANT_RE.test(at)||!local||Number.isNaN(Date.parse(at))) return false;
 const [,ys,mos,ds,hs,mis,ss,mss='',zone,sign,ohs='00',oms='00']=local;
 const y=Number(ys),mo=Number(mos),d=Number(ds),h=Number(hs),mi=Number(mis),sec=Number(ss),ms=Number(mss.padEnd(3,'0'));
 if(mo<1||mo>12||d<1||d>31||h>23||mi>59||sec>59) return false;
 const localUtc=Date.UTC(y,mo-1,d,h,mi,sec,ms),check=new Date(localUtc);
 if(check.getUTCFullYear()!==y||check.getUTCMonth()!==mo-1||check.getUTCDate()!==d||check.getUTCHours()!==h||check.getUTCMinutes()!==mi||check.getUTCSeconds()!==sec) return false;
 if(zone!=='Z'){
  const oh=Number(ohs),om=Number(oms);
  if(oh>23||om>59) return false;
  const offset=(oh*60+om)*60000*(sign==='+'?1:-1);
  if(new Date(localUtc-offset).getTime()!==Date.parse(at)) return false;
 }
 return true;
}
const allowedSource=new Set(['instagram','website','meta_paid','instagram_profile','organic_search','direct','referral','unknown']);

export function createAcquisitionEventId(seed=''){
 const stable=text(seed);
 return stable?`AQ_${crypto.createHash('sha256').update(stable).digest('base64url')}`:`AQ_${crypto.randomBytes(24).toString('base64url')}`;
}

export function planAcquisitionRegistration({
 member_identity_id,
 prospect_id,
 member_prospect_binding_verified=false,
 persisted_member_identity_id='',
 persisted_prospect_id='',
 source='unknown',
 campaign='',
 referrer='',
 utm_source='',
 utm_medium='',
 utm_campaign=''
}={}){
 const member=text(member_identity_id),prospect=text(prospect_id);
 if(!MID.test(member)||!PID.test(prospect)) return {status:'invalid_identity',record_allowed:false};
 if(member_prospect_binding_verified!==true||text(persisted_member_identity_id)!==member||text(persisted_prospect_id)!==prospect) return {status:'member_prospect_binding_not_verified',record_allowed:false};
 const normalized=allowedSource.has(text(source))?text(source):'unknown';
 return {
  status:'ready',event_id:createAcquisitionEventId(),member_identity_id:member,prospect_id:prospect,
  source:normalized,campaign:text(campaign),referrer:text(referrer),utm_source:text(utm_source),utm_medium:text(utm_medium),utm_campaign:text(utm_campaign),
  identity_authority:false,append_only:true,record_allowed:false
 };
}

export function planFunnelEvent({
 stage,
 member_identity_id,
 prospect_id='',
 canonical_customer_id='',
 reservation_id='',
 occurred_at,
 idempotency_key='',
 member_prospect_binding_verified=false,
 persisted_prospect_member_identity_id='',
 persisted_prospect_id='',
 member_customer_binding_verified=false,
 persisted_customer_member_identity_id='',
 persisted_canonical_customer_id='',
 reservation_binding_verified=false,
 persisted_reservation_id='',
 persisted_reservation_member_identity_id='',
 persisted_reservation_prospect_id='',
 persisted_reservation_customer_id=''
}={}){
 const stages=new Set(['registered','inquiry','reserved','shot','repeat']);
 const stageName=text(stage),member=text(member_identity_id),prospect=text(prospect_id),customer=text(canonical_customer_id),reservation=text(reservation_id),idempotency=text(idempotency_key);
 if(!stages.has(stageName)||!MID.test(member)) return {status:'invalid_event',record_allowed:false};
 if(customer&&!CID.test(customer)) return {status:'invalid_customer_id',record_allowed:false};
 if(prospect&&!PID.test(prospect)) return {status:'invalid_prospect_id',record_allowed:false};
 if(!prospect&&!customer) return {status:'lifecycle_binding_required',record_allowed:false};
 if(!validInstant(occurred_at)) return {status:'invalid_time',record_allowed:false};
 if(!idempotency||idempotency.length>256) return {status:'idempotency_key_required',record_allowed:false};
 const reservationBacked=['reserved','shot','repeat'].includes(stageName);
 if(reservationBacked&&!reservation) return {status:'reservation_context_required',record_allowed:false};
 if(prospect&&(member_prospect_binding_verified!==true||text(persisted_prospect_member_identity_id)!==member||text(persisted_prospect_id)!==prospect)) return {status:'member_prospect_binding_not_verified',record_allowed:false};
 if(customer&&(member_customer_binding_verified!==true||text(persisted_customer_member_identity_id)!==member||text(persisted_canonical_customer_id)!==customer)) return {status:'member_customer_binding_not_verified',record_allowed:false};
 if(reservationBacked){
  if(reservation_binding_verified!==true||text(persisted_reservation_id)!==reservation||text(persisted_reservation_member_identity_id)!==member) return {status:'reservation_binding_not_verified',record_allowed:false};
  if(prospect&&text(persisted_reservation_prospect_id)!==prospect) return {status:'reservation_binding_not_verified',record_allowed:false};
  if(customer&&text(persisted_reservation_customer_id)!==customer) return {status:'reservation_binding_not_verified',record_allowed:false};
 }
 const eventSeed=JSON.stringify(['member_funnel_v1',idempotency,stageName,member,prospect,customer,reservation,text(occurred_at)]);
 return {status:'ready',event_id:createAcquisitionEventId(eventSeed),stage:stageName,member_identity_id:member,prospect_id:prospect||null,canonical_customer_id:customer||null,reservation_id:reservation||null,occurred_at:text(occurred_at),append_only:true,identity_authority:false,record_allowed:false};
}
