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
 if(!validInstant(occurred_at)) return {status:'invalid_time',record_allowed:false};
 if(['reserved','shot','repeat'].includes(text(stage))&&!text(reservation_id)) return {status:'reservation_context_required',record_allowed:false};
 return {status:'ready',event_id:createAcquisitionEventId(),stage:text(stage),member_identity_id:text(member_identity_id),prospect_id:text(prospect_id)||null,canonical_customer_id:text(canonical_customer_id)||null,reservation_id:text(reservation_id)||null,occurred_at:text(occurred_at),append_only:true,identity_authority:false,record_allowed:false};
}
