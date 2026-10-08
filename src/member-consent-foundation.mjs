import crypto from 'node:crypto';
const text=v=>v==null?'':String(v).trim();
const VERSION_RE=/^[A-Za-z0-9._-]{1,64}$/;
const HASH_RE=/^[0-9a-f]{64}$/;
const MEMBER_RE=/^MID_[A-Za-z0-9_-]{22,}$/;
const ISO_INSTANT_RE=/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?(?:Z|[+-]\d{2}:\d{2})$/;

export function documentDigest(content){
 return crypto.createHash('sha256').update(String(content??'')).digest('hex');
}

export function planConsentRecord({
 member_identity_id,
 terms_version,
 terms_sha256,
 privacy_version,
 privacy_sha256,
 terms_accepted=false,
 privacy_accepted=false,
 accepted_at
}={}){
 const member=text(member_identity_id);
 const tv=text(terms_version),pv=text(privacy_version),th=text(terms_sha256),ph=text(privacy_sha256),at=text(accepted_at);
 if(!MEMBER_RE.test(member)) return {status:'invalid_member_identity',record_allowed:false};
 if(!VERSION_RE.test(tv)||!VERSION_RE.test(pv)||!HASH_RE.test(th)||!HASH_RE.test(ph)) return {status:'invalid_document_identity',record_allowed:false};
 if(terms_accepted!==true||privacy_accepted!==true) return {status:'consent_incomplete',record_allowed:false};
 const match=at.match(ISO_INSTANT_RE);
 if(!match||Number.isNaN(Date.parse(at))) return {status:'invalid_accepted_at',record_allowed:false};
 const local=at.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,3}))?(Z|([+-])(\d{2}):(\d{2}))$/);
 if(!local) return {status:'invalid_accepted_at',record_allowed:false};
 const [,ys,mos,ds,hs,mis,ss,mss='',zone,sign,ohs='00',oms='00']=local;
 const y=Number(ys),mo=Number(mos),d=Number(ds),h=Number(hs),mi=Number(mis),sec=Number(ss),ms=Number(mss.padEnd(3,'0'));
 if(mo<1||mo>12||d<1||d>31||h>23||mi>59||sec>59) return {status:'invalid_accepted_at',record_allowed:false};
 const localUtc=Date.UTC(y,mo-1,d,h,mi,sec,ms);
 const check=new Date(localUtc);
 if(check.getUTCFullYear()!==y||check.getUTCMonth()!==mo-1||check.getUTCDate()!==d||check.getUTCHours()!==h||check.getUTCMinutes()!==mi||check.getUTCSeconds()!==sec) return {status:'invalid_accepted_at',record_allowed:false};
 if(zone!=='Z'){
  const oh=Number(ohs),om=Number(oms);
  if(oh>23||om>59) return {status:'invalid_accepted_at',record_allowed:false};
  const offset=(oh*60+om)*60000*(sign==='+'?1:-1);
  if(new Date(localUtc-offset).getTime()!==Date.parse(at)) return {status:'invalid_accepted_at',record_allowed:false};
 }
 return {
  status:'ready',
  member_identity_id:member,
  terms_version:tv,
  terms_sha256:th,
  privacy_version:pv,
  privacy_sha256:ph,
  accepted_at:at,
  append_only:true,
  replace_prior_consent:false,
  record_allowed:false
 };
}

export function planConsentRequirement({latest_terms_version,latest_privacy_version,last_terms_version='',last_privacy_version=''}={}){
 const lt=text(latest_terms_version),lp=text(latest_privacy_version),pt=text(last_terms_version),pp=text(last_privacy_version);
 if(!VERSION_RE.test(lt)||!VERSION_RE.test(lp)) return {status:'invalid_latest_version',access_ready:false};
 const termsRequired=pt!==lt;
 const privacyRequired=pp!==lp;
 return {status:'ready',terms_reconsent_required:termsRequired,privacy_reconsent_required:privacyRequired,reconsent_required:termsRequired||privacyRequired};
}
