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
 if(!ISO_INSTANT_RE.test(at)||Number.isNaN(Date.parse(at))) return {status:'invalid_accepted_at',record_allowed:false};
 const parsed=new Date(at);
 const normalized=parsed.toISOString();
 const normalizedInput=at.endsWith('Z')?new Date(at).toISOString():normalized;
 if(normalizedInput!==normalized) return {status:'invalid_accepted_at',record_allowed:false};
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
