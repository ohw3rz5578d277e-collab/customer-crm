import crypto from 'node:crypto';

const CUSTOMER_ID_RE=/^\d{8}$/;
const MEMBER_ID_RE=/^MID_[A-Za-z0-9_-]{22,}$/;
const PROSPECT_ID_RE=/^PID_[A-Za-z0-9_-]{22,}$/;
const INVITE_TOKEN_RE=/^[A-Za-z0-9_-]{32,}$/;

const text=v=>v==null?'':String(v).trim();
const randomOpaque=(prefix,bytes=24)=>prefix+crypto.randomBytes(bytes).toString('base64url');

export function createMemberIdentityId(){
  return randomOpaque('MID_');
}

export function createProspectId(){
  return randomOpaque('PID_');
}

export function createInvitationToken(){
  return crypto.randomBytes(32).toString('base64url');
}

export function invitationTokenDigest(token){
  const value=text(token);
  if(!INVITE_TOKEN_RE.test(value)) throw new Error('invalid_invitation_token');
  return crypto.createHash('sha256').update(value).digest('hex');
}

export function buildExistingCustomerFirstLoginPlan({customer_id,member_identity_id='',active_invitation_count=0,now_ms=Date.now(),ttl_days=7}={}){
  const customerId=text(customer_id);
  if(!CUSTOMER_ID_RE.test(customerId)) return {status:'invalid_customer_id',write_allowed:false};
  if(Number(active_invitation_count)>1) return {status:'ambiguous_active_invitation',review_required:true,write_allowed:false};
  if(!Number.isFinite(Number(now_ms))||!Number.isInteger(Number(ttl_days))||Number(ttl_days)<1||Number(ttl_days)>30) return {status:'invalid_invitation_ttl',write_allowed:false};
  if(member_identity_id && !MEMBER_ID_RE.test(text(member_identity_id))) return {status:'invalid_member_identity',write_allowed:false};
  return {
    status:'ready',
    customer_id:customerId,
    member_identity_id:member_identity_id?text(member_identity_id):createMemberIdentityId(),
    invitation:{
      raw_token:createInvitationToken(),
      expires_at:new Date(Number(now_ms)+Number(ttl_days)*24*60*60*1000).toISOString(),
      invalidate_existing_active:Number(active_invitation_count)===1,
      single_use:true,
      customer_id_exposed:false
    },
    customer_id_generation:false,
    fuzzy_identity_linking:false,
    write_allowed:false
  };
}

export function buildProspectRegistrationPlan({member_identity_id='',prospect_id=''}={}){
  if(member_identity_id && !MEMBER_ID_RE.test(text(member_identity_id))) return {status:'invalid_member_identity',write_allowed:false};
  if(prospect_id && !PROSPECT_ID_RE.test(text(prospect_id))) return {status:'invalid_prospect_id',write_allowed:false};
  return {
    status:'ready',
    member_identity_id:member_identity_id?text(member_identity_id):createMemberIdentityId(),
    prospect_id:prospect_id?text(prospect_id):createProspectId(),
    customer_id:null,
    customer_id_generation:false,
    fuzzy_identity_linking:false,
    write_allowed:false
  };
}

export function buildProspectPromotionPlan({member_identity_id,prospect_id,canonical_customer_id,collision_count=0,persisted_member_identity_id='',persisted_prospect_id=''}={}){
  const memberId=text(member_identity_id);
  const prospectId=text(prospect_id);
  const customerId=text(canonical_customer_id);
  if(!MEMBER_ID_RE.test(memberId)||!PROSPECT_ID_RE.test(prospectId)||!CUSTOMER_ID_RE.test(customerId)){
    return {status:'invalid_identity',write_allowed:false};
  }
  if(text(persisted_member_identity_id)!==memberId||text(persisted_prospect_id)!==prospectId){
    return {status:'binding_mismatch',review_required:true,write_allowed:false};
  }
  if(Number(collision_count)!==0){
    return {status:'review_required',review_required:true,write_allowed:false};
  }
  return {
    status:'ready',
    member_identity_id:memberId,
    prospect_id:prospectId,
    canonical_customer_id:customerId,
    preserve_member_identity:true,
    customer_id_generation:false,
    automatic_merge:false,
    write_allowed:false
  };
}

export const __test={CUSTOMER_ID_RE,MEMBER_ID_RE,PROSPECT_ID_RE,INVITE_TOKEN_RE};
