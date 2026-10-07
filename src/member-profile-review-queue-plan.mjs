import crypto from 'node:crypto';
const ALLOWED_REASONS=new Set(['identity_mismatch','version_conflict','promotion_collision','binding_mismatch']);
const text=v=>v==null?'':String(v).trim();

export function createReviewId(){
 return 'RV_'+crypto.randomBytes(24).toString('base64url');
}

export function reviewPayloadDigest(payload){
 const canonical=JSON.stringify(payload||{});
 return crypto.createHash('sha256').update(canonical).digest('hex');
}

export function planProfileReview({
 reason_code,
 member_identity_id,
 claimed_customer_id='',
 prospect_id='',
 submitted_profile={}
}={}){
 const reason=text(reason_code);
 if(!ALLOWED_REASONS.has(reason)) return {status:'invalid_reason',queue_write_allowed:false,master_write_allowed:false};
 if(!text(member_identity_id)) return {status:'missing_member_identity',queue_write_allowed:false,master_write_allowed:false};
 return {
   status:'review_required',
   review_id:createReviewId(),
   reason_code:reason,
   member_identity_id:text(member_identity_id),
   claimed_customer_id:text(claimed_customer_id),
   prospect_id:text(prospect_id),
   payload_digest_sha256:reviewPayloadDigest(submitted_profile),
   submitted_profile_ephemeral:true,
   customer_message:'変更内容を受け付けました',
   master_write_allowed:false,
   queue_write_allowed:false,
   admin_decision_required:true
 };
}

export function planReviewDecision({review_status,decision,identity_reverified=false,latest_version_verified=false}={}){
 if(text(review_status)!=='pending') return {status:'not_pending',master_write_allowed:false};
 if(!['approve','reject'].includes(text(decision))) return {status:'invalid_decision',master_write_allowed:false};
 if(text(decision)==='reject') return {status:'reject_ready',master_write_allowed:false,audit_required:true,execution_requires_separate_gate:true};
 if(!identity_reverified||!latest_version_verified) return {status:'reverification_required',master_write_allowed:false};
 return {status:'approve_ready',master_write_allowed:false,audit_required:true,new_sync_event_required:true,execution_requires_separate_gate:true};
}
