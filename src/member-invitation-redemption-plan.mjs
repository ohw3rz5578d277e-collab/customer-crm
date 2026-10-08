import crypto from 'node:crypto';

const CUSTOMER_ID_RE=/^\d{8}$/;
const INVITATION_ID_RE=/^[A-Za-z0-9_-]{16,128}$/;
const INVITE_TOKEN_RE=/^[A-Za-z0-9_-]{32,256}$/;
const SHA256_HEX_RE=/^[0-9a-f]{64}$/;
const UTC_INSTANT_RE=/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
const DB_UTC_NOW="strftime('%Y-%m-%dT%H:%M:%fZ','now')";
const hasOwn=(obj,key)=>Object.prototype.hasOwnProperty.call(obj,key);
const strictText=value=>typeof value==='string'?value:null;

function parseCount(value){
  if(typeof value==='number'){
    return Number.isSafeInteger(value)&&value>=0?value:null;
  }
  if(typeof value==='string'&&/^(?:0|[1-9]\d*)$/.test(value)){
    const parsed=Number(value);
    return Number.isSafeInteger(parsed)?parsed:null;
  }
  return null;
}

function parseUtcInstant(value){
  if(typeof value!=='string'||!UTC_INSTANT_RE.test(value)) return null;
  const epoch=Date.parse(value);
  if(!Number.isFinite(epoch)) return null;
  return new Date(epoch).toISOString()===value?epoch:null;
}

function tokenDigest(rawToken){
  return crypto.createHash('sha256').update(rawToken).digest('hex');
}

function digestEqual(left,right){
  if(!SHA256_HEX_RE.test(left)||!SHA256_HEX_RE.test(right)) return false;
  return crypto.timingSafeEqual(Buffer.from(left,'hex'),Buffer.from(right,'hex'));
}

function blocked(status,{review_required=false,public_status='invalid_or_unavailable_invitation'}={}){
  return {
    status,
    public_status,
    review_required,
    ready:false,
    write_allowed:false,
    execute:false,
    production_write_authorized:false,
    raw_token_output:false
  };
}

function inspectNullableTimestampEvidence(evidence,key){
  if(!hasOwn(evidence,key)) return {ok:false,status:`missing_${key}_evidence`};
  const value=evidence[key];
  if(value===null) return {ok:true,value:null};
  const epoch=parseUtcInstant(value);
  if(epoch===null) return {ok:false,status:`invalid_${key}_evidence`};
  return {ok:true,value,epoch};
}

export function planOneTimeInvitationRedemption(evidence={}){
  if(!evidence||typeof evidence!=='object'||Array.isArray(evidence)){
    return blocked('invalid_invitation_evidence');
  }

  const rawToken=strictText(evidence.raw_token);
  if(rawToken===null||!INVITE_TOKEN_RE.test(rawToken)){
    return blocked('invalid_invitation_token');
  }

  if(evidence.server_now_verified!==true){
    return blocked('server_now_not_verified');
  }
  const nowText=strictText(evidence.now);
  const nowEpoch=parseUtcInstant(nowText);
  if(nowEpoch===null) return blocked('invalid_now_evidence');

  if(evidence.invitation_record_verified!==true){
    return blocked('invitation_record_not_verified');
  }

  const invitationId=strictText(evidence.persisted_invitation_id);
  const customerId=strictText(evidence.persisted_canonical_customer_id);
  const persistedDigest=strictText(evidence.persisted_token_sha256);
  const expiresAt=strictText(evidence.persisted_expires_at);

  if(invitationId===null||!INVITATION_ID_RE.test(invitationId)){
    return blocked('invalid_persisted_invitation_id',{review_required:true});
  }
  if(customerId===null||!CUSTOMER_ID_RE.test(customerId)){
    return blocked('invalid_persisted_customer_id',{review_required:true});
  }
  if(persistedDigest===null||!SHA256_HEX_RE.test(persistedDigest)){
    return blocked('invalid_persisted_token_digest',{review_required:true});
  }

  const expiresEpoch=parseUtcInstant(expiresAt);
  if(expiresEpoch===null){
    return blocked('invalid_persisted_expiry',{review_required:true});
  }

  const consumed=inspectNullableTimestampEvidence(evidence,'persisted_consumed_at');
  if(!consumed.ok) return blocked(consumed.status,{review_required:true});
  const invalidated=inspectNullableTimestampEvidence(evidence,'persisted_invalidated_at');
  if(!invalidated.ok) return blocked(invalidated.status,{review_required:true});

  const computedDigest=tokenDigest(rawToken);

  const matchingCount=parseCount(evidence.matching_invitation_count);
  if(matchingCount===null){
    return blocked('invalid_matching_invitation_count',{review_required:true});
  }
  const matchingCountDigest=strictText(evidence.matching_invitation_count_token_sha256);
  if(matchingCountDigest===null||!SHA256_HEX_RE.test(matchingCountDigest)){
    return blocked('invalid_matching_invitation_count_scope',{review_required:true});
  }
  if(!digestEqual(matchingCountDigest,computedDigest)){
    return blocked('matching_invitation_count_scope_mismatch',{review_required:true});
  }
  if(matchingCount===0) return blocked('invitation_not_found');
  if(matchingCount!==1){
    return blocked('ambiguous_invitation_digest',{review_required:true});
  }
  if(!digestEqual(persistedDigest,computedDigest)){
    return blocked('persisted_invitation_digest_mismatch',{review_required:true});
  }

  const activeCount=parseCount(evidence.active_invitation_count_for_customer);
  if(activeCount===null){
    return blocked('invalid_active_invitation_count',{review_required:true});
  }
  const activeCountCustomerId=strictText(evidence.active_invitation_count_customer_id);
  if(activeCountCustomerId===null||!CUSTOMER_ID_RE.test(activeCountCustomerId)){
    return blocked('invalid_active_invitation_count_scope',{review_required:true});
  }
  if(activeCountCustomerId!==customerId){
    return blocked('active_invitation_count_scope_mismatch',{review_required:true});
  }

  if(invalidated.value!==null){
    return blocked('invitation_revoked');
  }
  if(consumed.value!==null){
    return blocked('invitation_already_consumed');
  }
  if(nowEpoch>=expiresEpoch){
    return blocked('invitation_expired');
  }
  if(activeCount!==1){
    return blocked('active_invitation_cardinality_violation',{review_required:true});
  }

  const statement=[
    'UPDATE member_customer_invitations',
    `SET consumed_at = ${DB_UTC_NOW}`,
    'WHERE invitation_id = ?',
    '  AND canonical_customer_id = ?',
    '  AND token_sha256 = ?',
    '  AND expires_at = ?',
    '  AND consumed_at IS NULL',
    '  AND invalidated_at IS NULL',
    `  AND expires_at > ${DB_UTC_NOW}`,
    '  AND NOT EXISTS (',
    '    SELECT 1',
    '    FROM member_customer_invitations AS other',
    '    WHERE other.canonical_customer_id = ?',
    '      AND other.invitation_id <> ?',
    '      AND other.consumed_at IS NULL',
    '      AND other.invalidated_at IS NULL',
    `      AND other.expires_at > ${DB_UTC_NOW}`,
    '  )'
  ].join('\n');

  return {
    status:'ready',
    public_status:'ready',
    review_required:false,
    ready:true,
    invitation_id:invitationId,
    canonical_customer_id:customerId,
    token_sha256:computedDigest,
    token_digest_verified:true,
    invitation_record_verified:true,
    server_now_verified:true,
    validated_at:nowText,
    single_use:true,
    customer_id_from_server_record:true,
    customer_id_from_client:false,
    raw_token_output:false,
    raw_token_retained:false,
    atomic_redemption:{
      mode:'single_statement_compare_and_set',
      statement,
      binds:[invitationId,customerId,computedDigest,expiresAt,customerId,invitationId],
      database_clock_required:true,
      active_cardinality_rechecked_at_execution:true,
      expiry_rechecked_at_execution:true,
      success_requires_affected_rows:1,
      zero_rows_status:'redemption_conflict_or_stale_evidence',
      retry_without_revalidation:false
    },
    write_allowed:false,
    execute:false,
    production_write_authorized:false,
    execution_requires_separate_gate:true,
    customer_id_generation:false,
    customer_mutation:false,
    member_identity_mutation:false,
    fuzzy_identity_linking:false
  };
}

export const __test={
  CUSTOMER_ID_RE,
  INVITATION_ID_RE,
  INVITE_TOKEN_RE,
  SHA256_HEX_RE,
  UTC_INSTANT_RE,
  DB_UTC_NOW,
  parseCount,
  parseUtcInstant
};
