import { createHash } from 'node:crypto';

const CURRENT_ID_RE=/^\d{8}$/;
const LINE_ID_RE=/^U[0-9a-fA-F]{20,}$/;

function text(v){return v==null?'':String(v).trim()}
function normName(v){return text(v).normalize('NFKC').toLowerCase().replace(/[\s　・･.．,，、()（）\[\]［］【】「」『』]/g,'')}
function normPhone(v){return text(v).normalize('NFKC').replace(/[^0-9+]/g,'').replace(/^\+81/,'0')}
function normEmail(v){return text(v).normalize('NFKC').toLowerCase()}
function lineHash(v){return createHash('sha256').update(text(v)).digest('hex').slice(0,16)}
function add(map,key,value){if(!key)return;if(!map.has(key))map.set(key,[]);map.get(key).push(value)}
function uniqueValues(items,fn){return [...new Set((items||[]).map(fn).filter(Boolean))]}
function one(items){return Array.isArray(items)&&items.length===1?items[0]:null}

function normalizeCustomer(c){
  return {
    customer_id:text(c.customer_id??c.customerId),
    line_user_id:text(c.line_user_id??c.lineUserId),
    name:text(c.name??c.customer_name??c.displayName),
    phone:normPhone(c.phone??c.tel??c.telephone),
    email:normEmail(c.email??c.mail),
    deleted_at:text(c.deleted_at)
  };
}
function normalizeMaster(c){
  return {
    customer_id:text(c.customer_id??c.customerId??c['顧客ID']),
    line_user_id:text(c.line_user_id??c.lineUserId??c['LINE UserID']),
    name:text(c.name??c.customer_name??c.displayName??c['名前']??c['LINE名（本人設定）'])
  };
}
function normalizeReview(r){
  return {
    reservation_customer_id:text(r.reservation_customer_id),
    crm_candidate_customer_id:text(r.crm_candidate_customer_id),
    decision:text(r.decision).toUpperCase()
  };
}
function normalizeReservationIdentity(r){
  return {
    customer_id:text(r.customer_id??r.reservation_customer_id),
    name:text(r.customer_name??r.name),
    phone:normPhone(r.phone??r.tel??r.telephone),
    email:normEmail(r.email??r.mail),
    line_user_id:text(r.line_user_id??r.lineUserId)
  };
}
function normalizeExactReservationEvidence(r){
  return {
    source_customer_id:text(r.source_customer_id??r.reservation_customer_id??r.customer_id_hint),
    reservation_id:text(r.reservation_id??r.reservation_id_hash),
    target_customer_id:text(r.target_customer_id??r.crm_candidate_customer_id??r.crm_customer_id)
  };
}

function groupCandidates(candidates){
  const groups=new Map();
  for(const c of candidates||[]){
    const line=text(c.line_user_id);
    const currentHint=text(c.customer_id_hint);
    const legacyHint=text(c.legacy_customer_id_hint);
    const fallback=`NO_LINE::${currentHint||legacyHint||text(c.csv_file_name)||text(c.source_row)}`;
    const key=line||fallback;
    if(!groups.has(key))groups.set(key,[]);
    groups.get(key).push(c);
  }
  return [...groups.entries()].map(([key,rows])=>({
    key,
    line_user_id:LINE_ID_RE.test(key)?key:'',
    message_rows:rows.length,
    customer_id_hints:uniqueValues(rows,x=>CURRENT_ID_RE.test(text(x.customer_id_hint))?text(x.customer_id_hint):''),
    legacy_customer_id_hints:uniqueValues(rows,x=>text(x.legacy_customer_id_hint)),
    csv_file_names:uniqueValues(rows,x=>text(x.csv_file_name)),
    weak_names:uniqueValues(rows,x=>text(x.sender_name)),
    message_keys:uniqueValues(rows,x=>text(x.message_key))
  }));
}

function buildIndexes(customers,master,reviews,reservationIdentities,exactReservationEvidence){
  const active=(customers||[]).map(normalizeCustomer).filter(x=>x.customer_id&&!x.deleted_at);
  const masters=(master||[]).map(normalizeMaster).filter(x=>x.customer_id||x.line_user_id);
  const revs=(reviews||[]).map(normalizeReview);
  const reservations=(reservationIdentities||[]).map(normalizeReservationIdentity).filter(x=>x.customer_id);
  const exactReservations=(exactReservationEvidence||[]).map(normalizeExactReservationEvidence).filter(x=>x.source_customer_id&&x.reservation_id);

  const prodById=new Map(),prodByLine=new Map(),prodByPhone=new Map(),prodByEmail=new Map(),prodByName=new Map();
  for(const c of active){
    prodById.set(c.customer_id,c);
    add(prodByLine,c.line_user_id,c);
    add(prodByPhone,c.phone,c);
    add(prodByEmail,c.email,c);
    add(prodByName,normName(c.name),c);
  }
  const masterByLine=new Map();
  for(const c of masters)add(masterByLine,c.line_user_id,c);

  const reviewByReservation=new Map();
  for(const r of revs)add(reviewByReservation,r.reservation_customer_id,r);

  const reservationById=new Map();
  for(const r of reservations)add(reservationById,r.customer_id,r);

  const exactReservationBySource=new Map();
  for(const r of exactReservations)add(exactReservationBySource,r.source_customer_id,r);

  return {active,prodById,prodByLine,prodByPhone,prodByEmail,prodByName,masterByLine,reviewByReservation,reservationById,exactReservationBySource};
}

function exactContactTargets(reservation,indexes){
  const ids=new Set();
  let phoneMatches=[],emailMatches=[];
  if(reservation?.phone)phoneMatches=indexes.prodByPhone.get(reservation.phone)||[];
  if(reservation?.email)emailMatches=indexes.prodByEmail.get(reservation.email)||[];
  for(const c of phoneMatches)ids.add(c.customer_id);
  for(const c of emailMatches)ids.add(c.customer_id);
  return {ids:[...ids],phone_matches:uniqueValues(phoneMatches,x=>x.customer_id),email_matches:uniqueValues(emailMatches,x=>x.customer_id)};
}

function classifyGroup(group,indexes){
  const evidence=[];
  const conflicts=[];
  const line=group.line_user_id;

  if(line){
    const direct=indexes.prodByLine.get(line)||[];
    if(direct.length===1){
      return {category:'ALREADY_RESOLVED',reason:'PRODUCTION_EXACT_LINE',target_customer_id:direct[0].customer_id,evidence:['production_line_exact'],conflicts};
    }
    if(direct.length>1){
      return {category:'BLOCKED_CONFLICT',reason:'PRODUCTION_DUPLICATE_LINE_ID',target_customer_id:'',evidence,conflicts:['multiple_production_customers_share_line_id']};
    }
  }

  const hinted=[...new Set([...group.customer_id_hints,...group.legacy_customer_id_hints])].filter(Boolean);
  const reviewTargets=[];
  const pendingReviewTargets=[];
  let hasPendingReview=false;
  let hasDifferentReview=false;
  for(const hint of hinted){
    for(const r of indexes.reviewByReservation.get(hint)||[]){
      if(r.decision==='SAME_PERSON'&&indexes.prodById.has(r.crm_candidate_customer_id)){
        reviewTargets.push(r.crm_candidate_customer_id);
        evidence.push('explicit_same_person_review:'+hint+'->'+r.crm_candidate_customer_id);
      }else if(r.decision==='DIFFERENT_PERSON'){
        hasDifferentReview=true;
        conflicts.push('explicit_different_person_review:'+hint+'->'+r.crm_candidate_customer_id);
      }else if(r.decision==='DEFERRED'||r.decision==='UNREVIEWED'){
        hasPendingReview=true;
        if(indexes.prodById.has(r.crm_candidate_customer_id))pendingReviewTargets.push(r.crm_candidate_customer_id);
        evidence.push('pending_reconciliation_review:'+hint+'->'+r.crm_candidate_customer_id);
      }
    }
  }
  const uniqueReviewTargets=[...new Set(reviewTargets)];
  if(uniqueReviewTargets.length===1&&!hasDifferentReview){
    return {category:'AUTO_CONFIRMABLE',reason:'EXPLICIT_SAME_PERSON_REVIEW',target_customer_id:uniqueReviewTargets[0],evidence,conflicts};
  }
  if(uniqueReviewTargets.length>1){
    return {category:'BLOCKED_CONFLICT',reason:'MULTIPLE_SAME_PERSON_REVIEW_TARGETS',target_customer_id:'',evidence,conflicts:['same_source_id_maps_to_multiple_current_customers']};
  }
  const uniquePendingTargets=[...new Set(pendingReviewTargets)];
  if(uniquePendingTargets.length===1&&!hasDifferentReview){
    return {category:'REVIEW_REQUIRED',reason:'EXISTING_REVIEW_NOT_CONFIRMED',target_customer_id:uniquePendingTargets[0],evidence,conflicts};
  }
  if(uniquePendingTargets.length>1){
    return {category:'BLOCKED_CONFLICT',reason:'MULTIPLE_PENDING_REVIEW_TARGETS',target_customer_id:'',evidence,conflicts:['pending_reviews_point_to_multiple_current_customers']};
  }
  if(hasDifferentReview){
    return {category:'BLOCKED_CONFLICT',reason:'EXPLICIT_DIFFERENT_PERSON_REVIEW',target_customer_id:'',evidence,conflicts};
  }

  const exactReservationRows=[];
  for(const hint of hinted){
    exactReservationRows.push(...(indexes.exactReservationBySource.get(hint)||[]));
  }
  if(exactReservationRows.length){
    evidence.push('reservation_id_exact');
    const targetIds=[...new Set(exactReservationRows.map(x=>x.target_customer_id).filter(Boolean))];

    if(targetIds.length>1){
      return {
        category:'BLOCKED_CONFLICT',
        reason:'MULTIPLE_EXACT_RESERVATION_TARGETS',
        target_customer_id:'',
        evidence,
        conflicts:[...conflicts,'exact_reservation_ids_point_to_multiple_current_customer_ids']
      };
    }

    if(exactReservationRows.some(x=>!x.target_customer_id)){
      return {
        category:'REVIEW_REQUIRED',
        reason:'EXACT_RESERVATION_TARGET_MISSING',
        target_customer_id:'',
        evidence:[...evidence,'exact_reservation_target_missing'],
        conflicts
      };
    }

    if(targetIds.length===0){
      return {
        category:'REVIEW_REQUIRED',
        reason:'EXACT_RESERVATION_TARGET_MISSING',
        target_customer_id:'',
        evidence:[...evidence,'exact_reservation_target_missing'],
        conflicts
      };
    }

    const targetId=targetIds[0];
    const target=indexes.prodById.get(targetId);
    if(!CURRENT_ID_RE.test(targetId)||!target){
      return {
        category:'REVIEW_REQUIRED',
        reason:'EXACT_RESERVATION_TARGET_NOT_CURRENT',
        target_customer_id:CURRENT_ID_RE.test(targetId)?targetId:'',
        evidence:[...evidence,'exact_reservation_target_not_current'],
        conflicts
      };
    }

    if(!line){
      return {
        category:'REVIEW_REQUIRED',
        reason:'EXACT_RESERVATION_REQUIRES_LINE_ID',
        target_customer_id:targetId,
        evidence:[...evidence,'candidate_line_id_missing'],
        conflicts
      };
    }

    if(target.line_user_id&&target.line_user_id!==line){
      return {
        category:'BLOCKED_CONFLICT',
        reason:'EXACT_RESERVATION_LINE_CONFLICT',
        target_customer_id:targetId,
        evidence:[...evidence,'exact_reservation_unique_current_target'],
        conflicts:[...conflicts,'exact_reservation_target_has_different_nonempty_line_id']
      };
    }

    return {
      category:'AUTO_CONFIRMABLE',
      reason:'EXACT_RESERVATION_ID_UNIQUE_CURRENT_TARGET',
      target_customer_id:targetId,
      evidence:[
        ...evidence,
        'exact_reservation_unique_current_target',
        target.line_user_id?'production_line_id_exact':'production_line_id_empty'
      ],
      conflicts
    };
  }

  if(line){
    const masterMatches=indexes.masterByLine.get(line)||[];
    const masterIds=[...new Set(masterMatches.map(x=>x.customer_id).filter(Boolean))];
    if(masterIds.length===1){
      const target=indexes.prodById.get(masterIds[0]);
      if(target){
        if(target.line_user_id&&target.line_user_id!==line){
          return {category:'BLOCKED_CONFLICT',reason:'MASTER_LINE_CONFLICTS_WITH_PRODUCTION_LINE',target_customer_id:target.customer_id,evidence:['customer_master_exact_line'],conflicts:['production_customer_has_different_nonempty_line_id']};
        }
        if(!target.line_user_id){
          return {category:'AUTO_CONFIRMABLE',reason:'MASTER_EXACT_LINE_TO_EXISTING_CURRENT_ID',target_customer_id:target.customer_id,evidence:['customer_master_exact_line','production_current_customer_exists','production_line_id_empty'],conflicts};
        }
      }else{
        evidence.push('customer_master_exact_line_but_customer_missing_in_production:'+masterIds[0]);
        const masterNames=[...new Set(masterMatches.map(x=>normName(x.name)).filter(Boolean))];
        if(masterNames.length===1){
          const nameTargets=[...new Set((indexes.prodByName.get(masterNames[0])||[]).map(x=>x.customer_id))];
          if(nameTargets.length===1){
            return {category:'REVIEW_REQUIRED',reason:'MASTER_LINE_NAME_UNIQUE_CURRENT_CANDIDATE',target_customer_id:nameTargets[0],evidence:[...evidence,'name_only_unique_current_candidate'],conflicts};
          }
          if(nameTargets.length>1)evidence.push('master_name_ambiguous_in_production');
          else evidence.push('master_name_missing_in_production');
        }
      }
    }else if(masterIds.length>1){
      conflicts.push('customer_master_line_maps_to_multiple_customer_ids');
    }
  }

  for(const hint of hinted){
    const reservation=one(indexes.reservationById.get(hint)||[]);
    if(!reservation)continue;
    const contacts=exactContactTargets(reservation,indexes);
    if(contacts.ids.length===1){
      const target=indexes.prodById.get(contacts.ids[0]);
      if(target&&line&&target.line_user_id&&target.line_user_id!==line){
        conflicts.push('contact_target_has_different_nonempty_line_id');
      }else{
        return {
          category:'AUTO_CONFIRMABLE',
          reason:'UNIQUE_EXACT_CONTACT_MATCH',
          target_customer_id:contacts.ids[0],
          evidence:[
            ...(contacts.phone_matches.length?['reservation_phone_exact'] : []),
            ...(contacts.email_matches.length?['reservation_email_exact'] : [])
          ],
          conflicts
        };
      }
    }
    if(contacts.ids.length>1)conflicts.push('contact_evidence_points_to_multiple_customers');

    const byName=indexes.prodByName.get(normName(reservation.name))||[];
    const nameIds=[...new Set(byName.map(x=>x.customer_id))];
    if(nameIds.length===1)evidence.push('name_only_unique:'+nameIds[0]);
    else if(nameIds.length>1)evidence.push('name_only_ambiguous');
  }

  if(conflicts.length||hasDifferentReview){
    return {category:'BLOCKED_CONFLICT',reason:'CONTRADICTORY_EVIDENCE',target_customer_id:'',evidence,conflicts};
  }
  if(hasPendingReview||evidence.length){
    return {category:'REVIEW_REQUIRED',reason:hasPendingReview?'EXISTING_REVIEW_NOT_CONFIRMED':'WEAK_OR_INCOMPLETE_EVIDENCE',target_customer_id:'',evidence,conflicts};
  }
  return {category:'UNRESOLVED',reason:'NO_SAFE_IDENTITY_EVIDENCE',target_customer_id:'',evidence,conflicts};
}

export function classifyLineHistoryUnresolved({candidates=[],customers=[],customerMaster=[],reviews=[],reservationIdentities=[],exactReservationEvidence=[]}={}){
  const groups=groupCandidates(candidates);
  const indexes=buildIndexes(customers,customerMaster,reviews,reservationIdentities,exactReservationEvidence);
  const classified=groups.map(group=>{
    const result=classifyGroup(group,indexes);
    return {
      line_id_hash:group.line_user_id?lineHash(group.line_user_id):'',
      line_user_id_present:!!group.line_user_id,
      message_rows:group.message_rows,
      customer_id_hints:group.customer_id_hints,
      legacy_customer_id_hints:group.legacy_customer_id_hints,
      csv_file_names:group.csv_file_names,
      category:result.category,
      reason:result.reason,
      target_customer_id:result.target_customer_id,
      evidence:result.evidence,
      conflicts:result.conflicts
    };
  });

  const unresolved=classified.filter(x=>x.category!=='ALREADY_RESOLVED');
  const countGroups=category=>unresolved.filter(x=>x.category===category).length;
  const countMessages=category=>unresolved.filter(x=>x.category===category).reduce((n,x)=>n+x.message_rows,0);

  return {
    planner:'line_history_unresolved_triage_v2',
    candidate_message_rows:(candidates||[]).length,
    identity_groups:classified.length,
    already_resolved_groups:classified.filter(x=>x.category==='ALREADY_RESOLVED').length,
    already_resolved_message_rows:classified.filter(x=>x.category==='ALREADY_RESOLVED').reduce((n,x)=>n+x.message_rows,0),
    unresolved_identity_groups:unresolved.length,
    unresolved_message_rows:unresolved.reduce((n,x)=>n+x.message_rows,0),
    auto_confirmable_groups:countGroups('AUTO_CONFIRMABLE'),
    auto_confirmable_message_rows:countMessages('AUTO_CONFIRMABLE'),
    review_required_groups:countGroups('REVIEW_REQUIRED'),
    review_required_message_rows:countMessages('REVIEW_REQUIRED'),
    unresolved_groups:countGroups('UNRESOLVED'),
    fully_unresolved_message_rows:countMessages('UNRESOLVED'),
    blocked_conflict_groups:countGroups('BLOCKED_CONFLICT'),
    blocked_conflict_message_rows:countMessages('BLOCKED_CONFLICT'),
    classifications:classified,
    safety:{
      production_d1_write:0,
      customer_id_generation:0,
      customer_update:0,
      customer_delete:0,
      line_send:0,
      name_only_auto_link:false,
      output_contains_message_text:false
    }
  };
}
