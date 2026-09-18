import { createHash } from 'node:crypto';

const REVIEW_CATEGORIES=new Set(['REVIEW_REQUIRED','BLOCKED_CONFLICT','UNRESOLVED']);

function text(v){return v==null?'':String(v).trim()}

function hashParts(parts,length=16){
  return createHash('sha256')
    .update(JSON.stringify(parts))
    .digest('hex')
    .slice(0,length);
}

function sanitizedTypes(items){
  return [...new Set((items||[])
    .map(v=>text(v).split(':',1)[0])
    .map(v=>v.replace(/[^A-Za-z0-9_\-]/g,''))
    .filter(Boolean))]
    .sort();
}

function actionFor(category){
  if(category==='BLOCKED_CONFLICT')return 'RESOLVE_CONFLICT';
  if(category==='REVIEW_REQUIRED')return 'OWNER_REVIEW';
  return 'NEEDS_MORE_EVIDENCE';
}

function sortRank(category){
  if(category==='BLOCKED_CONFLICT')return 0;
  if(category==='REVIEW_REQUIRED')return 1;
  return 2;
}

export function buildLineHistoryOwnerReviewQueue(triage={}){
  const classifications=Array.isArray(triage.classifications)?triage.classifications:[];
  const items=[];

  for(const row of classifications){
    const category=text(row.category);
    if(!REVIEW_CATEGORIES.has(category))continue;

    const currentHints=[...(row.customer_id_hints||[])].map(text).filter(Boolean).sort();
    const legacyHints=[...(row.legacy_customer_id_hints||[])].map(text).filter(Boolean).sort();
    const target=text(row.target_customer_id);
    const lineHash=text(row.line_id_hash);

    const sourceIdentityHash=hashParts([
      lineHash,
      currentHints,
      legacyHints
    ]);

    const queueId=hashParts([
      category,
      text(row.reason),
      sourceIdentityHash,
      Number(row.message_rows||0)
    ],20);

    items.push({
      queue_id:queueId,
      source_identity_hash:sourceIdentityHash,
      line_id_hash:lineHash,
      line_user_id_present:!!row.line_user_id_present,
      message_rows:Number(row.message_rows||0),
      category,
      reason:text(row.reason),
      review_action:actionFor(category),
      current_hint_count:currentHints.length,
      legacy_hint_count:legacyHints.length,
      target_customer_id_present:!!target,
      target_customer_id_hash:target?hashParts([target]):'',
      evidence_types:sanitizedTypes(row.evidence),
      conflict_types:sanitizedTypes(row.conflicts)
    });
  }

  items.sort((a,b)=>
    sortRank(a.category)-sortRank(b.category)||
    b.message_rows-a.message_rows||
    a.queue_id.localeCompare(b.queue_id)
  );

  const countCategory=category=>items.filter(x=>x.category===category).length;
  const messageCategory=category=>items
    .filter(x=>x.category===category)
    .reduce((n,x)=>n+x.message_rows,0);

  return {
    planner:'line_history_owner_review_queue_v1',
    source_candidate_message_rows:Number(triage.candidate_message_rows||0),
    review_queue_groups:items.length,
    review_queue_message_rows:items.reduce((n,x)=>n+x.message_rows,0),
    blocked_conflict_groups:countCategory('BLOCKED_CONFLICT'),
    blocked_conflict_message_rows:messageCategory('BLOCKED_CONFLICT'),
    review_required_groups:countCategory('REVIEW_REQUIRED'),
    review_required_message_rows:messageCategory('REVIEW_REQUIRED'),
    unresolved_groups:countCategory('UNRESOLVED'),
    unresolved_message_rows:messageCategory('UNRESOLVED'),
    items,
    safety:{
      production_d1_read:0,
      production_d1_write:0,
      customer_id_generation:0,
      customer_update:0,
      customer_delete:0,
      customer_merge:0,
      line_send:0,
      raw_customer_id_output:false,
      raw_line_user_id_output:false,
      message_text_output:false,
      customer_name_output:false,
      csv_file_name_output:false
    }
  };
}
