const text=v=>v==null?'':String(v).trim();

const REASON_LABELS=Object.freeze({
  'event<=30':'30日以内の家族イベント',
  'event<=90':'90日以内の家族イベント',
  'high_ltv+event':'高LTV + 家族イベント',
  'first_shoot_no_repeat':'初回撮影後・未リピート',
  'dormant365':'365日以上の休眠',
  'dormant180':'180日以上の休眠',
  'high_ltv_dormant':'高LTV + 休眠'
});

function contactAvailability(view){
  const raw=view?.raw||{};
  return {
    line:!!view?.line_linked,
    phone:!!text(raw.phone),
    email:!!text(raw.email)
  };
}

function chooseChannel(preferred,available){
  const p=text(preferred).toUpperCase();
  if(p==='LINE'&&available.line)return'LINE';
  if((p==='PHONE'||p==='電話')&&available.phone)return'phone';
  if((p==='EMAIL'||p==='メール')&&available.email)return'email';
  if(available.line)return'LINE';
  if(available.phone)return'phone';
  if(available.email)return'email';
  return'';
}

export function approachContactState(view){
  const available=contactAvailability(view);
  const consentStatus=text(view?.consent?.status);
  if(view?.consent?.marketing_opt_out===true||consentStatus==='opted_out'){
    return {code:'blocked_opt_out',label:'配信対象外 / opt-out',ready:false,review_required:false,suggested_channel:'',available};
  }
  if(!available.line&&!available.phone&&!available.email){
    return {code:'no_contact',label:'連絡先なし',ready:false,review_required:true,suggested_channel:'',available};
  }
  if(consentStatus==='unknown'){
    return {code:'review_consent',label:'連絡可否の確認が必要',ready:false,review_required:true,suggested_channel:chooseChannel(view?.consent?.preferred_contact_channel,available),available};
  }
  return {code:'manual_contact_ready',label:'手動連絡候補',ready:true,review_required:false,suggested_channel:chooseChannel(view?.consent?.preferred_contact_channel,available),available};
}

function queueItem(view){
  const contact=approachContactState(view),next=view.next_opportunity||null;
  const reasonCode=text(view?.recommendation?.priority_reason);
  return {
    customer_id:text(view.customer_id),
    name:text(view.name)||'名前未設定',
    priority_score:Number(view?.recommendation?.priority_score||0),
    priority_reason:reasonCode,
    priority_reason_label:REASON_LABELS[reasonCode]||'マーケティングルールによる候補',
    next_offer:text(view?.recommendation?.next_offer)||'家族写真 / リピート撮影',
    next_opportunity:next?{type:text(next.type),label:text(next.label),days:next.days==null?null:Number(next.days),member_name:text(next.member_name)}:null,
    contact,
    draft_text:text(view?.recommendation?.next_line),
    marketing_classes:[...(view?.marketing_classes||[])],
    realized_ltv:Number(view?.realized_ltv||0),
    shoot_count:Number(view?.shoot_count||0),
    last_shoot_date:text(view?.last_shoot_date)
  };
}

export function parseApproachQueueParams(searchParams){
  const rawH=text(searchParams?.get?.('horizon_days')),rawLimit=text(searchParams?.get?.('limit')),status=text(searchParams?.get?.('status')||'all');
  const horizonDays=rawH===''?90:Number(rawH),limit=rawLimit===''?50:Number(rawLimit);
  if(!Number.isInteger(horizonDays)||horizonDays<1||horizonDays>3650)throw new Error('invalid_horizon_days');
  if(!Number.isInteger(limit)||limit<1||limit>100)throw new Error('invalid_limit');
  if(!['all','ready','review','blocked'].includes(status))throw new Error('invalid_status');
  return {horizon_days:horizonDays,limit,status};
}

export function buildApproachQueue(views,params={horizon_days:90,limit:50,status:'all'}){
  const all=(views||[])
    .filter(v=>/^\d{8}$/.test(text(v.customer_id)))
    .filter(v=>Number(v?.recommendation?.priority_score||0)>0)
    .filter(v=>{
      const days=v?.next_opportunity?.days;
      return days==null||Number(days)<=params.horizon_days;
    })
    .map(queueItem)
    .filter(item=>{
      if(params.status==='ready')return item.contact.ready;
      if(params.status==='review')return item.contact.review_required&&item.contact.code!=='blocked_opt_out';
      if(params.status==='blocked')return item.contact.code==='blocked_opt_out';
      return true;
    })
    .sort((a,b)=>b.priority_score-a.priority_score-(0)||((a.next_opportunity?.days??99999)-(b.next_opportunity?.days??99999))||a.customer_id.localeCompare(b.customer_id));

  const summary={
    total:all.length,
    ready:all.filter(x=>x.contact.ready).length,
    review_required:all.filter(x=>x.contact.review_required&&x.contact.code!=='blocked_opt_out').length,
    opted_out:all.filter(x=>x.contact.code==='blocked_opt_out').length,
    no_contact:all.filter(x=>x.contact.code==='no_contact').length
  };
  return {
    items:all.slice(0,params.limit),
    total:all.length,
    summary,
    filters:params,
    meta:{
      read_only:true,
      identity_key:'customer_id',
      name_matching:false,
      customer_id_generation:false,
      customer_write:false,
      line_send:false,
      automatic_contact:false,
      contact_details_exposed:false
    }
  };
}

export function approachQueueReasonLabels(){
  return {...REASON_LABELS};
}
