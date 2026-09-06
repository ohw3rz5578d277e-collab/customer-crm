const JST = 'Asia/Tokyo';
export const RELATION_LABELS = Object.freeze({ spouse:'配偶者', child:'子ども', parent:'父母', grandparent:'祖父母', other:'その他' });
const RELATIONS = new Set(Object.keys(RELATION_LABELS));
const HIGH_LTV_THRESHOLD = 100000;

function text(v){ return v == null ? '' : String(v).trim(); }
function num(v, fallback=0){ const n=Number(String(v ?? '').replace(/[,円¥\s]/g,'')); return Number.isFinite(n)?n:fallback; }
function bool(v){ return v===true || v===1 || String(v).toLowerCase()==='true' || String(v)==='1'; }
function dateOnly(v){ const s=text(v); const m=s.match(/^(\d{4})-(\d{2})-(\d{2})/); return m?`${m[1]}-${m[2]}-${m[3]}`:''; }
function utcDate(y,m,d){ return new Date(Date.UTC(y,m-1,d)); }
function parts(v){ const s=dateOnly(v); if(!s)return null; const [y,m,d]=s.split('-').map(Number); const dt=utcDate(y,m,d); if(dt.getUTCFullYear()!==y||dt.getUTCMonth()+1!==m||dt.getUTCDate()!==d)return null; return {y,m,d,s}; }
function pad(n){ return String(n).padStart(2,'0'); }
function dateStr(y,m,d){ return `${y}-${pad(m)}-${pad(d)}`; }
function daysBetween(a,b){ const A=parts(a),B=parts(b); if(!A||!B)return null; return Math.round((utcDate(B.y,B.m,B.d)-utcDate(A.y,A.m,A.d))/86400000); }
function addMonths(date, months){ const p=parts(date); if(!p)return ''; const base=new Date(Date.UTC(p.y,p.m-1+months,p.d)); if(base.getUTCDate()!==p.d){ const end=new Date(Date.UTC(base.getUTCFullYear(),base.getUTCMonth()+1,0)); return dateStr(end.getUTCFullYear(),end.getUTCMonth()+1,end.getUTCDate()); } return dateStr(base.getUTCFullYear(),base.getUTCMonth()+1,base.getUTCDate()); }
function addYears(date, years){ const p=parts(date); if(!p)return ''; const d=Math.min(p.d,new Date(Date.UTC(p.y+years,p.m,0)).getUTCDate()); return dateStr(p.y+years,p.m,d); }

export function jstToday(now = new Date()){
  const f=new Intl.DateTimeFormat('en-CA',{timeZone:JST,year:'numeric',month:'2-digit',day:'2-digit'});
  const obj=Object.fromEntries(f.formatToParts(now).filter(x=>x.type!=='literal').map(x=>[x.type,x.value]));
  return `${obj.year}-${obj.month}-${obj.day}`;
}

export function ageOn(birthdate, onDate=jstToday()){
  const b=parts(birthdate),o=parts(onDate); if(!b||!o)return null;
  let age=o.y-b.y; if(o.m<b.m || (o.m===b.m&&o.d<b.d)) age--;
  return age>=0?age:null;
}

export function nextBirthday(birthdate, onDate=jstToday()){
  const b=parts(birthdate),o=parts(onDate); if(!b||!o)return null;
  let y=o.y; let d=Math.min(b.d,new Date(Date.UTC(y,b.m,0)).getUTCDate()); let candidate=dateStr(y,b.m,d);
  if(daysBetween(onDate,candidate)<0){ y++; d=Math.min(b.d,new Date(Date.UTC(y,b.m,0)).getUTCDate()); candidate=dateStr(y,b.m,d); }
  return {date:candidate,days:daysBetween(onDate,candidate),next_age:y-b.y};
}

function legacyMember(customer,index){
  const name=text(customer[`child${index}_name`]); const birthdate=dateOnly(customer[`child${index}_birthdate`]);
  if(!name&&!birthdate)return null;
  return {id:`legacy-child-${index}`,customer_id:text(customer.customer_id),relation:'child',name,furigana:'',birthdate,gender:null,school_stage:null,memo:'',created_at:'',updated_at:'',deleted_at:null,source:'legacy'};
}

export function legacyFamilyMembers(customer={}){ return [1,2,3].map(i=>legacyMember(customer,i)).filter(Boolean); }
export function mergeFamilyMembers(customer={}, managed=[]){
  const active=(managed||[]).filter(x=>!text(x.deleted_at)).map(x=>({...x,relation:RELATIONS.has(text(x.relation))?text(x.relation):'other',source:x.source||'managed'}));
  const legacy=legacyFamilyMembers(customer).filter(l=>!active.some(m=>m.relation==='child'&&text(m.name)===text(l.name)&&dateOnly(m.birthdate)===dateOnly(l.birthdate)));
  return [...active,...legacy];
}

export function normalizeAddress(customer={}, profile={}){
  const raw=text(customer.address); let prefecture=text(profile.prefecture||customer.prefecture), city=text(profile.city||customer.city);
  if(!prefecture){ const m=raw.match(/^(東京都|北海道|(?:京都|大阪)府|.{2,3}県)/); prefecture=m?m[1]:''; }
  if(!city&&prefecture&&raw.startsWith(prefecture)){ const rest=raw.slice(prefecture.length); const m=rest.match(/^(.+?(?:市|区|町|村))/); city=m?m[1]:''; }
  return {postal_code:text(profile.postal_code||customer.postal_code),prefecture,city,address_line1:text(profile.address_line1),address_line2:text(profile.address_line2),raw_address:raw,summary:[prefecture,city].filter(Boolean).join(' ')};
}

function opportunity(type,label,date,days,extra={}){ return {type,label,date:date||'',days:days==null?null:Number(days),candidate:true,...extra}; }
function pushUnique(list,item){ const k=[item.type,item.member_id||'',item.date||'',item.label].join('|'); if(!list.some(x=>[x.type,x.member_id||'',x.date||'',x.label].join('|')===k))list.push(item); }
function schoolStageOpportunity(member){
  const s=text(member.school_stage); if(!s)return null;
  if(/年長/.test(s)) return '小学校入学候補';
  if(/小学.*6|小6/.test(s)) return '小学校卒業候補';
  if(/中学.*3|中3/.test(s)) return '中学校卒業候補';
  if(/高校.*3|高3/.test(s)) return '高校卒業候補';
  return null;
}

export function buildOpportunities(customer={}, family=[], onDate=jstToday()){
  const out=[];
  for(const member of family||[]){
    if(text(member.relation)!=='child' || !dateOnly(member.birthdate)) continue;
    const age=ageOn(member.birthdate,onDate); const nb=nextBirthday(member.birthdate,onDate); const memberMeta={member_id:text(member.id),member_name:text(member.name),relation:'child',age};
    if(nb) pushUnique(out,opportunity('birthday',`${text(member.name)||'お子さま'} ${nb.next_age}歳誕生日`,nb.date,nb.days,memberMeta));
    const half=addMonths(member.birthdate,6), halfDays=daysBetween(onDate,half); if(halfDays!=null&&halfDays>=0&&halfDays<=180) pushUnique(out,opportunity('half_birthday','ハーフバースデー候補',half,halfDays,memberMeta));
    const first=addYears(member.birthdate,1), firstDays=daysBetween(onDate,first); if(firstDays!=null&&firstDays>=0&&firstDays<=365) pushUnique(out,opportunity('first_birthday','1歳バースデー候補',first,firstDays,memberMeta));
    if(nb&&[3,5,7].includes(nb.next_age)&&nb.days<=365) pushUnique(out,opportunity('shichigosan',`${nb.next_age}歳七五三候補`,nb.date,nb.days,memberMeta));
    if([3,5,7].includes(age)) pushUnique(out,opportunity('shichigosan',`${age}歳七五三候補`,'',0,memberMeta));
    const stageLabel=schoolStageOpportunity(member); if(stageLabel) pushUnique(out,opportunity('school_entry_candidate',stageLabel,'',0,{...memberMeta,source:'school_stage'}));
    if(nb){
      const map={3:'入園候補',6:'小学校入学候補',12:'小学校卒業候補',15:'中学校卒業候補',18:'高校卒業候補'};
      if(map[nb.next_age]) pushUnique(out,opportunity(nb.next_age===6?'school_entry_candidate':'graduation_candidate',map[nb.next_age],nb.date,nb.days,{...memberMeta,source:'birthdate_estimate'}));
      if(nb.next_age===20) pushUnique(out,opportunity('coming_of_age_candidate','20歳成人記念候補',nb.date,nb.days,memberMeta));
    }
    if(age===19||age===20) pushUnique(out,opportunity('coming_of_age_candidate','成人記念候補','',0,memberMeta));
  }
  const anniversary=dateOnly(customer.anniversary); if(anniversary){ const a=nextBirthday(anniversary,onDate); if(a) pushUnique(out,opportunity('family_anniversary','家族記念日',a.date,a.days)); }
  const repeat=num(customer.repeat_count,0), dormant=num(customer.dormant_days,0), ltv=num(customer.total_revenue,0);
  if(text(customer.first_shoot_date)&&repeat<=1) pushUnique(out,opportunity('repeat_candidate','初回からのリピート候補','',null));
  if(dormant>=180) pushUnique(out,opportunity('dormant_180','休眠180日','',null,{dormant_days:dormant}));
  if(dormant>=365) pushUnique(out,opportunity('dormant_365','休眠365日','',null,{dormant_days:dormant}));
  if(ltv>=HIGH_LTV_THRESHOLD) pushUnique(out,opportunity('high_ltv','高LTV顧客','',null,{realized_ltv:ltv}));
  return out.sort((a,b)=>(a.days==null?99999:a.days)-(b.days==null?99999:b.days)||a.type.localeCompare(b.type));
}

export function rfmScore(customer={}){
  const dormant=Math.max(0,num(customer.dormant_days,99999)); const frequency=Math.max(0,num(customer.repeat_count,0)); const monetary=Math.max(0,num(customer.total_revenue,0));
  const R=dormant<=90?5:dormant<=180?4:dormant<=365?3:dormant<=730?2:1;
  const F=frequency>=5?5:frequency>=4?4:frequency>=3?3:frequency>=2?2:1;
  const M=monetary>=200000?5:monetary>=120000?4:monetary>=70000?3:monetary>=30000?2:1;
  return {R,F,M,overall:Math.max(1,Math.min(5,Math.round((R+F+M)/3)))};
}

export function marketingClasses(customer={}, opportunities=[], rfm=rfmScore(customer)){
  const set=new Set(); const repeat=num(customer.repeat_count,0), dormant=num(customer.dormant_days,0), ltv=num(customer.total_revenue,0);
  if(ltv>=200000||rfm.overall===5)set.add('VIP');
  if(repeat>=3)set.add('LOYAL');
  if(text(customer.first_shoot_date)&&repeat<=1)set.add('REPEAT_CANDIDATE');
  if(repeat<=1&&num(customer.dormant_days,0)<180)set.add('NEW_CUSTOMER');
  if(dormant>=180)set.add('DORMANT');
  if(dormant>=365||(dormant>=180&&ltv>=HIGH_LTV_THRESHOLD))set.add('AT_RISK');
  if(opportunities.some(o=>o.days!=null&&o.days>=0&&o.days<=90))set.add('EVENT_OPPORTUNITY');
  return [...set];
}

function recommendOffer(opportunities=[]){
  const preferred=['shichigosan','first_birthday','birthday','school_entry_candidate','graduation_candidate','coming_of_age_candidate','family_anniversary'];
  const hit=preferred.map(t=>opportunities.find(o=>o.type===t)).find(Boolean);
  if(!hit)return '家族写真 / リピート撮影';
  if(hit.type==='shichigosan')return '七五三 + 家族写真';
  if(hit.type==='first_birthday')return '1歳バースデー + 家族写真';
  if(hit.type==='coming_of_age_candidate')return '成人記念 + 家族写真';
  if(hit.type.includes('school')||hit.type==='graduation_candidate')return '入学・卒業 + 家族写真';
  return `${hit.label} 撮影`;
}

export function recommendationPriority(customer={}, opportunities=[]){
  const ltv=num(customer.total_revenue,0), dormant=num(customer.dormant_days,0), repeat=num(customer.repeat_count,0);
  const event=opportunities.filter(o=>o.days!=null&&o.days>=0).sort((a,b)=>a.days-b.days)[0];
  let score=0, reason='';
  if(event&&event.days<=30){score=1000;reason='event<=30';}
  else if(event&&event.days<=90){score=900;reason='event<=90';}
  if(event&&event.days<=90&&ltv>=HIGH_LTV_THRESHOLD){score+=80;reason='high_ltv+event';}
  if(text(customer.first_shoot_date)&&repeat<=1&&score<650){score=650;reason='first_shoot_no_repeat';}
  if(dormant>=365&&score<500){score=500;reason='dormant365';}
  else if(dormant>=180&&score<550){score=550;reason='dormant180';}
  if(dormant>=180&&ltv>=HIGH_LTV_THRESHOLD&&score<700){score=700;reason='high_ltv_dormant';}
  return {score,reason,event:event||null};
}

export function buildCustomerMarketingView(customer={}, family=[], profile={}, onDate=jstToday()){
  const merged=mergeFamilyMembers(customer,family); const opportunities=buildOpportunities(customer,merged,onDate); const rfm=rfmScore(customer); const classes=marketingClasses(customer,opportunities,rfm); const priority=recommendationPriority(customer,opportunities); const address=normalizeAddress(customer,profile);
  const marketingOptOut=profile.marketing_opt_out==null?null:bool(profile.marketing_opt_out);
  const contactPermission=text(profile.marketing_contact_permission).toLowerCase();
  const consent=(marketingOptOut===true||contactPermission==='denied')?'opted_out':contactPermission==='allowed'?'explicit_allowed':'unknown';
  return {
    customer_id:text(customer.customer_id),name:text(customer.name||customer.line_display_name),customer_rank:text(customer.customer_rank),line_linked:!!text(customer.line_user_id),line_display_name:text(customer.line_display_name),
    realized_ltv:num(customer.total_revenue,0),shoot_count:num(customer.repeat_count,0),avg_order_value:num(customer.avg_order_value||customer.square_avg_payment,0),last_shoot_date:dateOnly(customer.last_shoot_date),first_shoot_date:dateOnly(customer.first_shoot_date),
    family:merged,family_summary:`子${merged.filter(x=>x.relation==='child').length}人`,address,rfm,marketing_classes:classes,opportunities,next_opportunity:opportunities.find(o=>o.days!=null&&o.days>=0)||opportunities[0]||null,
    recommendation:{priority_score:priority.score,priority_reason:priority.reason,next_offer:recommendOffer(opportunities),next_line:buildLineDraft(customer,priority.event||opportunities[0])},
    consent:{marketing_opt_out:marketingOptOut,marketing_contact_permission:contactPermission||'unknown',preferred_contact_channel:text(profile.preferred_contact_channel),status:consent},raw:customer
  };
}

export function buildLineDraft(customer={}, opportunity=null){
  const name=text(customer.name||customer.line_display_name)||'お客様'; const label=opportunity?opportunity.label:'次回撮影';
  return `${name}様、${label}の時期が近づいてきました。ご家族の今を残す撮影をご検討でしたら、日程や内容をお気軽にご相談ください。`;
}

export function filterMarketingViews(views=[], filters={}){
  return views.filter(v=>{
    if(filters.ltv_min!=null&&v.realized_ltv<num(filters.ltv_min))return false;
    if(filters.shoot_count_min!=null&&v.shoot_count<num(filters.shoot_count_min))return false;
    if(filters.dormant_days_min!=null&&num(v.raw.dormant_days)<num(filters.dormant_days_min))return false;
    if(filters.event_days_max!=null&&!(v.next_opportunity&&v.next_opportunity.days!=null&&v.next_opportunity.days<=num(filters.event_days_max)))return false;
    if(filters.child_age!=null&&!v.family.some(m=>m.relation==='child'&&ageOn(m.birthdate,filters.on_date||jstToday())===num(filters.child_age)))return false;
    if(text(filters.genre)&&!text(v.raw.genre_history).includes(text(filters.genre)))return false;
    if(text(filters.city)&&!text(v.address.city).includes(text(filters.city)))return false;
    if(filters.line_linked!=null&&v.line_linked!==bool(filters.line_linked))return false;
    if(filters.photo_public_ok!=null&&bool(v.raw.photo_public_ok)!==bool(filters.photo_public_ok))return false;
    if(text(filters.source)&&!text(v.raw.acquisition_source||v.raw.source_name||v.raw.source_type).includes(text(filters.source)))return false;
    return true;
  });
}

export const CUSTOMER360_HIGH_LTV_THRESHOLD = HIGH_LTV_THRESHOLD;
