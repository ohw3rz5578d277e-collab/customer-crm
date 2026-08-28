import { ageOn } from './crm-customer360-marketing-engine.mjs';

const text=v=>v==null?'':String(v).trim();
const num=v=>{if(v==null||text(v)==='')return null;const n=Number(String(v).replace(/[,円¥\s]/g,''));return Number.isFinite(n)?n:NaN};
const dateOnly=v=>{const m=text(v).match(/^(\d{4}-\d{2}-\d{2})/);return m?m[1]:''};
const list=v=>text(v).split(',').map(x=>x.trim()).filter(Boolean);
const norm=v=>text(v).normalize('NFKC').toLowerCase().replace(/\s+/g,'');
const phone=v=>norm(v).replace(/\D/g,'');
const PHOTO_OK=new Set(['1','true','ok','yes']);
const PHOTO_NG=new Set(['0','false','ng','no']);
export const CUSTOMER360_SORTS=Object.freeze(['recommended','event_soon','ltv_desc','ltv_asc','shoot_desc','last_shoot_desc','last_shoot_asc','dormant_desc','name_asc']);
export const CUSTOMER360_PAGE_SIZE_DEFAULT=50;
export const CUSTOMER360_PAGE_SIZE_MAX=100;

function photoStatus(raw={}){const v=raw.photo_public_ok;if(v===true||v===1||PHOTO_OK.has(text(v).toLowerCase()))return'ok';if(v===false||v===0||PHOTO_NG.has(text(v).toLowerCase()))return'ng';return'unknown'}
function sourceValue(raw={}){return text(raw.acquisition_source||raw.source_name||raw.source_type||raw.referrer)}
function campaignValue(raw={}){return text(raw.campaign_name)}
function eventMatches(o,type){if(!type)return true;if(type==='school')return /school|graduation/.test(text(o.type));if(type==='graduation')return text(o.type)==='graduation_candidate';if(type==='adult')return text(o.type)==='coming_of_age_candidate';return text(o.type)===type}
function inDateRange(value,from,to){const d=dateOnly(value);if(!from&&!to)return true;if(!d)return false;return(!from||d>=from)&&(!to||d<=to)}
function finiteRange(value,min,max){return(min==null||value>=min)&&(max==null||value<=max)}
function maybeNumber(sp,name,{min=0,max=Number.MAX_SAFE_INTEGER,integer=false}={}){const raw=sp.get(name);if(raw==null||raw==='')return null;const n=num(raw);if(!Number.isFinite(n)||n<min||n>max||(integer&&!Number.isInteger(n)))throw new Error(`invalid_${name}`);return n}
function maybeDate(sp,name){const raw=text(sp.get(name));if(!raw)return'';const m=raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);if(!m)throw new Error(`invalid_${name}`);const y=Number(m[1]),mo=Number(m[2]),d=Number(m[3]),dt=new Date(Date.UTC(y,mo-1,d));if(dt.getUTCFullYear()!==y||dt.getUTCMonth()+1!==mo||dt.getUTCDate()!==d)throw new Error(`invalid_${name}`);return raw}
function ensureOrder(a,b,label){if(a!=null&&b!=null&&a>b)throw new Error(`invalid_${label}_range`)}

export function parseCustomerSearchParams(sp){
  const p={q:text(sp.get('q')).slice(0,200),ltv_min:maybeNumber(sp,'ltv_min'),ltv_max:maybeNumber(sp,'ltv_max'),aov_min:maybeNumber(sp,'aov_min'),aov_max:maybeNumber(sp,'aov_max'),shoot_min:maybeNumber(sp,'shoot_min',{integer:true}),shoot_max:maybeNumber(sp,'shoot_max',{integer:true}),dormant_min:maybeNumber(sp,'dormant_min',{integer:true}),dormant_max:maybeNumber(sp,'dormant_max',{integer:true}),family_min:maybeNumber(sp,'family_min',{integer:true}),family_max:maybeNumber(sp,'family_max',{integer:true}),child_min:maybeNumber(sp,'child_min',{integer:true}),child_max:maybeNumber(sp,'child_max',{integer:true}),child_age_min:maybeNumber(sp,'child_age_min',{min:0,max:120,integer:true}),child_age_max:maybeNumber(sp,'child_age_max',{min:0,max:120,integer:true}),event_days_max:maybeNumber(sp,'event_days_max',{min:0,max:3650,integer:true}),page:maybeNumber(sp,'page',{min:1,max:1000000,integer:true})??1,page_size:maybeNumber(sp,'page_size',{min:1,max:CUSTOMER360_PAGE_SIZE_MAX,integer:true})??CUSTOMER360_PAGE_SIZE_DEFAULT,last_shoot_from:maybeDate(sp,'last_shoot_from'),last_shoot_to:maybeDate(sp,'last_shoot_to'),first_shoot_from:maybeDate(sp,'first_shoot_from'),first_shoot_to:maybeDate(sp,'first_shoot_to'),birth_month:list(sp.get('birth_month')).map(Number),school_stage:list(sp.get('school_stage')),relation:list(sp.get('relation')),event_type:list(sp.get('event_type')),prefecture:list(sp.get('prefecture')),city:list(sp.get('city')),line:text(sp.get('line')),consent:text(sp.get('consent')),preferred_channel:list(sp.get('preferred_channel')),photo_public:text(sp.get('photo_public')),source:list(sp.get('source')),campaign:list(sp.get('campaign')),marketing_class:list(sp.get('marketing_class')),genre:list(sp.get('genre')),sort:text(sp.get('sort'))||'recommended',on_date:text(sp.get('as_of'))};
  for(const m of p.birth_month)if(!Number.isInteger(m)||m<1||m>12)throw new Error('invalid_birth_month');
  if(!CUSTOMER360_SORTS.includes(p.sort))throw new Error('invalid_sort');
  if(p.line&&!['linked','unlinked'].includes(p.line))throw new Error('invalid_line');
  if(p.photo_public&&!['ok','ng','unknown'].includes(p.photo_public))throw new Error('invalid_photo_public');
  if(p.consent&&!['unknown','opted_out','contactable_candidate'].includes(p.consent))throw new Error('invalid_consent');
  ensureOrder(p.ltv_min,p.ltv_max,'ltv');ensureOrder(p.aov_min,p.aov_max,'aov');ensureOrder(p.shoot_min,p.shoot_max,'shoot');ensureOrder(p.dormant_min,p.dormant_max,'dormant');ensureOrder(p.family_min,p.family_max,'family');ensureOrder(p.child_min,p.child_max,'child');ensureOrder(p.child_age_min,p.child_age_max,'child_age');
  if(p.last_shoot_from&&p.last_shoot_to&&p.last_shoot_from>p.last_shoot_to)throw new Error('invalid_last_shoot_range');
  if(p.first_shoot_from&&p.first_shoot_to&&p.first_shoot_from>p.first_shoot_to)throw new Error('invalid_first_shoot_range');
  return p;
}

export function customerMatchesSearch(view,p){
  const raw=view.raw||{},family=view.family||[];
  if(p.q){const q=norm(p.q),qPhone=phone(p.q);const searchable=[view.customer_id,view.name,raw.furigana,view.line_display_name,raw.phone,raw.email,view.address?.prefecture,view.address?.city,raw.genre_history,...family.flatMap(m=>[m.name,m.furigana])];const ordinary=searchable.some(v=>norm(v).includes(q));const phoneHit=qPhone.length>=3&&phone(raw.phone).includes(qPhone);if(!ordinary&&!phoneHit)return false}
  const dormant=Number(raw.dormant_days||0),children=family.filter(m=>m.relation==='child');
  if(!finiteRange(Number(view.realized_ltv||0),p.ltv_min,p.ltv_max)||!finiteRange(Number(view.avg_order_value||0),p.aov_min,p.aov_max)||!finiteRange(Number(view.shoot_count||0),p.shoot_min,p.shoot_max)||!finiteRange(dormant,p.dormant_min,p.dormant_max)||!finiteRange(family.length,p.family_min,p.family_max)||!finiteRange(children.length,p.child_min,p.child_max))return false;
  if(!inDateRange(view.last_shoot_date,p.last_shoot_from,p.last_shoot_to)||!inDateRange(view.first_shoot_date,p.first_shoot_from,p.first_shoot_to))return false;
  if(p.child_age_min!=null||p.child_age_max!=null){if(!children.some(m=>{const a=ageOn(m.birthdate,p.on_date);return a!=null&&finiteRange(a,p.child_age_min,p.child_age_max)}))return false}
  if(p.birth_month.length&&!children.some(m=>{const d=dateOnly(m.birthdate);return d&&p.birth_month.includes(Number(d.slice(5,7)))}))return false;
  if(p.school_stage.length&&!children.some(m=>p.school_stage.includes(text(m.school_stage))))return false;
  if(p.relation.length&&!family.some(m=>p.relation.includes(text(m.relation))))return false;
  if(p.event_type.length||p.event_days_max!=null){if(!(view.opportunities||[]).some(o=>(!p.event_type.length||p.event_type.some(t=>eventMatches(o,t)))&&(p.event_days_max==null||(o.days!=null&&o.days>=0&&o.days<=p.event_days_max))))return false}
  if(p.prefecture.length&&!p.prefecture.includes(text(view.address?.prefecture)))return false;if(p.city.length&&!p.city.includes(text(view.address?.city)))return false;
  if(p.line==='linked'&&!view.line_linked)return false;if(p.line==='unlinked'&&view.line_linked)return false;
  if(p.consent==='unknown'&&view.consent?.status!=='unknown')return false;if(p.consent==='opted_out'&&view.consent?.status!=='opted_out')return false;if(p.consent==='contactable_candidate'&&(view.consent?.status==='opted_out'||!(view.line_linked||text(raw.phone)||text(raw.email))))return false;
  if(p.preferred_channel.length&&!p.preferred_channel.includes(text(view.consent?.preferred_contact_channel)))return false;if(p.photo_public&&photoStatus(raw)!==p.photo_public)return false;
  const src=sourceValue(raw),camp=campaignValue(raw),genre=text(raw.genre_history);if(p.source.length&&!p.source.includes(src))return false;if(p.campaign.length&&!p.campaign.includes(camp))return false;if(p.genre.length&&!p.genre.every(x=>genre.includes(x)))return false;if(p.marketing_class.length&&!p.marketing_class.every(x=>(view.marketing_classes||[]).includes(x)))return false;
  return true;
}

function compareDate(a,b,dir=1){const A=dateOnly(a)||'',B=dateOnly(b)||'';if(A===B)return 0;if(!A)return 1;if(!B)return-1;return A<B?-1*dir:1*dir}
export function sortCustomerViews(views,sort='recommended'){const stable=(a,b)=>text(a.customer_id).localeCompare(text(b.customer_id),'ja');return[...views].sort((a,b)=>{let d=0;if(sort==='recommended')d=Number(b.recommendation?.priority_score||0)-Number(a.recommendation?.priority_score||0)||(Number(a.next_opportunity?.days??99999)-Number(b.next_opportunity?.days??99999));else if(sort==='event_soon')d=Number(a.next_opportunity?.days??99999)-Number(b.next_opportunity?.days??99999);else if(sort==='ltv_desc')d=Number(b.realized_ltv||0)-Number(a.realized_ltv||0);else if(sort==='ltv_asc')d=Number(a.realized_ltv||0)-Number(b.realized_ltv||0);else if(sort==='shoot_desc')d=Number(b.shoot_count||0)-Number(a.shoot_count||0);else if(sort==='last_shoot_desc')d=compareDate(a.last_shoot_date,b.last_shoot_date,-1);else if(sort==='last_shoot_asc')d=compareDate(a.last_shoot_date,b.last_shoot_date,1);else if(sort==='dormant_desc')d=Number(b.raw?.dormant_days||0)-Number(a.raw?.dormant_days||0);else if(sort==='name_asc')d=text(a.name).localeCompare(text(b.name),'ja');return d||stable(a,b)})}

export function listCustomerDto(view){const raw=view.raw||{},next=view.next_opportunity||null;return{customer_id:view.customer_id,name:view.name,furigana:text(raw.furigana),family_summary:view.family_summary,child_count:(view.family||[]).filter(x=>x.relation==='child').length,next_opportunity:next?{type:next.type,label:next.label,days:next.days,member_name:text(next.member_name),candidate:true}:null,realized_ltv:Number(view.realized_ltv||0),shoot_count:Number(view.shoot_count||0),last_shoot_date:view.last_shoot_date||'',area_summary:text(view.address?.summary),prefecture:text(view.address?.prefecture),city:text(view.address?.city),line_linked:!!view.line_linked,photo_public_status:photoStatus(raw),marketing_classes:[...(view.marketing_classes||[])],recommendation:{priority_score:Number(view.recommendation?.priority_score||0),next_offer:text(view.recommendation?.next_offer)},source_summary:sourceValue(raw),campaign_summary:campaignValue(raw)}}

export function buildFacets(views){const uniq=fn=>[...new Set(views.map(fn).flat().map(text).filter(Boolean))].sort((a,b)=>a.localeCompare(b,'ja'));return{prefectures:uniq(v=>v.address?.prefecture),cities:uniq(v=>v.address?.city),genres:uniq(v=>text(v.raw?.genre_history).split(',').map(x=>x.trim()).filter(Boolean)),sources:uniq(v=>sourceValue(v.raw||{})),campaigns:uniq(v=>campaignValue(v.raw||{})),school_stages:uniq(v=>(v.family||[]).filter(m=>m.relation==='child').map(m=>m.school_stage))}}
export function searchCustomerViews(views,p){const matched=(views||[]).filter(v=>customerMatchesSearch(v,p)),sorted=sortCustomerViews(matched,p.sort),start=(p.page-1)*p.page_size,items=sorted.slice(start,start+p.page_size);return{items:items.map(listCustomerDto),total:sorted.length,page:p.page,page_size:p.page_size,has_next:start+p.page_size<sorted.length}}
