import { isCompletedReservationStatus } from './crm-reservation-status-contract.mjs';

const CUSTOMER_ID_RE=/^\d{8}$/;
const text=v=>v==null?'':String(v).trim();
const dateOnly=v=>{const m=text(v).match(/^(\d{4}-\d{2}-\d{2})/);return m?m[1]:''};
const num=v=>{const n=Number(String(v??'').replace(/[,円¥\s]/g,''));return Number.isFinite(n)?Math.max(0,n):0};

function validDate(raw){
  const v=text(raw),m=v.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if(!m)return'';
  const y=Number(m[1]),mo=Number(m[2]),d=Number(m[3]),dt=new Date(Date.UTC(y,mo-1,d));
  return dt.getUTCFullYear()===y&&dt.getUTCMonth()+1===mo&&dt.getUTCDate()===d?v:'';
}
function addDays(date,days){
  const dt=new Date(date+'T00:00:00Z');dt.setUTCDate(dt.getUTCDate()+days);return dt.toISOString().slice(0,10);
}
function daysBetween(a,b){
  return Math.round((Date.parse(b+'T00:00:00Z')-Date.parse(a+'T00:00:00Z'))/86400000);
}
function monthStart(date){return date.slice(0,7)+'-01'}

export function parsePeriodAnalyticsParams(searchParams,onDate){
  const asOf=validDate(onDate);
  if(!asOf)throw new Error('invalid_as_of');
  const rawFrom=text(searchParams?.get?.('from')),rawTo=text(searchParams?.get?.('to'));
  const from=rawFrom?validDate(rawFrom):monthStart(asOf);
  const to=rawTo?validDate(rawTo):asOf;
  if(rawFrom&&!from)throw new Error('invalid_from');
  if(rawTo&&!to)throw new Error('invalid_to');
  if(from>to)throw new Error('invalid_period_range');
  if(to>asOf)throw new Error('period_to_after_as_of');
  const spanDays=daysBetween(from,to)+1;
  if(spanDays<1||spanDays>3660)throw new Error('period_too_large');
  const previousTo=addDays(from,-1);
  const previousFrom=addDays(previousTo,-(spanDays-1));
  return {from,to,as_of:asOf,span_days:spanDays,previous:{from:previousFrom,to:previousTo}};
}

function aggregate(rows,from,to){
  const completed=[];
  for(const row of rows||[]){
    const customerId=text(row.customer_id),shootDate=dateOnly(row.shoot_date);
    if(!CUSTOMER_ID_RE.test(customerId)||!shootDate||shootDate<from||shootDate>to||!isCompletedReservationStatus(row.status))continue;
    completed.push({...row,customer_id:customerId,shoot_date:shootDate,amount:num(row.total_amount),genre:text(row.genre)||'未設定'});
  }
  const customerCounts=new Map(),genres=new Map(),months=new Map();
  let revenue=0;
  for(const row of completed){
    revenue+=row.amount;
    customerCounts.set(row.customer_id,(customerCounts.get(row.customer_id)||0)+1);
    const g=genres.get(row.genre)||{genre:row.genre,shoots:0,revenue:0,customers:new Set()};
    g.shoots++;g.revenue+=row.amount;g.customers.add(row.customer_id);genres.set(row.genre,g);
    const month=row.shoot_date.slice(0,7),m=months.get(month)||{month,shoots:0,revenue:0,customers:new Set()};
    m.shoots++;m.revenue+=row.amount;m.customers.add(row.customer_id);months.set(month,m);
  }
  const uniqueCustomers=customerCounts.size,repeatCustomers=[...customerCounts.values()].filter(v=>v>=2).length;
  return {
    from,to,
    revenue:Math.round(revenue),
    completed_shoots:completed.length,
    unique_customers:uniqueCustomers,
    average_order_value:completed.length?Math.round(revenue/completed.length):0,
    repeat_customers_in_period:repeatCustomers,
    repeat_rate_pct:uniqueCustomers?Math.round(repeatCustomers/uniqueCustomers*1000)/10:0,
    genres:[...genres.values()].map(x=>({genre:x.genre,shoots:x.shoots,revenue:Math.round(x.revenue),unique_customers:x.customers.size})).sort((a,b)=>b.revenue-a.revenue||b.shoots-a.shoots||a.genre.localeCompare(b.genre,'ja')),
    monthly:[...months.values()].map(x=>({month:x.month,shoots:x.shoots,revenue:Math.round(x.revenue),unique_customers:x.customers.size})).sort((a,b)=>a.month.localeCompare(b.month))
  };
}

function change(current,previous,key){
  const c=Number(current[key]||0),p=Number(previous[key]||0);
  return p===0?(c===0?0:null):Math.round((c-p)/p*1000)/10;
}

export function buildPeriodAnalytics(rows,period){
  const current=aggregate(rows,period.from,period.to),previous=aggregate(rows,period.previous.from,period.previous.to);
  return {
    period,
    current,
    previous,
    change_pct:{
      revenue:change(current,previous,'revenue'),
      completed_shoots:change(current,previous,'completed_shoots'),
      unique_customers:change(current,previous,'unique_customers'),
      average_order_value:change(current,previous,'average_order_value')
    },
    meta:{
      read_only:true,
      identity_key:'customer_id',
      customer_id_generation:false,
      customer_write:false,
      line_send:false
    }
  };
}
