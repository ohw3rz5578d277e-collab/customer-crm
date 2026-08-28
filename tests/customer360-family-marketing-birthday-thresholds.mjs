import { nextBirthday, buildCustomerMarketingView, legacyFamilyMembers, filterMarketingViews } from '../src/crm-customer360-marketing-engine.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let passed=0;function pass(label,ok){assert(ok,label);passed++;console.log(`PASS ${passed}: ${label}`)}
const asOf='2026-08-28';
const make=(customer_id,birthdate)=>{
  const c={customer_id,name:`Customer ${customer_id}`,child1_name:'child',child1_birthdate:birthdate,repeat_count:1,total_revenue:50000,dormant_days:30};
  return buildCustomerMarketingView(c,legacyFamilyMembers(c),{},asOf);
};

const d29=nextBirthday('2020-09-26',asOf); // 29 days
const d59=nextBirthday('2020-10-26',asOf); // 59 days
const d89=nextBirthday('2020-11-25',asOf); // 89 days
pass('birthday <=30 boundary candidate',d29.days===29&&d29.days<=30);
pass('birthday 31-60 boundary candidate',d59.days===59&&d59.days>30&&d59.days<=60);
pass('birthday 61-90 boundary candidate',d89.days===89&&d89.days>60&&d89.days<=90);

const views=[make('26001001','2020-09-26'),make('26001002','2020-10-26'),make('26001003','2020-11-25')];
const within=(days)=>views.filter(v=>v.opportunities.some(o=>o.type==='birthday'&&o.days>=0&&o.days<=days));
pass('birthday 30 segment includes only <=30',within(30).map(v=>v.customer_id).join(',')==='26001001');
pass('birthday 60 segment includes <=30 and <=60',within(60).map(v=>v.customer_id).join(',')==='26001001,26001002');
pass('birthday 90 segment includes all <=90',within(90).map(v=>v.customer_id).join(',')==='26001001,26001002,26001003');
pass('advanced event-days filter agrees with 90-day opportunity',filterMarketingViews(views,{event_days_max:90,on_date:asOf}).length===3);
console.log(`CUSTOMER360_BIRTHDAY_THRESHOLDS=${passed}/${passed} PASS`);
