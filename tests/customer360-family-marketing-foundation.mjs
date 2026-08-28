import fs from 'node:fs';
import {
  ageOn,nextBirthday,legacyFamilyMembers,mergeFamilyMembers,buildOpportunities,rfmScore,
  marketingClasses,buildCustomerMarketingView,filterMarketingViews
} from '../src/crm-customer360-marketing-engine.mjs';
import {handleCustomer360Request,customer360Health} from '../src/crm-customer360-runtime.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
let n=0;const pass=(label,ok)=>{assert(ok,label);console.log(`PASS ${++n}: ${label}`)};
const asOf='2026-08-28';
const customer={customer_id:'26000123',name:'山田 花子',line_user_id:'U123456789012345678901234',address:'大阪府豊中市本町1-2-3',child1_name:'さくら',child1_birthdate:'2019-10-29',child2_name:'あかり',child2_birthdate:'2025-02-01',child3_name:'みお',child3_birthdate:'2026-04-15',first_shoot_date:'2020-02-01',last_shoot_date:'2025-09-01',repeat_count:3,total_revenue:128000,avg_order_value:42667,dormant_days:361,genre_history:'お宮参り,1歳,七五三',photo_public_ok:1};

const legacy=legacyFamilyMembers(customer);
pass('legacy child1-child3 compatibility',legacy.length===3&&legacy.every(x=>x.source==='legacy'));
const unlimited=mergeFamilyMembers({...customer,child1_name:'',child1_birthdate:'',child2_name:'',child2_birthdate:'',child3_name:'',child3_birthdate:''},Array.from({length:15},(_,i)=>({id:`m${i}`,customer_id:'26000123',relation:'child',name:`child${i}`,birthdate:'2020-01-01'})));
pass('family member count is unlimited model',unlimited.length===15);
pass('age calculation JST date-only',ageOn('2019-10-29',asOf)===6);
const nb=nextBirthday('2019-10-29',asOf);pass('next birthday date and days',nb.date==='2026-10-29'&&nb.days===62&&nb.next_age===7);
const ops=buildOpportunities(customer,legacy,asOf);
pass('birthday opportunity',ops.some(x=>x.type==='birthday'&&x.member_name==='さくら'&&x.days===62));
pass('half birthday opportunity',ops.some(x=>x.type==='half_birthday'&&x.member_name==='みお'));
pass('first birthday opportunity',ops.some(x=>x.type==='first_birthday'&&x.member_name==='みお'));
pass('shichigosan candidate',ops.some(x=>x.type==='shichigosan'&&x.label.includes('7歳')&&x.days===62));
const schoolOps=buildOpportunities({},[{id:'school',customer_id:'26000123',relation:'child',name:'年長児',birthdate:'2020-10-29',school_stage:'年長'}],asOf);
pass('school event candidate uses school_stage',schoolOps.some(x=>x.type==='school_entry_candidate'&&x.source==='school_stage'));
const adultOps=buildOpportunities({},[{id:'adult',customer_id:'26000123',relation:'child',name:'成人候補',birthdate:'2006-10-29'}],asOf);
pass('coming of age candidate',adultOps.some(x=>x.type==='coming_of_age_candidate'));
const rfm=rfmScore(customer);pass('RFM scoring',rfm.R===3&&rfm.F===3&&rfm.M===4&&rfm.overall===3);
pass('high LTV derived opportunity',ops.some(x=>x.type==='high_ltv'));
pass('dormant derived opportunity',ops.some(x=>x.type==='dormant_180'));
const classes=marketingClasses(customer,ops,rfm);pass('multi marketing classes',classes.includes('LOYAL')&&classes.includes('DORMANT')&&classes.includes('EVENT_OPPORTUNITY'));
const view=buildCustomerMarketingView(customer,legacy,{},asOf);
pass('realized LTV equals total_revenue',view.realized_ltv===128000);
pass('address list summary is prefecture and city',view.address.summary==='大阪府 豊中市');
pass('existing customer rank is not overwritten',view.customer_rank==='');
pass('consent defaults unknown not opt-in',view.consent.status==='unknown');
pass('family birthdate does not change canonical customer id',view.customer_id==='26000123');
const sameName=mergeFamilyMembers(customer,[{id:'a',customer_id:'26000123',relation:'child',name:'同名',birthdate:'2020-01-01'},{id:'b',customer_id:'26000123',relation:'child',name:'同名',birthdate:'2021-01-01'}]);
pass('same-name family members remain profile rows only',sameName.filter(x=>x.name==='同名').length===2&&view.customer_id==='26000123');
const hot=buildCustomerMarketingView({...customer,customer_id:'26000124',total_revenue:250000,dormant_days:20},legacy,{},asOf);
const dormant=buildCustomerMarketingView({...customer,customer_id:'26000125',child1_name:'',child1_birthdate:'',child2_name:'',child2_birthdate:'',child3_name:'',child3_birthdate:'',total_revenue:30000,dormant_days:400,repeat_count:2},[],{},asOf);
pass('marketing priority puts near event high LTV ahead of dormant',hot.recommendation.priority_score>dormant.recommendation.priority_score);
pass('advanced filter foundation',filterMarketingViews([view,hot,dormant],{ltv_min:100000,event_days_max:90,on_date:asOf}).length===2);
pass('line output is draft only',customer360Health().marketing_line_auto_send===false);
pass('family identity source remains false',customer360Health().family_identity_source===false);

let writes=0;
const disabledEnv={CRM_LOCAL_TEST_AUTH:'1',DB:{prepare(){const stmt={bind(){return stmt},async first(){return {customer_id:'26000123'}},async all(){return {results:[]}},async run(){writes++;return {success:true}}};return stmt}}};
const disabled=await handleCustomer360Request(new Request('https://example.test/api/customer360/family',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({customer_id:'26000123',relation:'child',name:'x'})}),disabledEnv);
pass('family writes fail closed unless explicit local gate enabled',disabled.status===403&&writes===0);
const unauth=await handleCustomer360Request(new Request('https://example.test/api/customer360/marketing-home'),{DB:disabledEnv.DB});
pass('Customer 360 API requires authenticated admin context',unauth.status===401);

const migration=fs.readFileSync('migrations_managed/20260828_customer360_family_marketing_foundation.sql','utf8');
const runtime=fs.readFileSync('src/crm-customer360-runtime.mjs','utf8');
const engine=fs.readFileSync('src/crm-customer360-marketing-engine.mjs','utf8');
const wrangler=JSON.parse(fs.readFileSync('wrangler.jsonc','utf8'));
pass('family schema is separate from customers identity table',migration.includes('customer_family_members')&&!/ALTER\s+TABLE\s+customers/i.test(migration));
pass('marketing profile includes area and consent foundation',migration.includes('prefecture')&&migration.includes('city')&&migration.includes('marketing_opt_out')&&migration.includes('preferred_contact_channel'));
pass('no duplicate campaign or marketing score tables created',!migration.includes('crm_marketing_campaigns')&&!migration.includes('crm_marketing_scores'));
pass('runtime never writes customers identity rows',!/\b(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+customers\b/i.test(runtime));
pass('engine contains no family/name customer identity matching',!/(customer_id\s*=\s*.*name|name\s*=\s*.*customer_id)/i.test(engine));
pass('feature entry selected only on branch config',wrangler.main==='src/production-index-crm-customer360-entry.js');
console.log(`CUSTOMER360_FAMILY_MARKETING_FOUNDATION=${n}/${n} PASS`);
