import fs from 'node:fs';
import { buildCustomerMarketingView, legacyFamilyMembers } from '../src/crm-customer360-marketing-engine.mjs';
import { parseCustomerSearchParams, searchCustomerViews, listCustomerDto, buildFacets, CUSTOMER360_SORTS } from '../src/crm-customer360-search.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}let n=0;const pass=(m,ok)=>{assert(ok,m);console.log(`PASS ${++n}: ${m}`)};const asOf='2026-08-28';
const rows=[
{customer_id:'26000101',name:'山田 花子',furigana:'ヤマダ ハナコ',line_display_name:'hanako',line_user_id:'U111111111111111111111',phone:'090-1234-5678',email:'hanako@example.jp',address:'大阪府豊中市本町1-2-3',child1_name:'さくら',child1_birthdate:'2019-10-29',first_shoot_date:'2020-02-01',last_shoot_date:'2025-09-01',repeat_count:3,total_revenue:128000,avg_order_value:42667,dormant_days:361,genre_history:'お宮参り,七五三',photo_public_ok:1,acquisition_source:'Instagram',campaign_name:'七五三秋'},
{customer_id:'26000102',name:'山田 花子',furigana:'ヤマダ ハナコ',line_display_name:'hanako2',line_user_id:'',phone:'08011112222',email:'',address:'大阪府吹田市江坂町1-1',child1_name:'りく',child1_birthdate:'2020-09-10',first_shoot_date:'2026-01-10',last_shoot_date:'2026-05-01',repeat_count:1,total_revenue:35000,avg_order_value:35000,dormant_days:119,genre_history:'お宮参り',photo_public_ok:0,acquisition_source:'紹介',campaign_name:'春紹介'},
{customer_id:'26000103',name:'佐藤 未来',furigana:'サトウ ミライ',line_display_name:'mirai',line_user_id:'U333333333333333333333',phone:'070-2222-3333',email:'mirai@example.jp',address:'兵庫県西宮市甲子園1-1',child1_name:'あかり',child1_birthdate:'2025-09-20',first_shoot_date:'2025-10-01',last_shoot_date:'2025-10-01',repeat_count:1,total_revenue:220000,avg_order_value:110000,dormant_days:331,genre_history:'ニューボーン,バースデー',photo_public_ok:null,acquisition_source:'Meta広告',campaign_name:'1歳秋'},
{customer_id:'26000104',name:'鈴木 太郎',furigana:'スズキ タロウ',line_display_name:'',line_user_id:'',phone:'',email:'taro@example.jp',address:'大阪府豊中市緑丘2-2',child1_name:'ひなた',child1_birthdate:'2006-10-29',first_shoot_date:'2022-03-01',last_shoot_date:'2024-01-01',repeat_count:5,total_revenue:250000,avg_order_value:50000,dormant_days:970,genre_history:'家族写真,成人',photo_public_ok:1,acquisition_source:'organic',campaign_name:'成人2026'},
{customer_id:'26000105',name:'高橋 葵',furigana:'タカハシ アオイ',line_display_name:'aoi',line_user_id:'U555555555555555555555',phone:'09099998888',email:'aoi@example.jp',address:'大阪府箕面市船場3-3',child1_name:'こころ',child1_birthdate:'2018-11-20',first_shoot_date:'2019-01-01',last_shoot_date:'2026-08-01',repeat_count:2,total_revenue:80000,avg_order_value:40000,dormant_days:27,genre_history:'七五三,家族写真',photo_public_ok:1,acquisition_source:'Instagram広告',campaign_name:'秋家族'}
];
const managed={
'26000101':[{id:'m1',customer_id:'26000101',relation:'spouse',name:'山田 太郎',furigana:'ヤマダ タロウ'},{id:'m2',customer_id:'26000101',relation:'child',name:'さくら',furigana:'ヤマダ サクラ',birthdate:'2019-10-29',school_stage:'小1'}],
'26000103':[{id:'m3',customer_id:'26000103',relation:'child',name:'あかり',furigana:'サトウ アカリ',birthdate:'2025-09-20'}]
};
const profiles={'26000101':{prefecture:'大阪府',city:'豊中市',address_line1:'本町1-2-3',marketing_opt_out:0,preferred_contact_channel:'LINE'},'26000103':{prefecture:'兵庫県',city:'西宮市',address_line1:'甲子園1-1',marketing_opt_out:1,preferred_contact_channel:'email'},'26000105':{marketing_contact_permission:'allowed',preferred_contact_channel:'LINE'}};
const views=rows.map(r=>buildCustomerMarketingView(r,managed[r.customer_id]||legacyFamilyMembers(r),profiles[r.customer_id]||{},asOf));
const run=query=>{const p=parseCustomerSearchParams(new URLSearchParams(query));p.on_date=asOf;return searchCustomerViews(views,p)};
pass('no query returns default list',run('').total===5);
pass('name partial search returns same-name customers',run('q=山田').total===2);
pass('same-name customers remain separate IDs',new Set(run('q=山田').items.map(x=>x.customer_id)).size===2);
pass('furigana search',run('q=サトウ').items[0]?.customer_id==='26000103');
pass('Customer ID exact search',run('q=26000104').items[0]?.customer_id==='26000104');
pass('Customer ID partial search',run('q=0105').items[0]?.customer_id==='26000105');
pass('phone normalized search hyphen to digits',run('q=09012345678').items[0]?.customer_id==='26000101');
pass('phone normalized search digits to partial',run('q=090-9999').items[0]?.customer_id==='26000105');
pass('family name search',run('q=太郎').items.some(x=>x.customer_id==='26000101'));
pass('family furigana search',run('q=サクラ').items.some(x=>x.customer_id==='26000101'));
pass('city search',run('q=豊中').total===2);
pass('zero result',run('q=存在しない顧客').total===0);
pass('LTV min/max',run('ltv_min=100000&ltv_max=230000').total===2);
pass('AOV min/max',run('aov_min=40000&aov_max=60000').total===3);
pass('shoot min/max',run('shoot_min=2&shoot_max=3').total===2);
pass('dormant range',run('dormant_min=300&dormant_max=400').total===2);
pass('last shoot date range',run('last_shoot_from=2026-01-01&last_shoot_to=2026-12-31').total===2);
pass('first shoot date range',run('first_shoot_from=2025-01-01&first_shoot_to=2026-12-31').total===2);
pass('family count range',run('family_min=2').items.some(x=>x.customer_id==='26000101'));
pass('child count range',run('child_min=1&child_max=1').total===5);
pass('child age range',run('child_age_min=6&child_age_max=7').items.some(x=>x.customer_id==='26000101'));
pass('birth month multi filter',run('birth_month=9,10').total>=3);
pass('school stage facet filter',run('school_stage=小1').items[0]?.customer_id==='26000101');
pass('relation exists',run('relation=spouse').items[0]?.customer_id==='26000101');
pass('birthday <=30',run('event_type=birthday&event_days_max=30').items.some(x=>x.customer_id==='26000102'));
pass('birthday <=60 excludes 62-day candidate',!run('event_type=birthday&event_days_max=60').items.some(x=>x.customer_id==='26000101'));
pass('birthday <=90 includes 62-day candidate',run('event_type=birthday&event_days_max=90').items.some(x=>x.customer_id==='26000101'));
pass('1-year event filter',run('event_type=first_birthday&event_days_max=90').items.some(x=>x.customer_id==='26000103'));
pass('shichigosan event filter',run('event_type=shichigosan').total>=1);
pass('coming-of-age event filter',run('event_type=adult').items.some(x=>x.customer_id==='26000104'));
pass('prefecture filter',run('prefecture=兵庫県').items[0]?.customer_id==='26000103');
pass('city multi filter',run('city=豊中市,箕面市').total===3);
pass('LINE linked filter',run('line=linked').total===3);
pass('LINE unlinked filter',run('line=unlinked').total===2);
pass('photo OK filter',run('photo_public=ok').total===3);
pass('photo NG filter',run('photo_public=ng').items[0]?.customer_id==='26000102');
pass('photo unknown filter',run('photo_public=unknown').items[0]?.customer_id==='26000103');
pass('source facet filter',run('source=Meta広告').items[0]?.customer_id==='26000103');
pass('campaign facet filter',run('campaign=七五三秋').items[0]?.customer_id==='26000101');
pass('genre multi filter',run('genre=七五三,家族写真').items[0]?.customer_id==='26000105');
pass('marketing class filter',run('marketing_class=LOYAL').total>=2);
pass('consent unknown filter',run('consent=unknown').total===3);
pass('marketing_opt_out false alone remains unknown',run('consent=unknown').items.some(x=>x.customer_id==='26000101'));
pass('consent opted out filter',run('consent=opted_out').items[0]?.customer_id==='26000103');
pass('explicit allowed contactable candidate',run('consent=contactable_candidate').items.some(x=>x.customer_id==='26000105'));
pass('contactable candidate excludes opt-out',!run('consent=contactable_candidate').items.some(x=>x.customer_id==='26000103'));
pass('combined AND filters',run('prefecture=大阪府&ltv_min=100000&line=linked').total===1&&run('prefecture=大阪府&ltv_min=100000&line=linked').items[0].customer_id==='26000101');
pass('filter clear represented by empty params',run('').total===5);
for(const sort of CUSTOMER360_SORTS){const a=run('sort='+sort).items.map(x=>x.customer_id),b=run('sort='+sort).items.map(x=>x.customer_id);pass('sort deterministic '+sort,JSON.stringify(a)===JSON.stringify(b)&&a.length===5)}
const paged=run('sort=name_asc&page=2&page_size=2');pass('pagination contract',paged.page===2&&paged.page_size===2&&paged.items.length===2&&paged.has_next===true&&paged.total===5);
const pLast=run('page=3&page_size=2');pass('pagination last page',pLast.items.length===1&&pLast.has_next===false);
for(const bad of ['ltv_min=10&ltv_max=1','shoot_min=-1','child_age_max=121','page_size=101','birth_month=13','sort=invalid','last_shoot_from=2026-99-99']){let threw=false;try{parseCustomerSearchParams(new URLSearchParams(bad))}catch{threw=true}pass('invalid filter fails safe '+bad,threw)}
const dto=listCustomerDto(views[0]),serialized=JSON.stringify(dto);for(const forbidden of ['address_line1','address_line2','birthdate','memo','line_history','raw','reservations'])pass('list DTO excludes '+forbidden,!serialized.includes('"'+forbidden+'"'));
pass('list DTO retains privacy-safe area summary',dto.area_summary==='大阪府 豊中市'&&!serialized.includes('本町1-2-3'));
pass('customer detail view remains authorized rich model',views[0].family.some(x=>x.birthdate==='2019-10-29')&&views[0].address.raw_address.includes('本町1-2-3'));
const facets=buildFacets(views);pass('facets include area genre source campaign school stage',facets.prefectures.includes('大阪府')&&facets.cities.includes('豊中市')&&facets.genres.includes('七五三')&&facets.sources.includes('Instagram')&&facets.campaigns.includes('七五三秋')&&facets.school_stages.includes('小1'));
const fullBytes=Buffer.byteLength(JSON.stringify(views)),listBytes=Buffer.byteLength(JSON.stringify(run('')));pass('privacy-safe list payload smaller than full raw views',listBytes<fullBytes);
const searchSrc=fs.readFileSync('src/crm-customer360-search.mjs','utf8'),runtimeSrc=fs.readFileSync('src/crm-customer360-runtime.mjs','utf8');pass('search module never imports identity resolver',!/identity-resolver|resolveCustomerIdentity|mergeCustomer/i.test(searchSrc));pass('search does not mutate customers',!/\b(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+customers\b/i.test(searchSrc+runtimeSrc));pass('runtime removed hard LIMIT 1000 list architecture',!runtimeSrc.includes('SELECT * FROM customers LIMIT 1000'));pass('marketing home no longer returns full customers array',!runtimeSrc.includes('customers:views')&&runtimeSrc.includes('top_opportunities'));
console.log(JSON.stringify({tests:n,full_raw_bytes:fullBytes,privacy_safe_list_bytes:listBytes,reduction_pct:Math.round((1-listBytes/fullBytes)*100)},null,2));
console.log(`CUSTOMER360_SEARCH_FILTER_UIX=${n}/${n} PASS`);
