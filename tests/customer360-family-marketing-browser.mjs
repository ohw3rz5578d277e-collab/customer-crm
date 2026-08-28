import http from 'node:http';
import { chromium } from 'playwright';
import { buildCustomerMarketingView, legacyFamilyMembers } from '../src/crm-customer360-marketing-engine.mjs';
import { injectCustomer360Marketing } from '../src/crm-customer360-ui.mjs';

function assert(ok,msg){if(!ok)throw new Error(msg)}
const asOf='2026-08-28';
const baseCustomers=[
  {customer_id:'26000123',name:'山田 花子',customer_rank:'A',line_user_id:'U123456789012345678901234',line_display_name:'hanako',address:'大阪府豊中市本町1-2-3',child1_name:'さくら',child1_birthdate:'2019-10-29',child2_name:'あかり',child2_birthdate:'2025-02-01',first_shoot_date:'2020-02-01',last_shoot_date:'2025-09-01',repeat_count:3,total_revenue:128000,avg_order_value:42667,dormant_days:361,genre_history:'お宮参り,1歳,七五三',photo_public_ok:1,acquisition_source:'Instagram',campaign_name:'七五三秋'},
  {customer_id:'26000124',name:'佐藤 未来',customer_rank:'B',line_user_id:'',address:'大阪府吹田市江坂町1-1',child1_name:'りく',child1_birthdate:'2026-04-15',first_shoot_date:'2026-05-01',last_shoot_date:'2026-05-01',repeat_count:1,total_revenue:35000,avg_order_value:35000,dormant_days:119,genre_history:'お宮参り',photo_public_ok:0,acquisition_source:'紹介',campaign_name:''}
];
const views=baseCustomers.map(c=>buildCustomerMarketingView(c,legacyFamilyMembers(c),{},asOf));
const details=new Map(views.map((v,i)=>[v.customer_id,{...v,reservations:i===0?[{shoot_date:'2025-09-01',genre:'七五三'},{shoot_date:'2024-02-01',genre:'1歳'}]:[{shoot_date:'2026-05-01',genre:'お宮参り'}],line_history:[],marketing_history:[{created_at:'2026-08-20',summary:'秋キャンペーン候補'}]}]));
const approach=[...views].sort((a,b)=>b.recommendation.priority_score-a.recommendation.priority_score);
const kpis={customers:2,average_realized_ltv:81500,repeat_rate_pct:50,vip_high_ltv:1,event_90d:1,dormant_180:1,line_link_rate_pct:50,approach_this_month:1};
let html='<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><style>html,body{margin:0;max-width:100%;overflow-x:hidden}body{font-family:-apple-system,BlinkMacSystemFont,"Noto Sans JP",sans-serif;background:#f4f7f8}.app{width:min(calc(100vw - 32px),1600px);margin:16px auto;box-sizing:border-box}</style></head><body><main class="app"><h1>CRM</h1></main></body></html>';
html=injectCustomer360Marketing(html);

async function send(res,status,data,type='application/json; charset=utf-8'){res.writeHead(status,{'content-type':type,'cache-control':'no-store'});res.end(type.startsWith('application/json')?JSON.stringify(data):data)}
const writes=[];
const server=http.createServer(async(req,res)=>{const u=new URL(req.url,'http://127.0.0.1');if(!['GET','HEAD'].includes(req.method))writes.push({method:req.method,path:u.pathname});if(u.pathname==='/'||u.pathname==='/admin')return send(res,200,html,'text/html; charset=utf-8');if(u.pathname==='/api/customer360/marketing-home')return send(res,200,{ok:true,as_of:asOf,kpis,approach,customers:views});if(u.pathname.startsWith('/api/customer360/customer/')){const id=decodeURIComponent(u.pathname.slice('/api/customer360/customer/'.length));const customer=details.get(id);return customer?send(res,200,{ok:true,customer}):send(res,404,{ok:false})}return send(res,404,{ok:false})});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin=`http://127.0.0.1:${server.address().port}`;
const browser=await chromium.launch({headless:true});
const viewports=[[1920,1080],[1440,900],[1024,768],[390,844]];
const results=[];
for(const [width,height] of viewports){
  const context=await browser.newContext({viewport:{width,height}});const page=await context.newPage();const pageErrors=[],consoleErrors=[];page.on('pageerror',e=>pageErrors.push(e.message));page.on('console',m=>{if(m.type()==='error')consoleErrors.push(m.text())});
  await page.goto(origin+'/admin',{waitUntil:'domcontentloaded'});await page.waitForFunction(()=>window.__crmCustomer360Marketing===1&&document.querySelector('#crmMktApproachBody tr'));
  assert(await page.getByText('今アプローチすべき顧客',{exact:true}).isVisible(),`${width}: marketing focus`);assert(await page.getByText('次の家族イベント',{exact:true}).isVisible(),`${width}: next family event`);
  assert(await page.locator('.crm-mkt-kpi').count()===8,`${width}: KPI count`);assert(await page.locator('#crmMktSegments .crm-mkt-seg').count()===17,`${width}: quick segments`);
  for(const id of ['crmFltLtv','crmFltShoot','crmFltDormant','crmFltEvent','crmFltChildAge','crmFltGenre','crmFltCity','crmFltLine','crmFltPhoto','crmFltSource','crmFltCampaign'])assert(await page.locator('#'+id).count()===1,`${width}: advanced ${id}`);
  const homeText=await page.locator('#crmMktHome').textContent();assert(!homeText.includes('2019-10-29'),`${width}: exact child birthdate hidden on list/home`);
  await page.locator('#crmMktNav [data-view="list"]').click();await page.waitForFunction(()=>document.querySelector('#crmMktList')?.classList.contains('open'));const listText=await page.locator('#crmMktList').textContent();assert(listText.includes('実績LTV')&&listText.includes('大阪府 豊中市')&&!listText.includes('大阪府豊中市本町1-2-3'),`${width}: list privacy and area summary`);
  await page.locator('#crmMktList [data-open="26000123"]').click();await page.waitForFunction(()=>document.querySelector('#crmMktDetail')?.classList.contains('open'));
  const detailText=await page.locator('#crmMktDetailBody').textContent();for(let i=1;i<=9;i++)assert(detailText.includes(`${i}.`),`${width}: Customer360 section ${i}`);assert(detailText.includes('2019-10-29'),`${width}: exact birthday visible only in Customer360`);assert(detailText.includes('実績LTV')&&detailText.includes('¥128,000'),`${width}: realized LTV`);assert(detailText.includes('文案のみ。自動送信は行いません。'),`${width}: LINE draft only`);assert(detailText.includes('7歳七五三候補'),`${width}: family opportunity`);
  const overflow=await page.evaluate(()=>({doc:[document.documentElement.scrollWidth,document.documentElement.clientWidth],body:[document.body.scrollWidth,document.body.clientWidth],detail:[document.querySelector('#crmMktDetail').scrollWidth,document.querySelector('#crmMktDetail').clientWidth]}));assert(overflow.doc[0]<=overflow.doc[1]&&overflow.body[0]<=overflow.body[1]&&overflow.detail[0]<=overflow.detail[1],`${width}: horizontal overflow 0 ${JSON.stringify(overflow)}`);assert(pageErrors.length===0,`${width}: page errors ${pageErrors.join('|')}`);assert(consoleErrors.length===0,`${width}: console errors ${consoleErrors.join('|')}`);
  results.push({viewport:`${width}x${height}`,horizontal_overflow:0,customer360:true,marketing_home:true,customer_list:true});await context.close();
}
assert(writes.length===0,`HTTP writes detected ${JSON.stringify(writes)}`);await browser.close();await new Promise(r=>server.close(r));console.log(JSON.stringify({results,http_writes:writes.length},null,2));console.log('CUSTOMER360_FAMILY_MARKETING_BROWSER=4/4 PASS');
