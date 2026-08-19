import http from 'node:http';
import { chromium } from 'playwright';
import { customerList, injectStableCustomerList } from '../src/production-index-crm-stable-customer-list.js';
import { customerDetail, injectDetailV2 } from '../src/production-index-crm-customer-list-detail-v2.js';

function assert(ok,message){if(!ok)throw new Error(message)}
let passed=0;
function pass(label,ok){assert(ok,label);passed+=1;console.log(`PASS ${passed}: ${label}`)}

const customers=Array.from({length:30},(_,i)=>{
  const n=i+1,id='26'+String(n).padStart(6,'0');
  return {id:n,customer_id:id,name:i<2?'同名顧客':`顧客 ${String(n).padStart(2,'0')}`,furigana:`こきゃく${n}`,line_display_name:i===0?'LINE A':i===1?'LINE B':'',line_user_id:i<2?`U${String(n).repeat(24).slice(0,24)}`:'',phone:`090${String(10000000+n)}`,email:`c${n}@example.test`,last_shoot_date:`2026-07-${String((n%28)+1).padStart(2,'0')}`,repeat_count:n%5,total_revenue:n*12000,customer_rank:n%2?'リピーター':'新規',memo:i===0?'A memo':i===1?'B memo':''};
});
const reservations=[
  {reservation_id:'R-A',customer_id:'26000001',customer_name:'同名顧客',shoot_date:'2026-08-01',genre:'お宮参り',total_amount:35000,status:'撮影済み'},
  {reservation_id:'R-B',customer_id:'26000002',customer_name:'同名顧客',shoot_date:'2026-07-01',genre:'七五三',total_amount:32000,status:'撮影済み'}
];
const lineLogs=[
  {id:1,customer_id:'26000001',customer_name:'同名顧客',message_text:'A LINE',created_at:'2026-08-02',direction:'out'},
  {id:2,customer_id:'26000002',customer_name:'同名顧客',message_text:'B LINE',created_at:'2026-07-02',direction:'in'}
];
const tasks=[
  {id:1,customer_id:'26000001',customer_name:'同名顧客',task_type:'follow',due_date:'2026-08-10'},
  {id:2,customer_id:'26000002',customer_name:'同名顧客',task_type:'follow',due_date:'2026-07-10'}
];
const dbWrites=[];
const env={DB:{prepare(sql){
  const state={params:[]};
  const stmt={bind(...params){state.params=params;return stmt},async all(){
    if(/^\s*SELECT name FROM sqlite_master/.test(sql))return{results:[{name:state.params[0]}]};
    if(/SELECT \* FROM customers LIMIT/.test(sql))return{results:customers};
    if(/SELECT \* FROM customers WHERE/.test(sql)){const id=String(state.params[0]||'');return{results:customers.filter(x=>String(x.customer_id)===id)}}
    if(/FROM customer_reservations/.test(sql))return{results:reservations};
    if(/FROM crm_reservation_drafts/.test(sql))return{results:[]};
    if(/FROM customer_line_draft_logs/.test(sql))return{results:lineLogs};
    if(/FROM crm_follow_tasks/.test(sql))return{results:tasks};
    return{results:[]};
  },async first(){return null},async run(){dbWrites.push(sql);throw new Error('write not allowed')}};
  return stmt;
}}};

let html='<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body><main>CRM fixture</main></body></html>';
html=injectStableCustomerList(html);
html=injectDetailV2(html);
const httpWrites=[];
async function sendWeb(res,response){res.writeHead(response.status,Object.fromEntries(response.headers));res.end(await response.text())}
const server=http.createServer(async(req,res)=>{
  const u=new URL(req.url,'http://127.0.0.1');
  if(req.method!=='GET'&&req.method!=='HEAD')httpWrites.push({method:req.method,path:u.pathname});
  if(u.pathname==='/'||u.pathname==='/admin'){res.writeHead(200,{'content-type':'text/html; charset=utf-8','cache-control':'no-store'});res.end(html);return}
  if(u.pathname==='/api/stable-customers'){await sendWeb(res,await customerList(env,u));return}
  if(u.pathname==='/api/stable-customer-detail'){await sendWeb(res,await customerDetail(env,u));return}
  res.writeHead(404);res.end('not found');
});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const origin=`http://127.0.0.1:${server.address().port}`;
const browser=await chromium.launch({headless:true});
const context=await browser.newContext({viewport:{width:412,height:915},screen:{width:412,height:915},isMobile:true,hasTouch:true,deviceScaleFactor:2.625});
const page=await context.newPage();
const pageErrors=[],consoleErrors=[];
page.on('pageerror',e=>pageErrors.push(e.message));
page.on('console',m=>{if(m.type()==='error')consoleErrors.push(m.text())});

await page.goto(origin+'/admin',{waitUntil:'domcontentloaded'});
await page.waitForFunction(()=>window.__crmStableCustomerList===1&&window.__crmCustomerDetailV2===1);
await page.evaluate(()=>window.__crmOpenStableCustomerList());
await page.waitForFunction(()=>document.querySelectorAll('.crm-stable-customer-card').length===30);
pass('CASE A customer tab/list opens',await page.locator('#crmStableCustomerPanel').isVisible());
pass('CASE B list renders customers',await page.locator('.crm-stable-customer-card').count()===30);
const firstText=(await page.locator('.crm-stable-customer-card').first().textContent()).replace(/\s+/g,' ');
pass('CASE B2 list prioritizes name with ID LINE count and cumulative sales',firstText.includes('同名顧客')&&firstText.includes('26000001')&&firstText.includes('LINE A')&&firstText.includes('撮影')&&firstText.includes('累計'));

const search=page.locator('#crmStableCustomerSearch');
await search.fill('26000002');
await page.waitForFunction(()=>document.querySelectorAll('.crm-stable-customer-card').length===1);
pass('CASE C search by customer_id',await page.locator('.crm-stable-customer-card').first().getAttribute('data-customer-id')==='26000002');
await search.fill('');
await page.waitForFunction(()=>document.querySelectorAll('.crm-stable-customer-card').length===30);

const listBox=page.locator('#crmStableCustomerList');
const scrollable=await listBox.evaluate(el=>({scrollHeight:el.scrollHeight,clientHeight:el.clientHeight}));
await listBox.evaluate(el=>{el.scrollTop=160});
pass('CASE K mobile vertical scroll',scrollable.scrollHeight>scrollable.clientHeight&&await listBox.evaluate(el=>el.scrollTop>0));
await listBox.evaluate(el=>{el.scrollTop=0});

await page.locator('.crm-stable-customer-card[data-customer-id="26000001"]').click();
await page.waitForFunction(()=>document.querySelector('#crmV2DetailPanel')?.classList.contains('open')&&/26000001/.test(document.querySelector('#crmV2DetailBody')?.textContent||''));
pass('CASE D customer opens',await page.locator('#crmV2DetailPanel').isVisible());
const bodyA=(await page.locator('#crmV2DetailBody').textContent()).replace(/\s+/g,' ');
pass('CASE E exact customer_id',bodyA.includes('26000001')&&!bodyA.includes('26000002'));
pass('CASE F reservation history exact',bodyA.includes('2026-08-01')&&bodyA.includes('お宮参り')&&!bodyA.includes('七五三'));
pass('CASE G cumulative sales',bodyA.includes('¥12,000'));
pass('CASE H LINE information',bodyA.includes('LINE A')&&bodyA.includes('A LINE')&&!bodyA.includes('B LINE'));
pass('CASE H2 memo/other visible',bodyA.includes('A memo'));

await page.locator('.crm-v2-detail-close').click();
await page.waitForFunction(()=>!document.querySelector('#crmV2DetailPanel')?.classList.contains('open'));
await page.evaluate(()=>window.__crmOpenStableCustomerList());
await page.waitForFunction(()=>document.querySelector('#crmStableCustomerPanel')?.classList.contains('open'));
pass('CASE I back to customer list',await page.locator('#crmStableCustomerPanel').isVisible());

await page.locator('.crm-stable-customer-card[data-customer-id="26000002"]').click();
await page.waitForFunction(()=>document.querySelector('#crmV2DetailPanel')?.classList.contains('open')&&/26000002/.test(document.querySelector('#crmV2DetailBody')?.textContent||''));
const bodyB=(await page.locator('#crmV2DetailBody').textContent()).replace(/\s+/g,' ');
pass('CASE J different customer never mixes same-name history',bodyB.includes('26000002')&&bodyB.includes('七五三')&&bodyB.includes('B LINE')&&!bodyB.includes('R-A')&&!bodyB.includes('A LINE')&&!bodyB.includes('お宮参り'));

const overflow=await page.evaluate(()=>({doc:[document.documentElement.scrollWidth,document.documentElement.clientWidth],detail:[document.querySelector('#crmV2DetailPanel').scrollWidth,document.querySelector('#crmV2DetailPanel').clientWidth]}));
pass('CASE L horizontal overflow absent',overflow.doc[0]<=overflow.doc[1]&&overflow.detail[0]<=overflow.detail[1]);

assert(httpWrites.length===0,`HTTP writes ${JSON.stringify(httpWrites)}`);
assert(dbWrites.length===0,`DB writes ${JSON.stringify(dbWrites)}`);
assert(pageErrors.length===0,`page errors ${pageErrors.join(' | ')}`);
assert(consoleErrors.length===0,`console errors ${consoleErrors.join(' | ')}`);
await browser.close();await new Promise(r=>server.close(r));
console.log(JSON.stringify({cases:passed,http_writes:httpWrites.length,db_writes:dbWrites.length,page_errors:pageErrors.length,console_errors:consoleErrors.length,viewport:'412x915'},null,2));
console.log(`CUSTOMER_MANAGEMENT_BROWSER=${passed}/${passed} PASS`);
