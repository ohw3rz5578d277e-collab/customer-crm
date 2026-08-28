import fs from 'node:fs';
import { customerList, injectStableCustomerList } from '../src/production-index-crm-stable-customer-list.js';
import { customerDetail, injectDetailV2 } from '../src/production-index-crm-customer-list-detail-v2.js';

function assert(condition,message){if(!condition)throw new Error(message)}
let passed=0;
async function test(name,fn){await fn();passed+=1;console.log(`PASS ${passed}: ${name}`)}

const customers=[
  {id:1,customer_id:'26000001',name:'同名顧客',furigana:'どうめいこきゃく',line_display_name:'LINE A',line_user_id:'Uaaaaaaaaaaaaaaaaaaaa',phone:'09011111111',email:'a@example.test',last_shoot_date:'2026-08-01',repeat_count:3,total_revenue:88000,customer_rank:'リピーター',memo:'A memo'},
  {id:2,customer_id:'26000002',name:'同名顧客',furigana:'どうめいこきゃく',line_display_name:'LINE B',line_user_id:'Ubbbbbbbbbbbbbbbbbbbb',phone:'09022222222',email:'b@example.test',last_shoot_date:'2026-07-01',repeat_count:1,total_revenue:32000,customer_rank:'新規',memo:'B memo'}
];
const reservationRows=[
  {reservation_id:'R-A',customer_id:'26000001',customer_name:'同名顧客',shoot_date:'2026-08-01',genre:'お宮参り',total_amount:35000,status:'撮影済み'},
  {reservation_id:'R-B',customer_id:'26000002',customer_name:'同名顧客',shoot_date:'2026-07-01',genre:'七五三',total_amount:32000,status:'撮影済み'}
];
const lineRows=[
  {id:1,customer_id:'26000001',customer_name:'同名顧客',message_text:'A LINE',created_at:'2026-08-02'},
  {id:2,customer_id:'26000002',customer_name:'同名顧客',message_text:'B LINE',created_at:'2026-07-02'}
];
const taskRows=[
  {id:1,customer_id:'26000001',customer_name:'同名顧客',task_type:'follow',due_date:'2026-08-10'},
  {id:2,customer_id:'26000002',customer_name:'同名顧客',task_type:'follow',due_date:'2026-07-10'}
];

function fakeEnv(){
  const sqls=[],writes=[];
  return {sqls,writes,env:{DB:{prepare(sql){
    const state={params:[]};sqls.push(sql);
    const stmt={bind(...params){state.params=params;return stmt},async all(){
      if(/^\s*SELECT name FROM sqlite_master/.test(sql))return{results:[{name:state.params[0]}]};
      if(/SELECT \* FROM customers LIMIT/.test(sql))return{results:customers};
      if(/SELECT \* FROM customers WHERE/.test(sql)){
        const id=String(state.params[0]||'');return{results:customers.filter(x=>String(x.customer_id)===id||(String(x.customer_id||'')===''&&String(x.id)===id))};
      }
      if(/FROM customer_reservations/.test(sql))return{results:reservationRows};
      if(/FROM crm_reservation_drafts/.test(sql))return{results:[]};
      if(/FROM customer_line_draft_logs/.test(sql))return{results:lineRows};
      if(/FROM crm_follow_tasks/.test(sql))return{results:taskRows};
      return{results:[]};
    },async first(){return null},async run(){writes.push(sql);throw new Error('write not allowed')}};
    return stmt;
  }}}};
}

await test('customer list exposes only existing operational facts including LINE state',async()=>{
  const f=fakeEnv();
  const res=await customerList(f.env,new URL('https://local/api/stable-customers?limit=1000'));
  const j=await res.json();
  assert(j.ok&&j.count===2,'list count');
  const a=j.customers.find(x=>x.customer_id==='26000001');
  assert(a&&a.name==='同名顧客'&&a.line_display_name==='LINE A'&&a.line_linked===true,'identity/LINE');
  assert(a.repeat_count===3&&a.total_revenue===88000&&a.last_shoot_date==='2026-08-01'&&a.customer_rank==='リピーター','operational facts');
  assert(f.writes.length===0,'list write');
});

await test('search supports customer_id without changing identity matching',async()=>{
  const f=fakeEnv();
  const res=await customerList(f.env,new URL('https://local/api/stable-customers?q=26000002'));
  const j=await res.json();
  assert(j.count===1&&j.customers[0].customer_id==='26000002','customer_id search');
});

await test('customer detail is exact customer_id and same-name reservations never mix',async()=>{
  const f=fakeEnv();
  const res=await customerDetail(f.env,new URL('https://local/api/stable-customer-detail?customer_id=26000001'));
  const j=await res.json();
  assert(res.status===200&&j.ok,'detail status');
  assert(j.lookup_key==='customer_id'&&j.fallback_used===false&&j.linkage_key==='customer_id','strict identity contract');
  assert(j.customer.customer_id==='26000001'&&j.customer.line_linked===true,'customer mismatch');
  assert(j.reservations.length===1&&j.reservations[0].reservation_id==='R-A','reservation mixed');
  assert(j.line_logs.length===1&&j.line_logs[0].message_text==='A LINE','LINE mixed');
  assert(j.follow_tasks.length===1&&j.follow_tasks[0].customer_id==='26000001','task mixed');
  assert(f.sqls.filter(x=>/FROM (customer_reservations|crm_reservation_drafts|customer_line_draft_logs|crm_follow_tasks)/.test(x)).every(x=>/CAST\(customer_id AS TEXT\)=\?/.test(x)&&!/(customer_name\s*=|CAST\(customer_name)/.test(x)),'name linkage SQL survived');
  assert(f.writes.length===0,'detail write');
});

await test('different customer_id returns different customer and history',async()=>{
  const f=fakeEnv();
  const res=await customerDetail(f.env,new URL('https://local/api/stable-customer-detail?customer_id=26000002'));
  const j=await res.json();
  assert(j.customer.customer_id==='26000002'&&j.customer.line_display_name==='LINE B','customer B mismatch');
  assert(j.reservations.length===1&&j.reservations[0].reservation_id==='R-B','B reservation mismatch');
  assert(!JSON.stringify(j).includes('R-A')&&!JSON.stringify(j.line_logs).includes('A LINE'),'A leaked into B');
});

await test('customer list UI prioritizes name then ID LINE count sales last use and rank',async()=>{
  const html=injectStableCustomerList('<!doctype html><html><head></head><body></body></html>');
  ['crm-stable-customer-name','crm-stable-customer-id','LINE 連携済み','撮影 ','累計 ','最終 ','ランク ','顧客名 / ID / LINEで検索'].forEach(x=>assert(html.includes(x),`list UI missing ${x}`));
  assert(html.includes('@media(max-width:767px)')&&html.includes('overflow:auto'),'mobile list/scroll');
});

await test('customer detail UI is summary-first and keeps LINE user id out of visible labels',async()=>{
  const html=injectDetailV2('<!doctype html><html><head></head><body></body></html>');
  ['顧客ID','顧客ランク','撮影回数','累計売上','最終利用','LINE / 連絡情報','予約履歴','LINE履歴','対応 / フォロー','メモ / その他'].forEach(x=>assert(html.includes(x),`detail UI missing ${x}`));
  assert(!html.includes('LINE User ID')&&!html.includes('line_user_id実値'),'LINE id surfaced');
  assert(html.includes('overflow-x:hidden')&&html.includes('@media(max-width:767px)'),'mobile detail overflow guard');
});

await test('production customer UI layer has no customer writes or LINE send',async()=>{
  const list=fs.readFileSync(new URL('../src/production-index-crm-stable-customer-list.js',import.meta.url),'utf8');
  const detail=fs.readFileSync(new URL('../src/production-index-crm-customer-list-detail-v2.js',import.meta.url),'utf8');
  for(const marker of ['INSERT INTO customers','UPDATE customers','DELETE FROM customers','LINE_SERVICE','pushMessage','replyMessage'])assert(!list.includes(marker)&&!detail.includes(marker),`forbidden marker ${marker}`);
});

await test('Reservation Workspace strict internal customer source remains customer_id only',async()=>{
  const internal=fs.readFileSync(new URL('../src/crm-internal-customer-detail.mjs',import.meta.url),'utf8');
  assert(internal.includes('internal_customer_detail_lookup_key:"customer_id"')&&internal.includes('internal_customer_detail_fallback:false'),'internal strict health');
  assert(internal.includes('WHERE CAST(customer_id AS TEXT)=?')&&!internal.includes('customer_name=?'),'internal reservation lookup not strict');
});

await test('wrangler production main is Customer360 and preserves browser-root UX chain',async()=>{
  const wrangler=JSON.parse(fs.readFileSync(new URL('../wrangler.jsonc',import.meta.url),'utf8'));
  const customer360=fs.readFileSync(new URL('../src/production-index-crm-customer360-entry.js',import.meta.url),'utf8');
  assert(wrangler.main==='src/production-index-crm-customer360-entry.js','wrangler main changed');
  assert(/import app from ['"]\.\/production-index-crm-browser-root-entry\.js['"]/.test(customer360),'Customer360 no longer preserves browser-root chain');
});

console.log(`CUSTOMER_MANAGEMENT_UX=${passed}/${passed} PASS`);
