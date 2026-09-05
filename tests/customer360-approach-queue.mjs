import assert from 'node:assert/strict';
import fs from 'node:fs';
import { approachContactState, parseApproachQueueParams, buildApproachQueue } from '../src/crm-customer360-approach-queue.mjs';
import { handleCustomer360Request } from '../src/crm-customer360-runtime.mjs';

const ready={
  customer_id:'26000001',name:'山田 花子',line_linked:true,realized_ltv:150000,shoot_count:3,last_shoot_date:'2026-06-01',
  next_opportunity:{type:'shichigosan',label:'七五三',days:20,member_name:'太郎'},
  recommendation:{priority_score:1080,priority_reason:'high_ltv+event',next_offer:'七五三 + 家族写真',next_line:'山田 花子様、七五三の時期が近づいてきました。'},
  marketing_classes:['VIP','EVENT_OPPORTUNITY'],consent:{marketing_opt_out:false,status:'explicit_not_opted_out',preferred_contact_channel:'LINE'},raw:{phone:'09011112222',email:'hanako@example.com'}
};
const review={
  customer_id:'26000002',name:'佐藤 一郎',line_linked:false,realized_ltv:30000,shoot_count:1,last_shoot_date:'2026-01-01',
  next_opportunity:null,recommendation:{priority_score:650,priority_reason:'first_shoot_no_repeat',next_offer:'家族写真 / リピート撮影',next_line:'佐藤 一郎様、次回撮影のご相談です。'},
  marketing_classes:['REPEAT_CANDIDATE'],consent:{marketing_opt_out:null,status:'unknown',preferred_contact_channel:'phone'},raw:{phone:'09033334444',email:''}
};
const blocked={
  customer_id:'26000003',name:'鈴木 次郎',line_linked:true,realized_ltv:120000,shoot_count:4,last_shoot_date:'2025-01-01',
  next_opportunity:null,recommendation:{priority_score:700,priority_reason:'high_ltv_dormant',next_offer:'家族写真',next_line:'鈴木 次郎様、ご無沙汰しています。'},
  marketing_classes:['AT_RISK'],consent:{marketing_opt_out:true,status:'opted_out',preferred_contact_channel:'LINE'},raw:{phone:'09055556666',email:'x@example.com'}
};
const future={...ready,customer_id:'26000004',name:'未来 顧客',next_opportunity:{type:'birthday',label:'誕生日',days:120},recommendation:{...ready.recommendation,priority_score:900,priority_reason:'event<=90'}};
const invalid={...ready,customer_id:'row:5',name:'ID未確定'};

assert.equal(approachContactState(ready).code,'manual_contact_ready');
assert.equal(approachContactState(ready).suggested_channel,'LINE');
assert.equal(approachContactState(review).code,'review_consent');
assert.equal(approachContactState(review).ready,false);
assert.equal(approachContactState(blocked).code,'blocked_opt_out');

const p=parseApproachQueueParams(new URLSearchParams('horizon_days=90&limit=10&status=all'));
assert.deepEqual(p,{horizon_days:90,limit:10,status:'all'});
assert.throws(()=>parseApproachQueueParams(new URLSearchParams('horizon_days=0')),/invalid_horizon_days/);
assert.throws(()=>parseApproachQueueParams(new URLSearchParams('limit=101')),/invalid_limit/);
assert.throws(()=>parseApproachQueueParams(new URLSearchParams('status=send')),/invalid_status/);

const q=buildApproachQueue([review,blocked,future,invalid,ready],p);
assert.equal(q.total,3);
assert.equal(q.items[0].customer_id,'26000001');
assert.equal(q.summary.ready,1);
assert.equal(q.summary.review_required,1);
assert.equal(q.summary.opted_out,1);
assert.equal(q.meta.line_send,false);
assert.equal(q.meta.automatic_contact,false);
assert.equal(q.meta.contact_details_exposed,false);
const serialized=JSON.stringify(q);
for(const secret of ['09011112222','hanako@example.com','09033334444','09055556666','x@example.com'])assert.ok(!serialized.includes(secret),'contact detail leaked '+secret);

assert.equal(buildApproachQueue([ready,review,blocked],{horizon_days:90,limit:50,status:'ready'}).items.length,1);
assert.equal(buildApproachQueue([ready,review,blocked],{horizon_days:90,limit:50,status:'review'}).items[0].customer_id,'26000002');
assert.equal(buildApproachQueue([ready,review,blocked],{horizon_days:90,limit:50,status:'blocked'}).items[0].customer_id,'26000003');

let writes=0;
const customerRow={customer_id:'26000011',name:'田中 三郎',line_user_id:'U123',phone:'09099990000',email:'tanaka@example.com',total_revenue:100000,repeat_count:2,dormant_days:0,first_shoot_date:'2026-01-01',last_shoot_date:'2026-06-01',deleted_at:''};
const env={CRM_LOCAL_TEST_AUTH:'1',DB:{prepare(sql){const stmt={bind(){return stmt},async first(){if(sql.includes('sqlite_master'))return null;return null},async all(){if(sql.includes('SELECT rowid AS __customer_ref,* FROM customers'))return{results:[customerRow]};return{results:[]}},async run(){writes++;return{success:true}}};return stmt}}};
const res=await handleCustomer360Request(new Request('https://example.test/api/customer360/approach-queue?horizon_days=90&limit=50&status=all'),env);
assert.equal(res.status,200);
const body=await res.json();
assert.equal(body.ok,true);
assert.equal(body.meta.read_only,true);
assert.equal(body.meta.line_send,false);
assert.equal(writes,0);
assert.ok(!JSON.stringify(body).includes('09099990000'));
assert.ok(!JSON.stringify(body).includes('tanaka@example.com'));

const runtime=fs.readFileSync('src/crm-customer360-runtime.mjs','utf8');
const ui=fs.readFileSync('src/crm-customer360-ui-client.mjs','utf8');
assert.ok(runtime.includes("url.pathname==='/api/customer360/approach-queue'"));
assert.ok(runtime.includes('buildApproachQueue(views,params)'));
assert.ok(runtime.includes("request.method==='GET'&&url.pathname==='/api/customer360/approach-queue'"));
const initStart=ui.indexOf('async function init()');
const initEnd=ui.indexOf('init();',initStart);
assert.ok(initStart>=0&&initEnd>initStart,'Customer360 init contract missing');
assert.ok(!ui.slice(initStart,initEnd).includes('loadApproachQueue'),'approach queue must remain lazy and never auto-fetch on init');
assert.ok(ui.includes("id=\"crmApproachLoad\""),'explicit approach queue load control missing');
console.log('CUSTOMER360_APPROACH_QUEUE=PASS');
console.log('APPROACH_QUEUE_PRODUCTION_WRITE=0');
console.log('APPROACH_QUEUE_CUSTOMER_ID_GENERATION=0');
console.log('APPROACH_QUEUE_LINE_SEND=0');
console.log('APPROACH_QUEUE_CONTACT_DETAILS_EXPOSED=0');
console.log('APPROACH_QUEUE_AUTO_FETCH=0');
console.log('APPROACH_QUEUE_AUTOMATIC_CONTACT=0');
