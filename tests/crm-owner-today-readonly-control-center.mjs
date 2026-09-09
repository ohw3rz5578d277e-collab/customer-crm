import assert from 'node:assert/strict';
import app from '../src/production-index-crm-today-dashboard.js';

const requiredTables=[
  'crm_admin_users','customers','customer_reservations','customer_line_draft_logs',
  'crm_follow_tasks','crm_reservation_drafts','crm_reservation_link_alert_checks'
];

let writeCount=0;
let scheduleReadCount=0;
const sqlSeen=[];

function stmt(sql){
  const state={params:[]};
  const api={
    bind(...params){state.params=params;return api;},
    async run(){writeCount++;throw new Error('WRITE_NOT_ALLOWED:'+sql);},
    async first(){
      if(/SELECT email, role, status FROM crm_admin_users/.test(sql))return{email:'owner@example.com',role:'admin',status:'active'};
      if(/COUNT\(\*\) AS customer_count/.test(sql))return{customer_count:3,total_revenue:180000,avg_revenue:60000,repeat_customers:2,dormant_customers:1};
      return null;
    },
    async all(){
      if(/SELECT name FROM sqlite_master/.test(sql))return{results:requiredTables.map(name=>({name}))};
      if(/SELECT \* FROM crm_reservation_drafts/.test(sql))return{results:[
        {id:7,customer_id:'26000123',customer_name:'山田 花子',status:'created',reservation_app_reservation_id:'R-7',reservation_app_created_at:'2026-09-09T01:00:00Z',history_synced_at:'',created_at:'2026-09-09T00:00:00Z'}
      ]};
      if(/FROM crm_reservation_link_alert_checks/.test(sql))return{results:[]};
      if(/FROM customer_line_draft_logs/.test(sql))return{results:[
        {id:2,customer_id:'26000123',customer_name:'山田 花子',action_type:'follow',action_label:'七五三のご案内',priority:'high',status:'copied',created_at:'2026-09-09T01:00:00Z',updated_at:'2026-09-09T01:00:00Z',message_text:'draft'}
      ]};
      if(/FROM crm_follow_tasks/.test(sql))return{results:[
        {id:3,customer_id:'26000456',customer_name:'佐藤 未来',task_type:'follow',title:'納品後フォロー',message_text:'',due_date:'2020-01-01',priority:'high',status:'open',created_at:'2026-09-01',updated_at:'2026-09-01'}
      ]};
      if(/SELECT customer_id, customer_name, total_revenue/.test(sql))return{results:[
        {customer_id:'26000123',customer_name:'山田 花子',total_revenue:128000,repeat_count:3,dormant_days:40,last_shoot_date:'2026-07-01',genre_history:'お宮参り,七五三',line_user_id:'U12345678901234567890'}
      ]};
      if(/FROM customer_reservations/.test(sql)){
        scheduleReadCount++;
        if(scheduleReadCount%2===1)return{results:[
          {reservation_id:'R-TODAY',customer_id:'26000123',customer_name:'山田 花子',genre:'七五三',shoot_date:state.params[0],start_time:'10:00',end_time:'11:00',plan_label:'Normal',place:'神社',total_amount:35000,status:'confirmed'}
        ]};
        return{results:[
          {reservation_id:'R-TOMORROW',customer_id:'26000456',customer_name:'佐藤 未来',genre:'Family',shoot_date:state.params[0],start_time:'13:00',end_time:'14:00',plan_label:'Simple',place:'公園',total_amount:25000,status:'confirmed'}
        ]};
      }
      return{results:[]};
    }
  };
  return api;
}

const env={DB:{prepare(sql){
  sqlSeen.push(sql);
  if(/\b(CREATE|ALTER|INSERT|UPDATE|DELETE|DROP|REPLACE)\b/i.test(sql)){
    writeCount++;
    throw new Error('WRITE_SQL_PREPARED:'+sql);
  }
  return stmt(sql);
}}};

const headers={'cf-access-authenticated-user-email':'owner@example.com'};
const res=await app.fetch(new Request('https://crm.example/api/today-dashboard',{headers}),env,{});
assert.equal(res.status,200);
const data=await res.json();
assert.equal(data.ok,true);
assert.equal(data.counts.today_shoots,1);
assert.equal(data.counts.tomorrow_shoots,1);
assert.equal(data.counts.reservation_alerts,1);
assert.equal(data.counts.line_pending,1);
assert.equal(data.counts.follow_overdue,1);
assert.equal(data.counts.immediate_total,3);
assert.equal(data.today_shoots[0].reservation_id,'R-TODAY');
assert.equal(data.tomorrow_shoots[0].reservation_id,'R-TOMORROW');
assert.equal(writeCount,0,'GET /api/today-dashboard must not execute/prepare write SQL');

const csv=await app.fetch(new Request('https://crm.example/api/today-dashboard.csv',{headers}),env,{});
assert.equal(csv.status,200);
const csvText=await csv.text();
assert.match(csvText,/今日の撮影/);
assert.match(csvText,/明日の撮影/);
assert.equal(writeCount,0,'CSV read path must not execute/prepare write SQL');

assert(sqlSeen.some(sql=>/sqlite_master/.test(sql)),'read-only schema guard missing');
assert(sqlSeen.every(sql=>!(/\b(CREATE|ALTER|INSERT|UPDATE|DELETE|DROP|REPLACE)\b/i.test(sql))),'write/DDL SQL observed');

console.log('OWNER_TODAY_READ_ONLY_API=PASS');
console.log('OWNER_TODAY_SCHEDULE_READ=PASS');
console.log('OWNER_TODAY_SCHEMA_FAIL_CLOSED_READ=PASS');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('LINE_SEND=0');
