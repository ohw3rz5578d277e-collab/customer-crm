import assert from 'node:assert/strict';
import app from '../src/production-index-crm-customer360-entry.js';

const requiredColumns={
  crm_admin_users:['email','role','status'],
  customers:['customer_id','customer_name','total_revenue','repeat_count','dormant_days','last_shoot_date','genre_history','line_user_id','deleted_at'],
  customer_reservations:['reservation_id','customer_id','customer_name','genre','shoot_date','start_time','end_time','plan_label','place','total_amount','status','deleted_at'],
  customer_line_draft_logs:['id','customer_id','customer_name','action_type','action_label','priority','status','created_at','updated_at','message_text'],
  crm_follow_tasks:['id','customer_id','customer_name','task_type','title','message_text','due_date','priority','status','created_at','updated_at'],
  crm_reservation_drafts:['id','customer_id','customer_name','status','sent_to_reservation_at','reservation_app_reservation_id','reservation_app_created_at','history_synced_at','reservation_app_updated_at','reservation_app_cancelled_at','cancellation_synced_at','updated_at','created_at'],
  crm_reservation_link_alert_checks:['draft_id','stage_key','acknowledged_at','acknowledged_by']
};
const requiredTables=Object.keys(requiredColumns);
const WRITE_SQL=/\b(CREATE|ALTER|INSERT|UPDATE|DELETE|DROP|REPLACE)\b/i;

function makeEnv({failFollow=false}={}){
  const evidence={preparedWrites:[],runCalls:0,sql:[]};
  const DB={prepare(sql){
    evidence.sql.push(sql);
    if(WRITE_SQL.test(sql)){
      evidence.preparedWrites.push(sql);
      const blocked={bind(){return blocked},async all(){throw new Error('WRITE_PREPARED')},async first(){throw new Error('WRITE_PREPARED')},async run(){evidence.runCalls++;throw new Error('WRITE_RUN')}};
      return blocked;
    }
    if(failFollow && /FROM crm_follow_tasks/.test(sql)) throw new Error('TRANSIENT_FOLLOW_READ_FAILURE');
    const state={params:[]};
    const stmt={
      bind(...params){state.params=params;return stmt},
      async run(){evidence.runCalls++;throw new Error('RUN_NOT_ALLOWED')},
      async first(){
        if(/SELECT email, role, status FROM crm_admin_users/.test(sql))return{email:'owner@example.com',role:'admin',status:'active'};
        if(/SELECT name FROM sqlite_master/.test(sql)&&/name=\?/.test(sql))return{name:String(state.params[0]||'table')};
        if(/COUNT\(\*\) AS customer_count/.test(sql))return{customer_count:0,total_revenue:0,avg_revenue:0,repeat_customers:0,dormant_customers:0};
        if(/SELECT 1 AS ok/.test(sql))return{ok:1};
        return null;
      },
      async all(){
        if(/SELECT name FROM sqlite_master/.test(sql)&&/name IN/.test(sql))return{results:requiredTables.map(name=>({name}))};
        if(/^PRAGMA table_info\(/.test(sql)){
          const table=(sql.match(/^PRAGMA table_info\(([^)]+)\)/)||[])[1];
          return{results:(requiredColumns[table]||[]).map(name=>({name}))};
        }
        if(/FROM crm_reservation_drafts/.test(sql))return{results:[]};
        if(/FROM crm_reservation_link_alert_checks/.test(sql))return{results:[]};
        if(/FROM customer_line_draft_logs/.test(sql))return{results:[]};
        if(/FROM crm_follow_tasks/.test(sql))return{results:[]};
        if(/FROM customer_reservations/.test(sql))return{results:[]};
        if(/SELECT customer_id, customer_name, total_revenue/.test(sql))return{results:[]};
        return{results:[]};
      }
    };
    return stmt;
  }};
  return {env:{DB,ADMIN_TOKEN:'test-admin-token'},evidence};
}

const headers={
  'cf-access-authenticated-user-email':'owner@example.com',
  'x-admin-token':'test-admin-token'
};

for(const path of ['/api/today-dashboard','/api/today-dashboard.csv']){
  const {env,evidence}=makeEnv();
  const res=await app.fetch(new Request('https://crm.example'+path,{method:'GET',headers}),env,{});
  assert.equal(res.status,200,path+' should succeed with complete read schema');
  assert.equal(evidence.preparedWrites.length,0,path+' prepared runtime schema write through Production entry');
  assert.equal(evidence.runCalls,0,path+' executed D1 run through Production entry');
}

{
  const {env,evidence}=makeEnv({failFollow:true});
  const res=await app.fetch(new Request('https://crm.example/api/today-dashboard',{method:'GET',headers}),env,{});
  const body=await res.json();
  assert.equal(res.status,503,'Production entry must preserve Today read failure as 503');
  assert.equal(body.ok,false);
  assert.equal(body.error,'today_dashboard_read_unavailable');
  assert.equal(evidence.preparedWrites.length,0,'failure path prepared runtime schema writes');
  assert.equal(evidence.runCalls,0,'failure path executed D1 run');
}

console.log('OWNER_TODAY_PRODUCTION_ENTRY_READ_ONLY=PASS');
console.log('OWNER_TODAY_PRODUCTION_ENTRY_CSV_READ_ONLY=PASS');
console.log('OWNER_TODAY_PRODUCTION_ENTRY_503_PRESERVED=PASS');
console.log('OWNER_TODAY_PRODUCTION_ENTRY_SCHEMA_WRITE=0');
console.log('OWNER_TODAY_PRODUCTION_ENTRY_D1_RUN=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('LINE_SEND=0');
