import { customerDetail } from '../src/production-index-crm-customer-list-detail-v2.js';

function assert(condition,message){if(!condition)throw new Error(message)}

const customer={id:1,customer_id:'26000001',name:'Order Sample',repeat_count:2,total_revenue:50000};
const persisted={reservation_id:'R-OLD',customer_id:'26000001',customer_name:'Order Sample',shoot_date:'2026-07-01',created_at:'2026-07-01T10:00:00Z',genre:'お宮参り'};
const newerDraft={reservation_id:'D-NEW',customer_id:'26000001',customer_name:'Order Sample',shoot_date:'2026-08-15',created_at:'2026-08-01T10:00:00Z',genre:'七五三'};

const env={DB:{prepare(sql){
  const state={params:[]};
  const stmt={
    bind(...params){state.params=params;return stmt},
    async all(){
      if(/^\s*SELECT name FROM sqlite_master/.test(sql))return{results:[{name:state.params[0]}]};
      if(/SELECT \* FROM customers WHERE/.test(sql))return{results:[customer]};
      if(/FROM customer_reservations/.test(sql))return{results:[persisted]};
      if(/FROM crm_reservation_drafts/.test(sql))return{results:[newerDraft]};
      if(/FROM customer_line_draft_logs/.test(sql)||/FROM crm_follow_tasks/.test(sql))return{results:[]};
      return{results:[]};
    },
    async first(){return null},
    async run(){throw new Error('write not allowed')}
  };
  return stmt;
}}};

const response=await customerDetail(env,new URL('https://local/api/stable-customer-detail?customer_id=26000001'));
const data=await response.json();
assert(response.status===200&&data.ok,'detail response');
assert(data.lookup_key==='customer_id'&&data.fallback_used===false,'identity contract');
assert(data.reservations.length===2,'combined history count');
assert(data.reservations[0].reservation_id==='D-NEW'&&data.reservations[0].source==='draft','newer draft must sort first');
assert(data.reservations[1].reservation_id==='R-OLD','older persisted reservation must sort second');
console.log('CUSTOMER_RESERVATION_HISTORY_ORDER=1/1 PASS');
