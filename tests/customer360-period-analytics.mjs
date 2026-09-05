import assert from 'node:assert/strict';
import fs from 'node:fs';
import { parsePeriodAnalyticsParams, buildPeriodAnalytics } from '../src/crm-customer360-period-analytics.mjs';
import { isCompletedReservationStatus, isCancelledReservationStatus } from '../src/crm-reservation-status-contract.mjs';
import { handleCustomer360Request } from '../src/crm-customer360-runtime.mjs';

const rows=[
  {customer_id:'26000001',genre:'七五三',shoot_date:'2026-08-05',total_amount:10000,status:'撮影完了'},
  {customer_id:'26000001',genre:'ファミリー',shoot_date:'2026-08-20',total_amount:15000,status:'納品済み'},
  {customer_id:'26000002',genre:'お宮参り',shoot_date:'2026-08-10',total_amount:20000,status:'completed'},
  {customer_id:'26000003',genre:'七五三',shoot_date:'2026-08-12',total_amount:99999,status:'キャンセル'},
  {customer_id:'reservation-x',genre:'七五三',shoot_date:'2026-08-13',total_amount:50000,status:'撮影済み'},
  {customer_id:'26000001',genre:'ファミリー',shoot_date:'2026-07-05',total_amount:12000,status:'done'},
  {customer_id:'26000003',genre:'バースデー',shoot_date:'2026-07-22',total_amount:18000,status:'先行データ待ち'}
];

assert.equal(isCompletedReservationStatus('撮影完了'),true);
assert.equal(isCompletedReservationStatus('撮影終了'),true);
assert.equal(isCompletedReservationStatus('COMPLETED'),true);
assert.equal(isCancelledReservationStatus('キャンセル'),true);
assert.equal(isCompletedReservationStatus('予約確定'),false);

const period=parsePeriodAnalyticsParams(new URLSearchParams('from=2026-08-01&to=2026-08-31'),'2026-09-05');
assert.deepEqual(period.previous,{from:'2026-07-01',to:'2026-07-31'});
const out=buildPeriodAnalytics(rows,period);
assert.equal(out.current.revenue,45000);
assert.equal(out.current.completed_shoots,3);
assert.equal(out.current.unique_customers,2);
assert.equal(out.current.average_order_value,15000);
assert.equal(out.current.repeat_customers_in_period,1);
assert.equal(out.current.repeat_rate_pct,50);
assert.equal(out.previous.revenue,30000);
assert.equal(out.change_pct.revenue,50);
assert.equal(out.change_pct.completed_shoots,50);
assert.equal(out.change_pct.unique_customers,0);
assert.equal(out.current.genres[0].genre,'お宮参り');
assert.equal(out.meta.customer_id_generation,false);
assert.equal(out.meta.customer_write,false);
assert.equal(out.meta.line_send,false);

const endedOnly=buildPeriodAnalytics([
  {customer_id:'26009999',genre:'ポートレート',shoot_date:'2026-08-18',total_amount:19800,status:'撮影終了'}
],period);
assert.equal(endedOnly.current.revenue,19800,'撮影終了 revenue must be included');
assert.equal(endedOnly.current.completed_shoots,1,'撮影終了 shoot must be completed');
assert.equal(endedOnly.current.unique_customers,1,'撮影終了 customer must be counted');

const def=parsePeriodAnalyticsParams(new URLSearchParams(),'2026-09-05');
assert.equal(def.from,'2026-09-01');
assert.equal(def.to,'2026-09-05');
assert.throws(()=>parsePeriodAnalyticsParams(new URLSearchParams('from=2026-09-06&to=2026-09-01'),'2026-09-05'),/invalid_period_range/);
assert.throws(()=>parsePeriodAnalyticsParams(new URLSearchParams('from=2026-09-01&to=2026-09-06'),'2026-09-05'),/period_to_after_as_of/);

let reads=0,writes=0;
const env={
  CRM_LOCAL_TEST_AUTH:'1',
  DB:{
    prepare(sql){
      const stmt={
        bind(){return stmt},
        async first(){
          if(sql.includes("sqlite_master"))return {name:'customer_reservations'};
          return null;
        },
        async all(){
          reads++;
          if(sql.includes('FROM customer_reservations'))return {results:rows};
          return {results:[]};
        },
        async run(){writes++;return {success:true}}
      };
      return stmt;
    }
  }
};
const res=await handleCustomer360Request(new Request('https://example.test/api/customer360/analytics?from=2026-08-01&to=2026-08-31'),env);
assert.equal(res.status,200);
const body=await res.json();
assert.equal(body.ok,true);
assert.equal(body.current.revenue,45000);
assert.equal(body.meta.read_only,true);
assert.equal(body.meta.identity_key,'customer_id');
assert.equal(writes,0);
assert.ok(reads>=1);
const bad=await handleCustomer360Request(new Request('https://example.test/api/customer360/analytics?from=bad&to=2026-08-31'),env);
assert.equal(bad.status,400);

let failedWrites=0;
const failingAnalyticsEnv={
  CRM_LOCAL_TEST_AUTH:'1',
  DB:{
    prepare(sql){
      const stmt={
        bind(){return stmt},
        async first(){
          if(sql.includes('sqlite_master'))return{name:'customer_reservations'};
          return null;
        },
        async all(){
          if(sql.includes('FROM customer_reservations'))throw new Error('injected_analytics_read_failure');
          return{results:[]};
        },
        async run(){failedWrites++;return{success:true}}
      };
      return stmt;
    }
  }
};
const unavailable=await handleCustomer360Request(new Request('https://example.test/api/customer360/analytics?from=2026-08-01&to=2026-08-31'),failingAnalyticsEnv);
assert.equal(unavailable.status,503);
const unavailableBody=await unavailable.json();
assert.equal(unavailableBody.ok,false);
assert.equal(unavailableBody.error,'analytics_read_unavailable');
assert.ok(!('current' in unavailableBody),'failed analytics read must not return zeroed metrics');
assert.equal(failedWrites,0);

const runtime=fs.readFileSync('src/crm-customer360-runtime.mjs','utf8');
const profile=fs.readFileSync('src/crm-customer360-profile-enrichment.mjs','utf8');
assert.ok(runtime.includes("url.pathname==='/api/customer360/analytics'"));
assert.ok(runtime.includes("SELECT customer_id,genre,shoot_date,total_amount,status FROM customer_reservations"));
assert.ok(runtime.includes('period.previous.from,period.to'));
assert.ok(profile.includes("from './crm-reservation-status-contract.mjs'"));
assert.ok(!profile.includes('const COMPLETED_STATUSES=new Set'));
console.log('CUSTOMER360_PERIOD_ANALYTICS=PASS');
console.log('RESERVATION_SHOOT_ENDED_ANALYTICS=PASS');
console.log('PERIOD_ANALYTICS_READ_FAILURE_SURFACED=PASS');
console.log('PERIOD_ANALYTICS_PRODUCTION_WRITE=0');
console.log('PERIOD_ANALYTICS_CUSTOMER_ID_GENERATION=0');
console.log('PERIOD_ANALYTICS_LINE_SEND=0');
