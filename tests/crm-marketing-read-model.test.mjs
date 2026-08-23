import assert from 'node:assert/strict';
import { buildCustomerMarketingModel, filterSortCustomers, isCompletedShoot } from '../src/crm-marketing-read-model.mjs';

const today = '2026-08-23';
const customers = [
  { customer_id:'26000001', name:'同姓 A', line_user_id:'U'+'a'.repeat(20) },
  { customer_id:'26000002', name:'同姓 A', line_user_id:'U'+'b'.repeat(20) },
  { customer_id:'26000003', name:'Prospect', line_user_id:'U'+'c'.repeat(20) },
  { customer_id:'26000004', name:'Future', line_user_id:'U'+'d'.repeat(20) },
  { customer_id:'26000005', name:'Missing' },
  { customer_id:'reservation-1', name:'Bad Identity', line_user_id:'bad-line' }
];
const reservations = [
  { customer_id:'26000001', reservation_id:'r1', shoot_date:'2025-01-01', genre:'お宮参り', total_amount:24800, status:'完了' },
  { customer_id:'26000001', reservation_id:'r2', shoot_date:'2025-07-01', genre:'ファミリー', total_amount:30000, status:'完了' },
  { customer_id:'26000001', reservation_id:'r3', shoot_date:'2026-09-10', genre:'七五三', total_amount:35000, status:'確定' },
  { customer_id:'26000001', reservation_id:'r4', shoot_date:'2025-02-01', genre:'取消', total_amount:999999, status:'キャンセル' },
  { customer_id:'26000002', reservation_id:'r5', shoot_date:'2025-01-01', genre:'お宮参り', total_amount:20000, status:'完了' },
  { customer_id:'26000002', reservation_id:'r6', shoot_date:'2025-06-01', genre:'ファミリー', total_amount:25000, status:'draft' },
  { customer_id:'26000004', reservation_id:'r7', shoot_date:'2026-10-01', genre:'バースデー', total_amount:26000, status:'確定' },
  { customer_id:'26000005', reservation_id:'r8', shoot_date:'2024-01-01', genre:'七五三', total_amount:35000, status:'完了' }
];

const model = buildCustomerMarketingModel({ customers, reservations }, { today });
const byId = id => model.customers.find(c => c.customer_id === id);

assert.equal(model.ok, true);
assert.equal(model.overview.customer_count, 6, 'Customer count is Customer ID row count, not same-name merge');
assert.equal(byId('26000001').shoot_count, 2, 'Customer value is per Customer ID');
assert.equal(byId('26000002').shoot_count, 1, 'same name / different Customer ID must not merge');
assert.equal(byId('26000001').ltv, 54800, 'LTV excludes cancelled and future reservations; no double count');
assert.equal(byId('26000001').reservation_count, 3, 'reservation_count may include valid future reservations but excludes cancelled');
assert.equal(isCompletedShoot({ shoot_date:'2026-10-01', status:'確定' }, today), false, 'future reservation is not a completed shoot');
assert.equal(isCompletedShoot({ shoot_date:'2025-01-01', status:'キャンセル' }, today), false, 'cancelled reservation is excluded from shoot count');
assert.equal(isCompletedShoot({ shoot_date:'2025-01-01', status:'draft' }, today), false, 'draft reservation is excluded from shoot count');
assert.equal(byId('26000003').segments.includes('PROSPECT'), true, 'LINE-only prospect is detected');
assert.equal(byId('26000002').segments.includes('NEW_CUSTOMER'), true, 'new customer is one completed shoot');
assert.equal(byId('26000001').segments.includes('REPEAT_CUSTOMER'), true, 'repeat customer is multiple completed shoots');
assert.equal(byId('26000005').segments.includes('DORMANT'), true, 'dormant customer is last shoot >=365 days and no future reservation');
assert.equal(byId('26000001').segments.includes('REPEAT_OPPORTUNITY'), false, 'future reservation suppresses duplicate recommendation');
assert.equal(byId('26000001').opportunity.type, 'UPCOMING_RESERVATION', 'future reservation creates upcoming reservation opportunity');
assert.equal(model.data_gaps.includes('campaign_open_click_response:未計測'), true, 'missing data is represented as 未計測');
assert.equal(byId('reservation-1').identity_review_required, true, 'identity review required is not marketing normal');
assert.equal(model.segments.IDENTITY_REVIEW_REQUIRED.count, 1, 'identity review required segment is counted');
assert.equal(filterSortCustomers(model.customers,{search:'同姓'}).length, 2, 'search finds both same-name customers without merging');
assert.equal(filterSortCustomers(model.customers,{segment:'PROSPECT'}).length, 1, 'segment filter works');
console.log('crm-marketing-read-model tests PASS');
