import assert from 'node:assert/strict';
import {planAcquisitionRegistration,planFunnelEvent as rawPlanFunnelEvent} from '../src/member-acquisition-attribution-foundation.mjs';
const m='MID_abcdefghijklmnopqrstuvwxyz123456',other='MID_zyxwvutsrqponmlkjihgfedcba654321',p='PID_abcdefghijklmnopqrstuvwxyz123456';
const planFunnelEvent=(args={})=>rawPlanFunnelEvent({
 member_prospect_binding_verified:Boolean(args.prospect_id),
 persisted_prospect_member_identity_id:args.member_identity_id,
 persisted_prospect_id:args.prospect_id,
 member_customer_binding_verified:Boolean(args.canonical_customer_id),
 persisted_customer_member_identity_id:args.member_identity_id,
 persisted_canonical_customer_id:args.canonical_customer_id,
 reservation_binding_verified:['reserved','shot','repeat'].includes(args.stage),
 persisted_reservation_id:args.reservation_id,
 persisted_reservation_member_identity_id:args.member_identity_id,
 persisted_reservation_prospect_id:args.prospect_id,
 persisted_reservation_customer_id:args.canonical_customer_id,
 ...args
});
let a=planAcquisitionRegistration({member_identity_id:m,prospect_id:p,source:'instagram',utm_campaign:'family-pass'});
assert.equal(a.status,'ready');assert.equal(a.source,'instagram');assert.equal(a.identity_authority,false);assert.equal(a.record_allowed,false);
a=planAcquisitionRegistration({member_identity_id:m,prospect_id:p,source:'evil'});
assert.equal(a.source,'unknown');
let e=planFunnelEvent({stage:'registered',member_identity_id:m,prospect_id:p,occurred_at:'2026-10-07T06:00:00Z'});
assert.equal(e.status,'ready');assert.equal(e.append_only,true);assert.equal(e.identity_authority,false);
assert.equal(planFunnelEvent({stage:'reserved',member_identity_id:m,canonical_customer_id:'12345678',occurred_at:'2026-10-07T06:00:00Z'}).status,'reservation_context_required');
assert.equal(planFunnelEvent({stage:'registered',member_identity_id:m,prospect_id:p,occurred_at:'2026-02-30T00:00:00Z'}).status,'invalid_time');
assert.equal(planFunnelEvent({stage:'registered',member_identity_id:m,prospect_id:p,occurred_at:'2026-04-31T09:00:00+09:00'}).status,'invalid_time');
assert.equal(planFunnelEvent({stage:'registered',member_identity_id:m,prospect_id:p,occurred_at:'2026-10-07T06:00:00Z',persisted_prospect_member_identity_id:other}).status,'member_prospect_binding_not_verified');
assert.equal(planFunnelEvent({stage:'inquiry',member_identity_id:m,canonical_customer_id:'12345678',occurred_at:'2026-10-07T06:00:00Z',persisted_customer_member_identity_id:other}).status,'member_customer_binding_not_verified');
e=planFunnelEvent({stage:'reserved',member_identity_id:m,canonical_customer_id:'12345678',reservation_id:'R-1',occurred_at:'2026-10-07T06:00:00Z'});
assert.equal(e.status,'ready');
assert.equal(planFunnelEvent({stage:'reserved',member_identity_id:m,canonical_customer_id:'12345678',reservation_id:'R-1',occurred_at:'2026-10-07T06:00:00Z',persisted_reservation_customer_id:'87654321'}).status,'reservation_binding_not_verified');
assert.equal(planFunnelEvent({stage:'shot',member_identity_id:m,prospect_id:p,reservation_id:'R-2',occurred_at:'2026-10-07T06:00:00Z',persisted_reservation_member_identity_id:other}).status,'reservation_binding_not_verified');
console.log('MEMBER_ACQUISITION_ATTRIBUTION_FOUNDATION=PASS');
console.log('ATTRIBUTION_IDENTITY_AUTHORITY=0');
console.log('PRODUCTION_ANALYTICS_WRITE=0');
