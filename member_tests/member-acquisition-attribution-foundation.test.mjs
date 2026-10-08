import assert from 'node:assert/strict';
import {planAcquisitionRegistration as rawPlanAcquisitionRegistration,planFunnelEvent as rawPlanFunnelEvent} from '../src/member-acquisition-attribution-foundation.mjs';
const m='MID_abcdefghijklmnopqrstuvwxyz123456',other='MID_zyxwvutsrqponmlkjihgfedcba654321',p='PID_abcdefghijklmnopqrstuvwxyz123456';
const planAcquisitionRegistration=(args={})=>rawPlanAcquisitionRegistration({
 member_prospect_binding_verified:true,
 persisted_member_identity_id:args.member_identity_id,
 persisted_prospect_id:args.prospect_id,
 ...args
});
const planFunnelEvent=(args={})=>{
 const reservationBacked=['reserved','shot','repeat'].includes(args.stage);
 return rawPlanFunnelEvent({
  idempotency_key:args.idempotency_key??`TEST:${args.stage??'unknown'}:${args.member_identity_id??''}:${args.prospect_id??''}:${args.canonical_customer_id??''}:${args.reservation_id??''}:${args.occurred_at??''}`,
  member_prospect_binding_verified:Boolean(args.prospect_id),
  persisted_prospect_member_identity_id:args.member_identity_id,
  persisted_prospect_id:args.prospect_id,
  member_customer_binding_verified:Boolean(args.canonical_customer_id),
  persisted_customer_member_identity_id:args.member_identity_id,
  persisted_canonical_customer_id:args.canonical_customer_id,
  reservation_binding_verified:reservationBacked,
  persisted_reservation_id:args.reservation_id,
  persisted_reservation_member_identity_id:args.member_identity_id,
  persisted_reservation_prospect_id:args.prospect_id,
  persisted_reservation_customer_id:args.canonical_customer_id,
  lifecycle_stage_binding_verified:reservationBacked,
  persisted_lifecycle_reservation_id:args.reservation_id,
  persisted_lifecycle_stage:args.stage,
  persisted_lifecycle_occurred_at:args.occurred_at,
  ...args
 });
};
let a=planAcquisitionRegistration({member_identity_id:m,prospect_id:p,source:'instagram',utm_campaign:'family-pass'});
assert.equal(a.status,'ready');assert.equal(a.source,'instagram');assert.equal(a.identity_authority,false);assert.equal(a.record_allowed,false);
a=planAcquisitionRegistration({member_identity_id:m,prospect_id:p,source:'evil'});
assert.equal(a.source,'unknown');
assert.equal(rawPlanAcquisitionRegistration({member_identity_id:m,prospect_id:p,source:'instagram'}).status,'member_prospect_binding_not_verified');
assert.equal(planAcquisitionRegistration({member_identity_id:m,prospect_id:p,source:'instagram',persisted_member_identity_id:other}).status,'member_prospect_binding_not_verified');
assert.equal(planAcquisitionRegistration({member_identity_id:m,prospect_id:p,source:'instagram',persisted_prospect_id:'PID_zyxwvutsrqponmlkjihgfedcba654321'}).status,'member_prospect_binding_not_verified');
let e=planFunnelEvent({stage:'registered',member_identity_id:m,prospect_id:p,occurred_at:'2026-10-07T06:00:00Z'});
assert.equal(e.status,'ready');assert.equal(e.append_only,true);assert.equal(e.identity_authority,false);
const retry=planFunnelEvent({stage:'registered',member_identity_id:m,prospect_id:p,occurred_at:'2026-10-07T06:00:00Z',idempotency_key:'registration-source-event-1'});
const retryAgain=planFunnelEvent({stage:'registered',member_identity_id:m,prospect_id:p,occurred_at:'2026-10-07T06:00:00Z',idempotency_key:'registration-source-event-1'});
assert.equal(retry.status,'ready');assert.equal(retryAgain.status,'ready');assert.equal(retry.event_id,retryAgain.event_id);
assert.equal(rawPlanFunnelEvent({stage:'registered',member_identity_id:m,occurred_at:'2026-10-07T06:00:00Z',idempotency_key:'orphan-event'}).status,'lifecycle_binding_required');
assert.equal(rawPlanFunnelEvent({stage:'registered',member_identity_id:m,prospect_id:p,occurred_at:'2026-10-07T06:00:00Z',member_prospect_binding_verified:true,persisted_prospect_member_identity_id:m,persisted_prospect_id:p}).status,'idempotency_key_required');
assert.equal(planFunnelEvent({stage:'reserved',member_identity_id:m,canonical_customer_id:'12345678',occurred_at:'2026-10-07T06:00:00Z'}).status,'reservation_context_required');
assert.equal(planFunnelEvent({stage:'registered',member_identity_id:m,prospect_id:p,occurred_at:'2026-02-30T00:00:00Z'}).status,'invalid_time');
assert.equal(planFunnelEvent({stage:'registered',member_identity_id:m,prospect_id:p,occurred_at:'2026-04-31T09:00:00+09:00'}).status,'invalid_time');
assert.equal(planFunnelEvent({stage:'registered',member_identity_id:m,prospect_id:p,occurred_at:'2026-10-07T06:00:00Z',persisted_prospect_member_identity_id:other}).status,'member_prospect_binding_not_verified');
assert.equal(planFunnelEvent({stage:'inquiry',member_identity_id:m,canonical_customer_id:'12345678',occurred_at:'2026-10-07T06:00:00Z',persisted_customer_member_identity_id:other}).status,'member_customer_binding_not_verified');
e=planFunnelEvent({stage:'reserved',member_identity_id:m,canonical_customer_id:'12345678',reservation_id:'R-1',occurred_at:'2026-10-07T06:00:00Z'});
assert.equal(e.status,'ready');
assert.equal(planFunnelEvent({stage:'reserved',member_identity_id:m,canonical_customer_id:'12345678',reservation_id:'R-1',occurred_at:'2026-10-07T06:00:00Z',persisted_reservation_customer_id:'87654321'}).status,'reservation_binding_not_verified');
assert.equal(planFunnelEvent({stage:'shot',member_identity_id:m,prospect_id:p,reservation_id:'R-2',occurred_at:'2026-10-07T06:00:00Z',persisted_reservation_member_identity_id:other}).status,'reservation_binding_not_verified');
assert.equal(planFunnelEvent({stage:'shot',member_identity_id:m,prospect_id:p,reservation_id:'R-2',occurred_at:'2026-10-07T06:00:00Z',persisted_lifecycle_stage:'reserved'}).status,'reservation_lifecycle_stage_not_verified');
assert.equal(planFunnelEvent({stage:'repeat',member_identity_id:m,canonical_customer_id:'12345678',reservation_id:'R-3',occurred_at:'2026-10-07T06:00:00Z',persisted_lifecycle_occurred_at:'2026-10-07T05:59:59Z'}).status,'reservation_lifecycle_stage_not_verified');
assert.equal(planFunnelEvent({stage:'shot',member_identity_id:m,prospect_id:p,reservation_id:'R-2',occurred_at:'2026-10-07T06:00:00Z',persisted_lifecycle_reservation_id:'R-OTHER'}).status,'reservation_lifecycle_stage_not_verified');
console.log('MEMBER_ACQUISITION_ATTRIBUTION_FOUNDATION=PASS');
console.log('ATTRIBUTION_IDENTITY_AUTHORITY=0');
console.log('PRODUCTION_ANALYTICS_WRITE=0');
