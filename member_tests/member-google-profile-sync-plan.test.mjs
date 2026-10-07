import assert from 'node:assert/strict';
import {createSyncEventId,planProfileSync,planSyncReplay} from '../src/member-google-profile-sync-plan.mjs';

const event=createSyncEventId();
const customer=planProfileSync({
 subject_type:'customer',customer_id:'12345678',member_identity_verified:true,
 profile_version:2,previous_profile_version:1,sync_event_id:event,profile:{name:'A',email:'a@example.test'}
});
assert.equal(customer.status,'ready');
assert.equal(customer.subject_id,'12345678');
assert.equal(customer.browser_direct_google_access,false);
assert.equal(customer.send_allowed,false);
assert.equal(planProfileSync({
 subject_type:'customer',customer_id:'12345678',member_identity_verified:'false',
 profile_version:2,previous_profile_version:1,sync_event_id:createSyncEventId(),profile:{name:'A'}
}).status,'identity_not_verified');

const prospect=planProfileSync({
 subject_type:'prospect',prospect_id:'PID_abcdefghijklmnopqrstuvwxyz123456',member_identity_verified:true,
 profile_version:1,previous_profile_version:0,sync_event_id:createSyncEventId(),profile:{name:'B'}
});
assert.equal(prospect.status,'ready');
assert.equal(prospect.subject_type,'prospect');
assert.equal(prospect.send_allowed,false);

assert.equal(planProfileSync({...customer,subject_type:'customer',customer_id:'12345678',member_identity_verified:false}).status,'identity_not_verified');

const nestedA=planProfileSync({
 subject_type:'customer',customer_id:'12345678',member_identity_verified:true,
 profile_version:2,previous_profile_version:1,sync_event_id:createSyncEventId(),
 profile:{address:{city:'Osaka',zip:'000'},tags:['a','b']}
});
const nestedB=planProfileSync({
 subject_type:'customer',customer_id:'12345678',member_identity_verified:true,
 profile_version:2,previous_profile_version:1,sync_event_id:createSyncEventId(),
 profile:{tags:['a','b'],address:{zip:'000',city:'Osaka'}}
});
assert.equal(nestedA.payload_digest_sha256,nestedB.payload_digest_sha256);

let replay=planSyncReplay({incoming_event_id:event,incoming_version:2,incoming_digest:'a',last_event_id:event,last_version:2,last_digest:'a'});
assert.equal(replay.status,'idempotent_replay');
assert.equal(replay.master_write,false);
assert.equal(replay.history_append,false);

replay=planSyncReplay({incoming_event_id:event,incoming_version:3,incoming_digest:'changed',last_event_id:event,last_version:2,last_digest:'a'});
assert.equal(replay.status,'event_id_integrity_conflict');
assert.equal(replay.review_required,true);

replay=planSyncReplay({incoming_event_id:'bad',incoming_version:3,incoming_digest:'b',last_event_id:event,last_version:2,last_digest:'a'});
assert.equal(replay.status,'invalid_sync_event_id');

replay=planSyncReplay({incoming_event_id:createSyncEventId(),incoming_version:2,incoming_digest:'b',last_event_id:event,last_version:2,last_digest:'a'});
assert.equal(replay.status,'version_digest_conflict');
assert.equal(replay.review_required,true);

replay=planSyncReplay({incoming_event_id:createSyncEventId(),incoming_version:3,incoming_digest:'b',last_event_id:event,last_version:2,last_digest:'a'});
assert.equal(replay.status,'accept_next_version');
assert.equal(replay.master_write,false);
assert.equal(replay.execution_requires_separate_gate,true);

console.log('GOOGLE_PROFILE_SYNC_PLANNER=PASS');
console.log('CUSTOMER_AND_PROSPECT_STREAMS=YES');
console.log('IDEMPOTENCY=REQUIRED');
console.log('HISTORY_APPEND=REQUIRED');
console.log('BROWSER_DIRECT_GOOGLE_ACCESS=0');
console.log('GOOGLE_SEND=0');
console.log('PRODUCTION_WRITE=0');
