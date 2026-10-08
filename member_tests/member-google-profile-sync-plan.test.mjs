import assert from 'node:assert/strict';
import {createSyncEventId,planProfileSync,planSyncReplay,profilePayloadDigest} from '../src/member-google-profile-sync-plan.mjs';

const event=createSyncEventId();
const customer=planProfileSync({
 subject_type:'customer',customer_id:'12345678',member_identity_verified:true,
 profile_version:2,previous_profile_version:1,sync_event_id:event,profile:{name:'A',email:'a@example.test'}
});
assert.equal(customer.status,'ready');
assert.equal(customer.subject_id,'12345678');
assert.equal(customer.browser_direct_google_access,false);
assert.equal(customer.send_allowed,false);

const prospect=planProfileSync({
 subject_type:'prospect',prospect_id:'PID_abcdefghijklmnopqrstuvwxyz123456',member_identity_verified:true,
 profile_version:1,previous_profile_version:0,sync_event_id:createSyncEventId(),profile:{name:'B'}
});
assert.equal(prospect.status,'ready');
assert.equal(prospect.subject_type,'prospect');
assert.equal(prospect.send_allowed,false);

assert.equal(planProfileSync({...customer,subject_type:'customer',customer_id:'12345678',member_identity_verified:false}).status,'identity_not_verified');
assert.equal(planProfileSync({...customer,subject_type:'customer',customer_id:'12345678',member_identity_verified:'false'}).status,'identity_not_verified');
assert.equal(planProfileSync({...customer,subject_type:'customer',customer_id:'12345678',member_identity_verified:'true'}).status,'identity_not_verified');
assert.equal(planProfileSync({
 subject_type:'customer',customer_id:'12345678',member_identity_verified:true,
 profile_version:'9007199254740993',previous_profile_version:'9007199254740992',
 sync_event_id:createSyncEventId(),profile:{name:'unsafe'}
}).status,'invalid_profile_version');

const nestedA={
 name:'A',
 address:{postal:'1000001',city:'Tokyo'},
 tags:[{b:2,a:1}]
};
const nestedB={
 tags:[{a:1,b:2}],
 address:{city:'Tokyo',postal:'1000001'},
 name:'A'
};
assert.equal(profilePayloadDigest(nestedA),profilePayloadDigest(nestedB));

const digestA=profilePayloadDigest({name:'A'});
const digestB=profilePayloadDigest({name:'B'});

let replay=planSyncReplay({incoming_event_id:createSyncEventId(),incoming_version:1,incoming_digest:digestA,last_event_id:'',last_version:0,last_digest:''});
assert.equal(replay.status,'accept_next_version');
assert.equal(replay.master_write,false);
assert.equal(replay.history_append,false);
assert.equal(replay.execution_requires_separate_gate,true);

for(const malformedLastVersion of [null,'',-1,-0.5,'9007199254740992']){
 replay=planSyncReplay({incoming_event_id:createSyncEventId(),incoming_version:1,incoming_digest:digestA,last_event_id:'',last_version:malformedLastVersion,last_digest:''});
 assert.equal(replay.status,'invalid_version_evidence');
 assert.equal(replay.review_required,true);
 assert.equal(replay.master_write,false);
 assert.equal(replay.history_append,false);
}

for(const malformedIncomingVersion of [null,'',0,0.5,-1,'9007199254740992','9007199254740993']){
 replay=planSyncReplay({incoming_event_id:createSyncEventId(),incoming_version:malformedIncomingVersion,incoming_digest:digestA,last_event_id:'',last_version:0,last_digest:''});
 assert.equal(replay.status,'invalid_version_evidence');
 assert.equal(replay.review_required,true);
 assert.equal(replay.master_write,false);
 assert.equal(replay.history_append,false);
}

for(const malformedIncomingEvent of [null,'','SE_short','not-an-event']){
 replay=planSyncReplay({incoming_event_id:malformedIncomingEvent,incoming_version:1,incoming_digest:digestA,last_event_id:'',last_version:0,last_digest:''});
 assert.equal(replay.status,'invalid_event_evidence');
 assert.equal(replay.review_required,true);
 assert.equal(replay.master_write,false);
 assert.equal(replay.history_append,false);
}

replay=planSyncReplay({incoming_event_id:createSyncEventId(),incoming_version:1,incoming_digest:digestA,last_event_id:createSyncEventId(),last_version:0,last_digest:''});
assert.equal(replay.status,'invalid_prior_state_evidence');
assert.equal(replay.review_required,true);
assert.equal(replay.master_write,false);
assert.equal(replay.history_append,false);

replay=planSyncReplay({incoming_event_id:createSyncEventId(),incoming_version:1,incoming_digest:digestA,last_event_id:'',last_version:0,last_digest:digestB});
assert.equal(replay.status,'invalid_prior_state_evidence');
assert.equal(replay.review_required,true);
assert.equal(replay.master_write,false);
assert.equal(replay.history_append,false);

replay=planSyncReplay({incoming_event_id:event,incoming_version:2,incoming_digest:digestA,last_event_id:event,last_version:2,last_digest:digestA});
assert.equal(replay.status,'idempotent_replay');
assert.equal(replay.master_write,false);
assert.equal(replay.history_append,false);

replay=planSyncReplay({incoming_event_id:event,incoming_version:2,incoming_digest:digestA.toUpperCase(),last_event_id:event,last_version:2,last_digest:digestA});
assert.equal(replay.status,'idempotent_replay');
assert.equal(replay.master_write,false);
assert.equal(replay.history_append,false);

replay=planSyncReplay({incoming_event_id:event,incoming_version:2,incoming_digest:digestA,last_event_id:event,last_version:2,last_digest:digestA.toUpperCase()});
assert.equal(replay.status,'idempotent_replay');
assert.equal(replay.master_write,false);
assert.equal(replay.history_append,false);

replay=planSyncReplay({incoming_event_id:event,incoming_version:2,incoming_digest:'',last_event_id:event,last_version:2,last_digest:''});
assert.equal(replay.status,'invalid_replay_evidence');
assert.equal(replay.review_required,true);
assert.equal(replay.master_write,false);
assert.equal(replay.history_append,false);

replay=planSyncReplay({incoming_event_id:event,incoming_version:2,incoming_digest:'not-a-sha256',last_event_id:event,last_version:2,last_digest:digestA});
assert.equal(replay.status,'invalid_replay_evidence');
assert.equal(replay.review_required,true);
assert.equal(replay.master_write,false);
assert.equal(replay.history_append,false);

replay=planSyncReplay({incoming_event_id:createSyncEventId(),incoming_version:2,incoming_digest:digestB,last_event_id:'',last_version:1,last_digest:''});
assert.equal(replay.status,'invalid_event_evidence');
assert.equal(replay.review_required,true);
assert.equal(replay.master_write,false);
assert.equal(replay.history_append,false);

replay=planSyncReplay({incoming_event_id:createSyncEventId(),incoming_version:2,incoming_digest:digestB,last_event_id:'not-an-event',last_version:1,last_digest:digestA});
assert.equal(replay.status,'invalid_event_evidence');
assert.equal(replay.review_required,true);
assert.equal(replay.master_write,false);
assert.equal(replay.history_append,false);

replay=planSyncReplay({incoming_event_id:event,incoming_version:2,incoming_digest:digestB,last_event_id:event,last_version:2,last_digest:digestA});
assert.equal(replay.status,'event_replay_conflict');
assert.equal(replay.review_required,true);
assert.equal(replay.master_write,false);
assert.equal(replay.history_append,false);

replay=planSyncReplay({incoming_event_id:event,incoming_version:3,incoming_digest:digestA,last_event_id:event,last_version:2,last_digest:digestA});
assert.equal(replay.status,'event_replay_conflict');
assert.equal(replay.review_required,true);
assert.equal(replay.master_write,false);
assert.equal(replay.history_append,false);

replay=planSyncReplay({incoming_event_id:createSyncEventId(),incoming_version:2,incoming_digest:digestB,last_event_id:event,last_version:2,last_digest:digestA});
assert.equal(replay.status,'version_digest_conflict');
assert.equal(replay.review_required,true);

replay=planSyncReplay({incoming_event_id:createSyncEventId(),incoming_version:3,incoming_digest:digestB,last_event_id:event,last_version:2,last_digest:digestA});
assert.equal(replay.status,'accept_next_version');
assert.equal(replay.master_write,false);
assert.equal(replay.execution_requires_separate_gate,true);

console.log('GOOGLE_PROFILE_SYNC_PLANNER=PASS');
console.log('CUSTOMER_AND_PROSPECT_STREAMS=YES');
console.log('STRICT_IDENTITY_EVIDENCE=YES');
console.log('NESTED_PROFILE_CANONICALIZATION=YES');
console.log('INITIAL_SYNC_WITHOUT_PRIOR_DIGEST=YES');
console.log('REPLAY_VERSION_EVIDENCE=SAFE_INTEGER_RANGE');
console.log('PROFILE_VERSION_EVIDENCE=SAFE_INTEGER_RANGE');
console.log('REPLAY_EVENT_EVIDENCE=REQUIRED');
console.log('INITIAL_PRIOR_STATE_EVIDENCE=EMPTY_ONLY');
console.log('REPLAY_DIGEST_EVIDENCE=REQUIRED_WHEN_PRIOR_EXISTS');
console.log('REPLAY_DIGEST_CASE=NORMALIZED');
console.log('EVENT_ID_CONFLICT_REVIEW=YES');
console.log('IDEMPOTENCY=REQUIRED');
console.log('HISTORY_APPEND=REQUIRED');
console.log('BROWSER_DIRECT_GOOGLE_ACCESS=0');
console.log('GOOGLE_SEND=0');
console.log('PRODUCTION_WRITE=0');
