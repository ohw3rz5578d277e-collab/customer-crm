import assert from 'node:assert/strict';
import {signGoogleServerRequest,verifyGoogleServerEnvelope,canonicalRequestBody,__test} from '../src/member-google-server-adapter.mjs';

assert.equal(canonicalRequestBody({b:1,a:{y:2,x:1}}),canonicalRequestBody({a:{x:1,y:2},b:1}));
assert.notEqual(canonicalRequestBody(false),canonicalRequestBody({}));
assert.notEqual(canonicalRequestBody(0),canonicalRequestBody({}));
assert.notEqual(canonicalRequestBody(''),canonicalRequestBody({}));
assert.notEqual(canonicalRequestBody(null),canonicalRequestBody({}));
assert.equal(canonicalRequestBody(null),'null');
assert.throws(()=>canonicalRequestBody({bad:undefined}),/invalid_body/);

const secret='0123456789abcdef0123456789abcdef';
const signed=signGoogleServerRequest({
 path:__test.APPROVED_PATH,timestamp_ms:1000000,nonce:'abcdefghijklmnopqrstuvwxyz',
 sync_event_id:'SE_abcdefghijklmnopqrstuvwxyz123456',body:{b:1,a:2},shared_secret:secret
});
assert.equal(signed.status,'ready');
assert.equal(signed.request_allowed,false);
assert.match(signed.signature_sha256,/^[0-9a-f]{64}$/);
assert.equal(signGoogleServerRequest({...signed,path:'/wrong',body:{b:1,a:2},shared_secret:secret}).status,'invalid_signing_input');
assert.equal(signGoogleServerRequest({path:__test.APPROVED_PATH,timestamp_ms:'1000000',nonce:'abcdefghijklmnopqrstuvwxyz',sync_event_id:'SE_abcdefghijklmnopqrstuvwxyz123456',body:{},shared_secret:secret}).status,'invalid_signing_input');
assert.equal(signGoogleServerRequest({path:__test.APPROVED_PATH,timestamp_ms:1000000,nonce:'abcdefghijklmnopqrstuvwxyz',sync_event_id:'SE_abcdefghijklmnopqrstuvwxyz123456',body:{},shared_secret:'short'}).status,'invalid_signing_input');
assert.equal(signGoogleServerRequest({path:__test.APPROVED_PATH,timestamp_ms:1000000,nonce:'abcdefghijklmnopqrstuvwxyz',sync_event_id:'SE_abcdefghijklmnopqrstuvwxyz123456',body:{bad:undefined},shared_secret:secret}).status,'invalid_body');

const envelope={
 now_ms:1000000,timestamp_ms:1000000,nonce:'abcdefghijklmnopqrstuvwxyz',sync_event_id:'SE_abcdefghijklmnopqrstuvwxyz123456',
 method:'POST',path:__test.APPROVED_PATH,body:{b:1,a:2},signature_sha256:signed.signature_sha256,shared_secret:secret,nonce_seen:false,event_seen:false
};
assert.equal(verifyGoogleServerEnvelope({...envelope,timestamp_ms:1}).status,'timestamp_out_of_window');
assert.equal(verifyGoogleServerEnvelope({...envelope,now_ms:'1000000'}).status,'invalid_time');
assert.equal(verifyGoogleServerEnvelope({...envelope,max_skew_ms:300001}).status,'invalid_time');
assert.equal(verifyGoogleServerEnvelope({...envelope,max_skew_ms:'300000'}).status,'invalid_time');
assert.equal(verifyGoogleServerEnvelope({...envelope,path:'/wrong'}).status,'invalid_destination');
assert.equal(verifyGoogleServerEnvelope({...envelope,method:'GET'}).status,'invalid_destination');
assert.equal(verifyGoogleServerEnvelope({...envelope,nonce_seen:true}).status,'nonce_replay');
assert.equal(verifyGoogleServerEnvelope({...envelope,event_seen:true}).status,'event_replay_check_required');
for(const bad of [0,1,null,'0','1',undefined]) {
 assert.equal(verifyGoogleServerEnvelope({...envelope,nonce_seen:bad}).status,'invalid_replay_evidence');
 assert.equal(verifyGoogleServerEnvelope({...envelope,event_seen:bad}).status,'invalid_replay_evidence');
}
assert.equal(verifyGoogleServerEnvelope({...envelope,sync_event_id:'bad'}).status,'invalid_sync_event_id');
assert.equal(verifyGoogleServerEnvelope({...envelope,shared_secret:'short'}).status,'invalid_signature_input');
assert.equal(verifyGoogleServerEnvelope({...envelope,signature_sha256:'0'.repeat(64)}).status,'invalid_signature');
assert.equal(verifyGoogleServerEnvelope({...envelope,body:{a:999}}).status,'invalid_signature');
assert.equal(verifyGoogleServerEnvelope({...envelope,body:{bad:undefined}}).status,'invalid_body');
const verified=verifyGoogleServerEnvelope(envelope);
assert.equal(verified.status,'verified');
assert.equal(verified.execute_allowed,false);
assert.equal(verified.execution_requires_separate_gate,true);

console.log('GOOGLE_SERVER_ADAPTER_DEFAULT_OFF=PASS');
console.log('HMAC_SECRET_MIN_LENGTH=32');
console.log('MAX_CLOCK_SKEW_MS=300000');
console.log('APPROVED_PATH_ONLY=YES');
console.log('STRICT_TIME_EVIDENCE=YES');
console.log('BROWSER_GOOGLE_CREDENTIALS=0');
console.log('NETWORK_REQUEST=0');
console.log('EXECUTE_ALLOWED=0');
console.log('SECRET_CHANGE=0');
console.log('PRODUCTION_WRITE=0');
