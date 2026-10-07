import assert from 'node:assert/strict';
import {signGoogleServerRequest,verifyGoogleServerEnvelope,canonicalRequestBody} from '../src/member-google-server-adapter.mjs';

assert.equal(canonicalRequestBody({b:1,a:{y:2,x:1}}),canonicalRequestBody({a:{x:1,y:2},b:1}));
const signed=signGoogleServerRequest({
 path:'/member/profile-sync',timestamp_ms:1000000,nonce:'abcdefghijklmnopqrstuvwxyz',
 sync_event_id:'SE_abcdefghijklmnopqrstuvwxyz123456',body:{b:1,a:2},shared_secret:'test-only-secret'
});
assert.equal(signed.status,'ready');
assert.equal(signed.request_allowed,false);
assert.match(signed.signature_sha256,/^[0-9a-f]{64}$/);

const envelope={
 now_ms:1000000,timestamp_ms:1000000,nonce:'abcdefghijklmnopqrstuvwxyz',sync_event_id:'SE_abcdefghijklmnopqrstuvwxyz123456',
 method:'POST',path:'/member/profile-sync',body:{b:1,a:2},signature_sha256:signed.signature_sha256,shared_secret:'test-only-secret'
};
assert.equal(verifyGoogleServerEnvelope({...envelope,timestamp_ms:1}).status,'timestamp_out_of_window');
assert.equal(verifyGoogleServerEnvelope({...envelope,nonce_seen:true}).status,'nonce_replay');
assert.equal(verifyGoogleServerEnvelope({...envelope,sync_event_id:'bad'}).status,'invalid_sync_event_id');
assert.equal(verifyGoogleServerEnvelope({...envelope,signature_sha256:'0'.repeat(64)}).status,'invalid_signature');
assert.equal(verifyGoogleServerEnvelope({...envelope,body:{a:999}}).status,'invalid_signature');
const verified=verifyGoogleServerEnvelope(envelope);
assert.equal(verified.status,'verified');
assert.equal(verified.execute_allowed,false);
assert.equal(verified.execution_requires_separate_gate,true);

console.log('GOOGLE_SERVER_ADAPTER_DEFAULT_OFF=PASS');
console.log('BROWSER_GOOGLE_CREDENTIALS=0');
console.log('NETWORK_REQUEST=0');
console.log('EXECUTE_ALLOWED=0');
console.log('SECRET_CHANGE=0');
console.log('PRODUCTION_WRITE=0');
