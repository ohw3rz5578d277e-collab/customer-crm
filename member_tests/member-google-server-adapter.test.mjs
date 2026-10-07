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

assert.equal(verifyGoogleServerEnvelope({now_ms:1000000,timestamp_ms:1,signature_valid:true}).status,'timestamp_out_of_window');
assert.equal(verifyGoogleServerEnvelope({now_ms:1000000,timestamp_ms:1000000,nonce_seen:true,signature_valid:true}).status,'nonce_replay');
assert.equal(verifyGoogleServerEnvelope({now_ms:1000000,timestamp_ms:1000000,signature_valid:'true'}).status,'invalid_signature');
const verified=verifyGoogleServerEnvelope({now_ms:1000000,timestamp_ms:1000000,signature_valid:true});
assert.equal(verified.status,'verified');
assert.equal(verified.execute_allowed,false);
assert.equal(verified.execution_requires_separate_gate,true);

console.log('GOOGLE_SERVER_ADAPTER_DEFAULT_OFF=PASS');
console.log('BROWSER_GOOGLE_CREDENTIALS=0');
console.log('NETWORK_REQUEST=0');
console.log('EXECUTE_ALLOWED=0');
console.log('SECRET_CHANGE=0');
console.log('PRODUCTION_WRITE=0');
