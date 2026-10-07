import crypto from 'node:crypto';

const text=v=>v==null?'':String(v).trim();
const EVENT_RE=/^SE_[A-Za-z0-9_-]{22,}$/;

export function canonicalRequestBody(payload){
 const canonicalize=value=>{
   if(Array.isArray(value)) return value.map(canonicalize);
   if(value && typeof value==='object') return Object.fromEntries(Object.keys(value).sort().map(k=>[k,canonicalize(value[k])]));
   return value;
 };
 return JSON.stringify(canonicalize(payload===undefined?{}:payload));
}

export function signGoogleServerRequest({method='POST',path,timestamp_ms,nonce,sync_event_id,body,shared_secret}={}){
 const ts=Number(timestamp_ms);
 const n=text(nonce), event=text(sync_event_id), secret=text(shared_secret), p=text(path);
 if(!Number.isInteger(ts)||ts<=0||n.length<22||!EVENT_RE.test(event)||!secret||!p.startsWith('/')){
   return {status:'invalid_signing_input',request_allowed:false};
 }
 const bodyText=canonicalRequestBody(body);
 const bodyDigest=crypto.createHash('sha256').update(bodyText).digest('hex');
 const signing=[String(method).toUpperCase(),p,String(ts),n,event,bodyDigest].join('\n');
 const signature=crypto.createHmac('sha256',secret).update(signing).digest('hex');
 return {status:'ready',timestamp_ms:ts,nonce:n,sync_event_id:event,body_digest_sha256:bodyDigest,signature_sha256:signature,request_allowed:false};
}

export function verifyGoogleServerEnvelope({
 now_ms,timestamp_ms,nonce,nonce_seen=false,event_seen=false,sync_event_id,
 method='POST',path,body,signature_sha256,shared_secret,max_skew_ms=300000
}={}){
 const now=Number(now_ms), ts=Number(timestamp_ms), skew=Number(max_skew_ms);
 const n=text(nonce), event=text(sync_event_id), secret=text(shared_secret), p=text(path), supplied=text(signature_sha256);
 if(!Number.isInteger(now)||!Number.isInteger(ts)||!Number.isInteger(skew)||skew<1) return {status:'invalid_time',execute_allowed:false};
 if(Math.abs(now-ts)>skew) return {status:'timestamp_out_of_window',execute_allowed:false};
 if(n.length<22) return {status:'invalid_nonce',execute_allowed:false};
 if(!EVENT_RE.test(event)) return {status:'invalid_sync_event_id',execute_allowed:false};
 if(!secret||!p.startsWith('/')||!/^[0-9a-f]{64}$/.test(supplied)) return {status:'invalid_signature_input',execute_allowed:false};
 if(nonce_seen===true) return {status:'nonce_replay',execute_allowed:false};
 if(event_seen===true) return {status:'event_replay_check_required',execute_allowed:false};
 const bodyDigest=crypto.createHash('sha256').update(canonicalRequestBody(body)).digest('hex');
 const signing=[String(method).toUpperCase(),p,String(ts),n,event,bodyDigest].join('\n');
 const expected=crypto.createHmac('sha256',secret).update(signing).digest('hex');
 const valid=crypto.timingSafeEqual(Buffer.from(expected,'hex'),Buffer.from(supplied,'hex'));
 if(!valid) return {status:'invalid_signature',execute_allowed:false};
 return {status:'verified',execute_allowed:false,execution_requires_separate_gate:true};
}
