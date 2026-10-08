import crypto from 'node:crypto';

const EVENT_RE=/^SE_[A-Za-z0-9_-]{22,}$/;
const NONCE_RE=/^[A-Za-z0-9_-]{22,128}$/;
const SIGNATURE_RE=/^[0-9a-f]{64}$/;
const APPROVED_PATH='/member/profile-sync';
const MAX_SKEW_MS=300000;
const strictText=value=>typeof value==='string'&&value.trim()===value?value:null;

function canonicalize(value){
 if(value===null||typeof value==='string'||typeof value==='boolean') return value;
 if(typeof value==='number'){
  if(!Number.isFinite(value)) throw new Error('invalid_body');
  return value;
 }
 if(Array.isArray(value)) return value.map(canonicalize);
 if(value&&typeof value==='object'){
  const out={};
  for(const key of Object.keys(value).sort()){
   const item=value[key];
   if(item===undefined||typeof item==='function'||typeof item==='symbol'||typeof item==='bigint') throw new Error('invalid_body');
   out[key]=canonicalize(item);
  }
  return out;
 }
 throw new Error('invalid_body');
}

export function canonicalRequestBody(payload){
 return JSON.stringify(canonicalize(payload===undefined?{}:payload));
}

export function signGoogleServerRequest({method='POST',path,timestamp_ms,nonce,sync_event_id,body,shared_secret}={}){
 const p=strictText(path),n=strictText(nonce),event=strictText(sync_event_id);
 const secret=typeof shared_secret==='string'?shared_secret:null;
 if(method!=='POST'||p!==APPROVED_PATH||typeof timestamp_ms!=='number'||!Number.isSafeInteger(timestamp_ms)||timestamp_ms<=0||n===null||!NONCE_RE.test(n)||event===null||!EVENT_RE.test(event)||secret===null||secret.length<32){
  return {status:'invalid_signing_input',request_allowed:false};
 }
 let bodyText;
 try{ bodyText=canonicalRequestBody(body); }catch{ return {status:'invalid_body',request_allowed:false}; }
 const bodyDigest=crypto.createHash('sha256').update(bodyText).digest('hex');
 const signing=[method,p,String(timestamp_ms),n,event,bodyDigest].join('\n');
 const signature=crypto.createHmac('sha256',secret).update(signing).digest('hex');
 return {status:'ready',timestamp_ms,nonce:n,sync_event_id:event,body_digest_sha256:bodyDigest,signature_sha256:signature,request_allowed:false};
}

export function verifyGoogleServerEnvelope({now_ms,timestamp_ms,nonce,nonce_seen,event_seen,sync_event_id,method='POST',path,body,signature_sha256,shared_secret,max_skew_ms=MAX_SKEW_MS}={}){
 const p=strictText(path),n=strictText(nonce),event=strictText(sync_event_id),supplied=strictText(signature_sha256);
 const secret=typeof shared_secret==='string'?shared_secret:null;
 if(typeof now_ms!=='number'||!Number.isSafeInteger(now_ms)||now_ms<=0||typeof timestamp_ms!=='number'||!Number.isSafeInteger(timestamp_ms)||timestamp_ms<=0||typeof max_skew_ms!=='number'||!Number.isSafeInteger(max_skew_ms)||max_skew_ms<1||max_skew_ms>MAX_SKEW_MS){
  return {status:'invalid_time',execute_allowed:false};
 }
 if(method!=='POST'||p!==APPROVED_PATH) return {status:'invalid_destination',execute_allowed:false};
 if(Math.abs(now_ms-timestamp_ms)>max_skew_ms) return {status:'timestamp_out_of_window',execute_allowed:false};
 if(n===null||!NONCE_RE.test(n)) return {status:'invalid_nonce',execute_allowed:false};
 if(event===null||!EVENT_RE.test(event)) return {status:'invalid_sync_event_id',execute_allowed:false};
 if(secret===null||secret.length<32||supplied===null||!SIGNATURE_RE.test(supplied)) return {status:'invalid_signature_input',execute_allowed:false};
 if(typeof nonce_seen!=='boolean'||typeof event_seen!=='boolean') return {status:'invalid_replay_evidence',execute_allowed:false};
 if(nonce_seen===true) return {status:'nonce_replay',execute_allowed:false};
 if(event_seen===true) return {status:'event_replay_check_required',execute_allowed:false};
 let bodyDigest;
 try{ bodyDigest=crypto.createHash('sha256').update(canonicalRequestBody(body)).digest('hex'); }catch{ return {status:'invalid_body',execute_allowed:false}; }
 const signing=[method,p,String(timestamp_ms),n,event,bodyDigest].join('\n');
 const expected=crypto.createHmac('sha256',secret).update(signing).digest('hex');
 const valid=crypto.timingSafeEqual(Buffer.from(expected,'hex'),Buffer.from(supplied,'hex'));
 if(!valid) return {status:'invalid_signature',execute_allowed:false};
 return {status:'verified',execute_allowed:false,execution_requires_separate_gate:true};
}

export const __test={EVENT_RE,NONCE_RE,SIGNATURE_RE,APPROVED_PATH,MAX_SKEW_MS};
