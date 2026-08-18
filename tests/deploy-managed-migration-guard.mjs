import assert from 'node:assert/strict';

const approvedOrder=[
  '20260814_customer_line_message_events.sql',
  '20260818_customer_identity_registry.sql',
  '20260818_customer_identity_sequence.sql'
];

export function validatePending(files){
  const unique=[...new Set(files)];
  const unexpected=unique.filter(x=>!approvedOrder.includes(x));
  if(unexpected.length) return {ok:false,error:'unexpected',files:unexpected};
  const positions=unique.map(x=>approvedOrder.indexOf(x));
  for(let i=1;i<positions.length;i++) if(positions[i]<=positions[i-1]) return {ok:false,error:'order'};
  return {ok:true};
}

assert.equal(validatePending(['20260818_customer_identity_registry.sql','20260818_customer_identity_sequence.sql']).ok,true); // A
assert.equal(validatePending(['20260818_customer_identity_sequence.sql']).ok,true); // B
assert.equal(validatePending([]).ok,true); // C
assert.equal(validatePending(['20260818_customer_identity_registry.sql','20990101_unknown.sql']).ok,false); // D
assert.equal(validatePending(['20260818_customer_identity_sequence.sql','20260818_customer_identity_registry.sql']).ok,false); // E
console.log('managed migration guard CASE A-E PASS');
