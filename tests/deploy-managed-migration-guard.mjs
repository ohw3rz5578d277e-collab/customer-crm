import assert from 'node:assert/strict';

const approvedOrder=[
  '20260814_customer_line_message_events.sql',
  '20260818_customer_identity_registry.sql',
  '20260818_customer_identity_sequence.sql',
  '20260828_customer360_family_marketing_foundation.sql'
];
const preexisting=new Set(approvedOrder.slice(0,3));
const customer360=approvedOrder[3];

export function classifyPending(files){
  const unique=[];for(const file of files)if(!unique.includes(file))unique.push(file);
  const unexpected=unique.filter(x=>!approvedOrder.includes(x));
  if(unexpected.length)return{ok:false,classification:'BLOCKED_UNEXPECTED_MANAGED_MIGRATION',files:unexpected};
  const old=unique.filter(x=>preexisting.has(x));
  if(old.length)return{ok:false,classification:'BLOCKED_PREEXISTING_MANAGED_MIGRATIONS_PENDING',files:old};
  if(unique.length===0)return{ok:true,classification:'REMOTE_APPLIED_STATE_CHECK_REQUIRED'};
  if(unique.length===1&&unique[0]===customer360)return{ok:true,classification:'CUSTOMER360_MIGRATION_ONLY_PENDING'};
  return{ok:false,classification:'BLOCKED_UNEXPECTED_MANAGED_MIGRATION_ORDER',files:unique};
}

assert.equal(classifyPending([customer360]).classification,'CUSTOMER360_MIGRATION_ONLY_PENDING'); // A
assert.equal(classifyPending([]).classification,'REMOTE_APPLIED_STATE_CHECK_REQUIRED'); // B
assert.equal(classifyPending(['20260814_customer_line_message_events.sql',customer360]).classification,'BLOCKED_PREEXISTING_MANAGED_MIGRATIONS_PENDING'); // C
assert.equal(classifyPending(['20260818_customer_identity_registry.sql']).classification,'BLOCKED_PREEXISTING_MANAGED_MIGRATIONS_PENDING'); // D
assert.equal(classifyPending(['20990101_unknown.sql']).classification,'BLOCKED_UNEXPECTED_MANAGED_MIGRATION'); // E
console.log('managed migration guard CASE A-E PASS');
