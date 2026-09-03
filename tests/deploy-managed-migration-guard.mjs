import assert from 'node:assert/strict';

const approvedOrder=[
  '20260814_customer_line_message_events.sql',
  '20260818_customer_identity_registry.sql',
  '20260818_customer_identity_sequence.sql',
  '20260828_customer360_family_marketing_foundation.sql',
  '20260903_customer360_profile_auto_enrichment.sql'
];
const preexisting=new Set(approvedOrder.slice(0,3));
const family=approvedOrder[3],profile=approvedOrder[4];

export function classifyPending(files){
  const unique=[];for(const file of files)if(!unique.includes(file))unique.push(file);
  const unexpected=unique.filter(x=>!approvedOrder.includes(x));
  if(unexpected.length)return{ok:false,classification:'BLOCKED_UNEXPECTED_MANAGED_MIGRATION',files:unexpected};
  const old=unique.filter(x=>preexisting.has(x));
  if(old.length)return{ok:false,classification:'BLOCKED_PREEXISTING_MANAGED_MIGRATIONS_PENDING',files:old};
  if(unique.length===0)return{ok:true,classification:'REMOTE_APPLIED_STATE_CHECK_REQUIRED'};
  if(unique.length===1&&unique[0]===profile)return{ok:true,classification:'CUSTOMER360_PROFILE_MIGRATION_ONLY_PENDING'};
  if(unique.length===2&&unique[0]===family&&unique[1]===profile)return{ok:true,classification:'CUSTOMER360_MIGRATION_SEQUENCE_PENDING'};
  return{ok:false,classification:'BLOCKED_UNEXPECTED_MANAGED_MIGRATION_ORDER',files:unique};
}

assert.equal(classifyPending([profile]).classification,'CUSTOMER360_PROFILE_MIGRATION_ONLY_PENDING'); // A
assert.equal(classifyPending([family,profile]).classification,'CUSTOMER360_MIGRATION_SEQUENCE_PENDING'); // B
assert.equal(classifyPending([]).classification,'REMOTE_APPLIED_STATE_CHECK_REQUIRED'); // C
assert.equal(classifyPending(['20260814_customer_line_message_events.sql',family,profile]).classification,'BLOCKED_PREEXISTING_MANAGED_MIGRATIONS_PENDING'); // D
assert.equal(classifyPending([profile,family]).classification,'BLOCKED_UNEXPECTED_MANAGED_MIGRATION_ORDER'); // E
assert.equal(classifyPending(['20990101_unknown.sql']).classification,'BLOCKED_UNEXPECTED_MANAGED_MIGRATION'); // F
console.log('managed migration guard CASE A-F PASS');