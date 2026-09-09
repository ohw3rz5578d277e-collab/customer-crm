import assert from 'node:assert/strict';

const approvedOrder=[
  '20260814_customer_line_message_events.sql',
  '20260818_customer_identity_registry.sql',
  '20260818_customer_identity_sequence.sql',
  '20260828_customer360_family_marketing_foundation.sql',
  '20260903_customer360_profile_auto_enrichment.sql',
  '20260908_customer_media_delivery_links.sql'
];
const preexisting=new Set(approvedOrder.slice(0,3));
const family=approvedOrder[3],profile=approvedOrder[4],media=approvedOrder[5];
const managedSequence=[family,profile,media];

export function classifyPending(files){
  const unique=[];for(const file of files)if(!unique.includes(file))unique.push(file);
  const unexpected=unique.filter(x=>!approvedOrder.includes(x));
  if(unexpected.length)return{ok:false,classification:'BLOCKED_UNEXPECTED_MANAGED_MIGRATION',files:unexpected};
  const old=unique.filter(x=>preexisting.has(x));
  if(old.length)return{ok:false,classification:'BLOCKED_PREEXISTING_MANAGED_MIGRATIONS_PENDING',files:old};
  if(unique.length===0)return{ok:true,classification:'REMOTE_APPLIED_STATE_CHECK_REQUIRED'};
  const expected=managedSequence.slice(managedSequence.length-unique.length);
  const valid=unique.length<=managedSequence.length&&unique.every((file,index)=>file===expected[index]);
  if(!valid)return{ok:false,classification:'BLOCKED_UNEXPECTED_MANAGED_MIGRATION_ORDER',files:unique};
  if(unique.length===1)return{ok:true,classification:'CUSTOMER360_MEDIA_MIGRATION_ONLY_PENDING'};
  if(unique.length===2)return{ok:true,classification:'CUSTOMER360_PROFILE_AND_MEDIA_MIGRATIONS_PENDING'};
  return{ok:true,classification:'CUSTOMER360_MIGRATION_SEQUENCE_PENDING'};
}

assert.equal(classifyPending([media]).classification,'CUSTOMER360_MEDIA_MIGRATION_ONLY_PENDING'); // A
assert.equal(classifyPending([profile,media]).classification,'CUSTOMER360_PROFILE_AND_MEDIA_MIGRATIONS_PENDING'); // B
assert.equal(classifyPending([family,profile,media]).classification,'CUSTOMER360_MIGRATION_SEQUENCE_PENDING'); // C
assert.equal(classifyPending([]).classification,'REMOTE_APPLIED_STATE_CHECK_REQUIRED'); // D
assert.equal(classifyPending(['20260814_customer_line_message_events.sql',family,profile,media]).classification,'BLOCKED_PREEXISTING_MANAGED_MIGRATIONS_PENDING'); // E
assert.equal(classifyPending([profile]).classification,'BLOCKED_UNEXPECTED_MANAGED_MIGRATION_ORDER'); // F
assert.equal(classifyPending([media,profile]).classification,'BLOCKED_UNEXPECTED_MANAGED_MIGRATION_ORDER'); // G
assert.equal(classifyPending(['20990101_unknown.sql']).classification,'BLOCKED_UNEXPECTED_MANAGED_MIGRATION'); // H
console.log('managed migration guard CASE A-H PASS');
