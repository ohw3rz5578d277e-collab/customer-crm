import assert from 'node:assert/strict';
import {
 CLEAN_MEMBER_STACK_ORIGINS,
 CLEAN_MEMBER_STACK_ORIGINAL_CONSTRUCTION_BASE,
 CLEAN_MEMBER_STACK_SOURCE_CANDIDATE,
 CLEAN_MEMBER_STACK_BASE,
 validateCleanMemberStackOrigins
} from '../src/member-clean-stack-provenance.mjs';
assert.equal(CLEAN_MEMBER_STACK_ORIGINAL_CONSTRUCTION_BASE,'9d282bc2c1906379f33b7d0d8d28604c26cc3881');
assert.equal(CLEAN_MEMBER_STACK_SOURCE_CANDIDATE,'1de2de91d4378df134acc1bee0afadf78dcca671');
assert.equal(CLEAN_MEMBER_STACK_BASE,'90b83b8126b0851e3728d904b875347e19191f49');
const good=CLEAN_MEMBER_STACK_ORIGINS.map(([pr,head])=>({pr,head,review_threads_resolved:true}));
let r=validateCleanMemberStackOrigins(good);
assert.equal(r.status,'provenance_verified');
assert.equal(r.production_ready,false);
assert.equal(r.merge_allowed,false);
r=validateCleanMemberStackOrigins(good.map(x=>x.pr===210?{...x,head:'0'.repeat(40)}:x));
assert.equal(r.status,'blocked');
r=validateCleanMemberStackOrigins(good.map(x=>x.pr===211?{...x,review_threads_resolved:false}:x));
assert.equal(r.status,'blocked');
console.log('MEMBER_CLEAN_STACK_PROVENANCE=PASS');
console.log('ORIGINAL_BASE_PR_202_HEAD='+CLEAN_MEMBER_STACK_ORIGINAL_CONSTRUCTION_BASE);
console.log('SOURCE_CANDIDATE_PR_213_HEAD='+CLEAN_MEMBER_STACK_SOURCE_CANDIDATE);
console.log('CURRENT_MAIN_BASE='+CLEAN_MEMBER_STACK_BASE);
console.log('PRODUCTION_READY=0');
console.log('MERGE_ALLOWED=0');
