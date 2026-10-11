import fs from 'node:fs';
import assert from 'node:assert/strict';
import {
  buildMemberLineLoginStageCandidate,
  validateMemberLineLoginStageCandidate,
  memberProductionLineLoginStagePlanHealth
} from '../src/member-production-line-login-stage-plan.mjs';

let passCount=0;
function pass(name,condition=true){assert.equal(condition,true,name);passCount+=1;console.log('PASS',name);}
function throws(name,fn,pattern){assert.throws(fn,pattern,name);passCount+=1;console.log('PASS',name);}

const entry=fs.readFileSync(new URL('../src/production-index-crm-customer360-entry.js',import.meta.url),'utf8');
const cfg=JSON.parse(fs.readFileSync(new URL('../wrangler.jsonc',import.meta.url),'utf8'));

const candidate=buildMemberLineLoginStageCandidate(entry,cfg);
const validation=validateMemberLineLoginStageCandidate(candidate);

pass('planner returns a valid candidate',validation.ok===true);
pass('canonical source is not mutated in memory',entry.includes('const MEMBER_PRODUCTION_OWNER_APPROVED=false;'));
pass('candidate preserves canonical Worker entrypoint',candidate.wrangler_config.main==='src/production-index-crm-customer360-entry.js');
pass('candidate enables Owner approval exactly once',(candidate.entry_source.match(/const MEMBER_PRODUCTION_OWNER_APPROVED=true;/g)||[]).length===1);
pass('candidate enables LINE login approval exactly once',(candidate.entry_source.match(/line_login_approved:true/g)||[]).length===1);
pass('candidate enables Member route',candidate.wrangler_config.vars.MEMBER_PRODUCTION_ROUTE_MODE==='enabled');
pass('candidate enables LINE external exchange',candidate.wrangler_config.vars.MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE==='enabled');
pass('candidate keeps private media route disabled',candidate.wrangler_config.vars.MEMBER_PRIVATE_MEDIA_CONTENT_ROUTE_MODE==='disabled');
pass('candidate keeps favorites mutation route disabled',candidate.wrangler_config.vars.MEMBER_FAVORITES_MUTATION_ROUTE_MODE==='disabled');
pass('candidate keeps favorites rate limit mode disabled',candidate.wrangler_config.vars.MEMBER_FAVORITES_RATE_LIMIT_MODE==='disabled');
pass('candidate keeps favorites write disabled',candidate.wrangler_config.vars.MEMBER_FAVORITES_WRITE_MODE==='disabled');
pass('candidate keeps memory write disabled',candidate.wrangler_config.vars.MEMBER_MEMORY_WRITE_MODE==='disabled');
pass('candidate keeps family pass entitlement write disabled',candidate.wrangler_config.vars.MEMBER_FAMILY_PASS_ENTITLEMENT_WRITE_MODE==='disabled');
pass('candidate preserves exact private media R2 binding',candidate.wrangler_config.r2_buckets.length===1&&candidate.wrangler_config.r2_buckets[0].binding==='MEMBER_PRIVATE_MEDIA_BUCKET'&&candidate.wrangler_config.r2_buckets[0].bucket_name==='customer-crm-member-private-media');
pass('plan requires immutable zero-traffic candidate',candidate.plan.immutable_candidate_required===true&&candidate.plan.production_traffic_target_percent===0);
pass('plan requires existing Member session secret',candidate.plan.member_session_secret_required===true);
pass('plan cannot create Customer or Customer ID',candidate.plan.customer_create===false&&candidate.plan.customer_id_generation===false);
pass('plan cannot create or auto-link Family',candidate.plan.family_create===false&&candidate.plan.family_auto_link===false);
pass('plan cannot write CRM or send LINE',candidate.plan.crm_write===false&&candidate.plan.line_send===false);
pass('plan cannot activate commerce or BLACK award',candidate.plan.commerce_activation===false&&candidate.plan.black_automatic_award===false);

const health=memberProductionLineLoginStagePlanHealth();
pass('health is source-only and mutation-free',health.source_only===true&&health.production_mutation===false);
pass('health prohibits deploy and traffic change',health.production_deploy===false&&health.production_traffic_change===false);
pass('health prohibits Production D1 access',health.production_d1_read===false&&health.production_d1_write===false);
pass('health prohibits R2 object access',health.r2_object_read===false&&health.r2_object_write===false);
pass('health prohibits secret mutation',health.secret_change===false);
pass('health stops at fresh Owner-gated immutable candidate stage',health.next_boundary==='OWNER_GATED_IMMUTABLE_LINE_LOGIN_CANDIDATE_STAGE');

throws('planner rejects canonical Owner already enabled',()=>buildMemberLineLoginStageCandidate(entry.replace('const MEMBER_PRODUCTION_OWNER_APPROVED=false;','const MEMBER_PRODUCTION_OWNER_APPROVED=true;'),cfg),/OWNER_DEFAULT_FALSE_NOT_EXACTLY_ONE|OWNER_ALREADY_ENABLED/);
throws('planner rejects canonical LINE approval already enabled',()=>buildMemberLineLoginStageCandidate(entry.replace('line_login_approved:false','line_login_approved:true'),cfg),/LINE_LOGIN_APPROVAL_FALSE_NOT_EXACTLY_ONE|LINE_LOGIN_APPROVAL_ALREADY_ENABLED/);
const wrongMain=structuredClone(cfg);wrongMain.main='src/unpatched-entry.js';
throws('planner rejects canonical Worker entrypoint drift',()=>buildMemberLineLoginStageCandidate(entry,wrongMain),/CANONICAL_ENTRYPOINT_MISMATCH/);
const routeEnabled=structuredClone(cfg);routeEnabled.vars={...(routeEnabled.vars||{}),MEMBER_PRODUCTION_ROUTE_MODE:'enabled'};
throws('planner rejects canonical route mode already enabled',()=>buildMemberLineLoginStageCandidate(entry,routeEnabled),/CANONICAL_MODE_ALREADY_ENABLED:MEMBER_PRODUCTION_ROUTE_MODE/);
const exchangeEnabled=structuredClone(cfg);exchangeEnabled.vars={...(exchangeEnabled.vars||{}),MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE:'enabled'};
throws('planner rejects canonical LINE exchange already enabled',()=>buildMemberLineLoginStageCandidate(entry,exchangeEnabled),/CANONICAL_MODE_ALREADY_ENABLED:MEMBER_LINE_LOGIN_EXTERNAL_EXCHANGE_MODE/);
const extraR2=structuredClone(cfg);extraR2.r2_buckets=[...(extraR2.r2_buckets||[]),{binding:'EXTRA',bucket_name:'extra'}];
throws('planner rejects resource binding drift',()=>buildMemberLineLoginStageCandidate(entry,extraR2),/CANONICAL_PRIVATE_MEDIA_BINDING_MISMATCH/);
const invalidCandidate=structuredClone(candidate);invalidCandidate.wrangler_config.main='src/unpatched-entry.js';
throws('validator rejects candidate Worker entrypoint drift',()=>validateMemberLineLoginStageCandidate(invalidCandidate),/LINE_LOGIN_STAGE_CANDIDATE_INVALID:main/);

console.log(`MEMBER_PRODUCTION_LINE_LOGIN_STAGE_PLAN=${passCount}/${passCount} PASS`);
console.log('PRODUCTION_MUTATION=0');
console.log('PRODUCTION_TRAFFIC_CHANGE=0');
console.log('PRODUCTION_DEPLOY=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('R2_OBJECT_READ=0');
console.log('R2_OBJECT_WRITE=0');
console.log('SECRET_CHANGE=0');
console.log('LINE_SEND=0');
console.log('CUSTOMER_PROSPECT_MUTATION=0');
