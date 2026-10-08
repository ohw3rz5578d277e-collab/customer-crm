import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import path from 'node:path';
import {fileURLToPath} from 'node:url';

const NODE_MAJOR=22;
const here=path.dirname(fileURLToPath(import.meta.url));
const actualNodeMajor=Number(process.versions.node.split('.')[0]);

assert.equal(
  actualNodeMajor,
  NODE_MAJOR,
  `Step 9 final gate requires Node ${NODE_MAJOR}; got ${process.versions.node}`
);

const stepMatrix=[
  {step:1,file:'member-exact-identity-binding-plan.test.mjs',contract:'exact Member / Prospect / CRM Customer / Family identity bindings'},
  {step:2,file:'member-invitation-redemption-plan.test.mjs',contract:'one-time hashed invitation validation and atomic redemption plan'},
  {step:3,file:'member-prospect-registration-consent-plan.test.mjs',contract:'Prospect registration and append-only consent plan'},
  {step:3,file:'member-prospect-registration-consent-event-schema.test.mjs',contract:'registration / consent append-only event schema contract'},
  {step:4,file:'member-profile-review-queue-plan.test.mjs',contract:'profile review queue before Master updates'},
  {step:5,file:'member-google-profile-sync-plan.test.mjs',contract:'server-side Google profile sync plan'},
  {step:5,file:'member-google-sync-delivery-plan.test.mjs',contract:'idempotent bounded Google sync delivery and reconciliation plan'},
  {step:6,file:'member-d1-pii-retention-plan.test.mjs',contract:'7-day cache / 30-day hard PII retention contract'},
  {step:6,file:'member-profile-cache-read-gate.test.mjs',contract:'profile cache read gate and hard-retention cap'},
  {step:7,file:'member-prospect-customer-promotion-plan.test.mjs',contract:'exact Prospect to canonical CRM Customer promotion plan'},
  {step:8,file:'member-family-pass-integration-read-model.test.mjs',contract:'Family-scoped MEMORY count and durable BLACK integration read evidence'},
  {step:8,file:'member-family-pass-black-entitlement.test.mjs',contract:'server-side FAMILY PASS MEMORY count and durable BLACK exact read evidence'},
  {step:9,file:'member-lifecycle-cross-contract-security.test.mjs',contract:'cross-contract lifecycle security regression'},
  {step:9,file:'member-production-entry-default-off-manifest.test.mjs',contract:'Production activation remains default-off'}
];

const seen=new Set();
for(const entry of stepMatrix){
  assert.ok(Number.isInteger(entry.step)&&entry.step>=1&&entry.step<=9,'invalid step matrix entry');
  assert.match(entry.file,/^[a-z0-9-]+\.test\.mjs$/,'invalid test filename');
  assert.ok(!seen.has(entry.file),`duplicate final-gate test entry: ${entry.file}`);
  seen.add(entry.file);

  const testPath=path.join(here,entry.file);
  const result=spawnSync(process.execPath,[testPath],{
    cwd:path.resolve(here,'..'),
    encoding:'utf8',
    env:{...process.env,MEMBER_RUNTIME_STEP9_NESTED:'1'},
    maxBuffer:16*1024*1024
  });

  if(result.stdout) process.stdout.write(result.stdout);
  if(result.stderr) process.stderr.write(result.stderr);

  assert.equal(
    result.error,
    undefined,
    `Step ${entry.step} failed to execute ${entry.file}: ${result.error?.message||'unknown spawn error'}`
  );
  assert.equal(
    result.signal,
    null,
    `Step ${entry.step} ${entry.file} terminated by signal ${result.signal}`
  );
  assert.equal(
    result.status,
    0,
    `Step ${entry.step} final-gate regression failed: ${entry.file} (${entry.contract})`
  );

  console.log(`STEP_${entry.step}_FINAL_GATE_TEST=PASS file=${entry.file}`);
}

for(let step=1;step<=8;step++){
  assert.ok(stepMatrix.some(entry=>entry.step===step),`missing backend sequence step ${step} from final gate`);
}

assert.ok(stepMatrix.some(entry=>entry.file==='member-family-pass-black-entitlement.test.mjs'));
assert.ok(stepMatrix.some(entry=>entry.file==='member-lifecycle-cross-contract-security.test.mjs'));
assert.ok(stepMatrix.some(entry=>entry.file==='member-production-entry-default-off-manifest.test.mjs'));

console.log('MEMBER_RUNTIME_STEP9_FINAL_GATE=PASS');
console.log(`NODE_MAJOR=${actualNodeMajor}`);
console.log('BACKEND_SEQUENCE_STEPS_1_8=PASS');
console.log('STEP_8_RUNTIME_READ_HARDENING=PASS');
console.log('CROSS_CONTRACT_SECURITY=PASS');
console.log('PRODUCTION_DEFAULT_OFF=PASS');
console.log('PRODUCTION_OPERATION=0');
