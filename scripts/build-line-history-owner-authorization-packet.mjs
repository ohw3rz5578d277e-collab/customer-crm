import fs from 'node:fs';
import { buildLineHistoryOwnerAuthorizationPacket } from '../src/crm-line-history-owner-authorization-packet.mjs';

function arg(name){
  const i=process.argv.indexOf(name);
  return i>=0?String(process.argv[i+1]||''):'';
}

const planPath=arg('--plan');
const mainSha=arg('--main-sha');
const outPath=arg('--out')||'line-history-owner-write-authorization-packet.json';

if(!planPath||!mainSha){
  console.error(
    'Usage: node scripts/build-line-history-owner-authorization-packet.mjs '+
    '--plan decision-plan-private.json --main-sha <40hex> '+
    '[--out authorization-packet.json]'
  );
  process.exit(2);
}

const raw=fs.readFileSync(planPath);
const plan=JSON.parse(raw.toString('utf8'));
const packet=buildLineHistoryOwnerAuthorizationPacket({
  plan,
  sourcePlanBytes:raw,
  mainSha
});

fs.writeFileSync(outPath,JSON.stringify(packet,null,2)+'\n');

console.log('RESULT=LINE_HISTORY_OWNER_AUTHORIZATION_PACKET_READY');
console.log('PACKET_READY='+(packet.packet_ready?'YES':'NO'));
console.log('AUTHORIZATION_REQUIRED='+(packet.authorization_required?'YES':'NO'));
console.log('AUTHORIZATION_GRANTED=NO');
console.log('PROPOSED_WRITE_ACTIONS='+packet.proposed_write_actions);
console.log('VALIDATION_ERROR_COUNT='+packet.validation_error_count);
console.log('BLOCKER_COUNT='+packet.blockers.length);
console.log('SOURCE_PLAN_SHA256='+packet.source_plan_sha256);
console.log('SOURCE_MAIN_SHA='+packet.source_main_sha);
if(packet.approval_text)console.log('APPROVAL_TEXT='+packet.approval_text);
console.log('OUTPUT='+outPath);
console.log('PRIVATE_ACTION_VALUES_PRINTED_TO_TERMINAL=0');
console.log('SQL_GENERATED=0');
console.log('SQL_EXECUTED=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_UPDATE=0');
console.log('CUSTOMER_DELETE=0');
console.log('CUSTOMER_MERGE=0');
console.log('LINE_SEND=0');
console.log('WORKER_DEPLOY=0');
console.log('PRODUCTION_DEPLOY=0');
