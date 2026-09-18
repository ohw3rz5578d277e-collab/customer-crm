import fs from 'node:fs';
import { buildExactReservationEvidence } from '../src/crm-line-history-exact-reservation-evidence.mjs';

function arg(name){
  const i=process.argv.indexOf(name);
  return i>=0?String(process.argv[i+1]||''):'';
}

function readRows(path){
  if(!path)throw new Error('missing input path');
  const raw=JSON.parse(fs.readFileSync(path,'utf8'));

  if(Array.isArray(raw)){
    if(raw.length===1&&raw[0]&&Array.isArray(raw[0].results))return raw[0].results;
    return raw;
  }

  if(raw&&Array.isArray(raw.rows))return raw.rows;
  if(raw&&Array.isArray(raw.results))return raw.results;
  if(raw&&raw.result&&Array.isArray(raw.result.results))return raw.result.results;

  throw new Error('unsupported JSON shape: '+path);
}

const reservationHistoryPath=arg('--reservation-history');
const crmReservationsPath=arg('--crm-reservations');
const outPath=arg('--out')||'line-history-exact-reservation-evidence.json';

if(!reservationHistoryPath||!crmReservationsPath){
  console.error(
    'Usage: node scripts/build-line-history-exact-reservation-evidence.mjs '+
    '--reservation-history reservation-history.json '+
    '--crm-reservations crm-exact-reservation.json '+
    '[--out exact-reservation-evidence.json]'
  );
  process.exit(2);
}

const result=buildExactReservationEvidence({
  reservationHistory:readRows(reservationHistoryPath),
  crmReservations:readRows(crmReservationsPath)
});

fs.writeFileSync(
  outPath,
  JSON.stringify(result.rows,null,2)+'\n'
);

console.log('RESULT=LINE_HISTORY_EXACT_RESERVATION_EVIDENCE_READY');
console.log('RESERVATION_HISTORY_ROWS='+result.reservation_history_rows);
console.log('CRM_RESERVATION_ROWS='+result.crm_reservation_rows);
console.log('EXACT_EVIDENCE_ROWS='+result.exact_evidence_rows);
console.log('SOURCE_GROUPS_WITH_EXACT_MATCH='+result.source_groups_with_exact_match);
console.log('OUTPUT='+outPath);
console.log('RAW_RESERVATION_ID_OUTPUT=0');
console.log('RAW_LINE_USER_ID_OUTPUT=0');
console.log('MESSAGE_TEXT_OUTPUT=0');
console.log('PRODUCTION_D1_READ=0');
console.log('PRODUCTION_D1_WRITE=0');
console.log('CUSTOMER_ID_GENERATION=0');
console.log('CUSTOMER_UPDATE=0');
console.log('CUSTOMER_DELETE=0');
console.log('LINE_SEND=0');
