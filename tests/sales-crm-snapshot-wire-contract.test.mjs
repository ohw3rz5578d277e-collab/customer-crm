import fs from 'node:fs';
import assert from 'node:assert/strict';
import {
  parseProductionSnapshotText,
  PRODUCTION_SNAPSHOT_FORMAT,
  PRODUCTION_SNAPSHOT_SCOPE
} from '../src/crm-sales-snapshot.mjs';

const EXPECTED_FORMAT='customer-crm-production-identity-snapshot-v1';
const EXPECTED_SCOPE='all_customer_identities';

assert.equal(PRODUCTION_SNAPSHOT_FORMAT,EXPECTED_FORMAT,'parser snapshot format drifted from wire contract');
assert.equal(PRODUCTION_SNAPSHOT_SCOPE,EXPECTED_SCOPE,'parser snapshot scope drifted from wire contract');

const literalEnvelope=JSON.stringify({
  snapshot_format:EXPECTED_FORMAT,
  complete:true,
  query_scope:EXPECTED_SCOPE,
  customer_count:1,
  customers:[{
    customer_id:'26990001',
    name:'契約テスト',
    line_user_id:'U11111111111111111111'
  }]
});
const rows=parseProductionSnapshotText(literalEnvelope);
assert.equal(rows.length,1);
assert.equal(rows[0].customer_id,'26990001');

const runner=fs.readFileSync('scripts/run-sales-crm-readonly-reconciliation.sh','utf8');
assert.match(runner,/data\.get\('snapshot_format'\) != 'customer-crm-production-identity-snapshot-v1'/);
assert.match(runner,/data\.get\('query_scope'\) != 'all_customer_identities'/);
assert.match(runner,/PRODUCTION_SNAPSHOT_FORMAT=customer-crm-production-identity-snapshot-v1/);
assert.match(runner,/PRODUCTION_SNAPSHOT_SCOPE=all_customer_identities/);

console.log('SALES_CRM_SNAPSHOT_WIRE_CONTRACT=1/1 PASS');
