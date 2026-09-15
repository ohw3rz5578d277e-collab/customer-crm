import assert from 'node:assert/strict';
import {
  LEGACY_OWNER_VISUAL_ASSET_IDS,
  stripLegacyOwnerVisualAssets,
  composeCustomer360AdminHtml
} from '../src/production-index-crm-customer360-entry.js';

const legacyAssets=LEGACY_OWNER_VISUAL_ASSET_IDS.map(id =>
  '<style id="'+id+'">#legacy{display:block}</style><script id="'+id+'">window.__legacyVisual=1</script>'
).join('');

const source='<!doctype html><html><head>'+
  '<style id="crm-production-safe-controls">.safe{display:none}</style>'+
  legacyAssets+
  '</head><body><div class="app"><main>base</main></div>'+
  '<a id="crmReconciliationLink" href="/admin/customer-id-reconciliation">legacy review</a>'+
  '</body></html>';

const stripped=stripLegacyOwnerVisualAssets(source);
for(const id of LEGACY_OWNER_VISUAL_ASSET_IDS){
  assert.equal(stripped.includes('id="'+id+'"'),false,'legacy asset survived: '+id);
}
assert.equal(stripped.includes('crmReconciliationLink'),false,'legacy reconciliation floating link survived');
assert.equal(stripped.includes('crm-production-safe-controls'),true,'production safety controls style must be preserved');
assert.equal(stripped.includes('<div class="app">'),true,'base application host must be preserved');

const composed=composeCustomer360AdminHtml(source);
for(const id of LEGACY_OWNER_VISUAL_ASSET_IDS){
  assert.equal(composed.includes('id="'+id+'"'),false,'legacy asset reappeared after canonical composition: '+id);
}
assert.equal(/<a\\b[^>]*\\bid=["']crmReconciliationLink["']/i.test(composed),false,'legacy reconciliation link DOM reappeared');
assert.equal(composed.includes('crm-owner-app-shell-script'),true,'canonical Owner App Shell missing');
assert.equal(composed.includes('crm-owner-view-state-v2-script'),true,'canonical view state missing');
assert.equal(composed.includes('crm-owner-line-chat-script'),true,'canonical LINE chat missing');
assert.equal(composed.includes('crm-customer-csv-import-script'),true,'canonical CSV import missing');
assert.equal(composed.includes('crm-production-safe-controls'),true,'production safety controls style removed by composition');

console.log('CRM_LEGACY_VISUAL_ASSETS_QUARANTINE=PASS');
console.log('LEGACY_VISUAL_ASSET_COUNT='+LEGACY_OWNER_VISUAL_ASSET_IDS.length);
console.log('PRODUCTION_WRITE=0');
console.log('LINE_SEND=0');
