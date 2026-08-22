import assert from 'node:assert/strict';
import fs from 'node:fs';

const source = fs.readFileSync(new URL('../src/crm-marketing-browser-polish.mjs', import.meta.url), 'utf8');

assert.match(source, /data-crm-mobile-label/);
assert.match(source, /リピート売上/);
assert.match(source, /min-height:44px/);
assert.match(source, /env\(safe-area-inset-bottom\)/);
assert.match(source, /marketing_line_send_enabled:\s*false/);
for (const forbidden of ['broadcast', 'multicast', 'pushMessage', 'replyMessage', 'schedule send']) {
  assert.equal(source.includes(forbidden), false, `forbidden send control found: ${forbidden}`);
}
console.log('crm-marketing-browser-polish tests PASS');
