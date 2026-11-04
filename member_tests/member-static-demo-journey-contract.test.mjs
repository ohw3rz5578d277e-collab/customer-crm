import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const html = readFileSync(new URL('../public/member-demo.html', import.meta.url), 'utf8');
const destinationTabs = ['home', 'memories', 'create', 'shop', 'my'];
const sampleMemories = ['shichigosan', 'family', 'birthday'];
const sampleFormats = ['wallpaper', 'movie', 'story'];

assert.match(html, /<meta name="robots" content="noindex,nofollow,noarchive">/);
assert.ok(html.includes('SAFE STATIC DEMO · NO CUSTOMER DATA'));
assert.ok(html.includes('BLACKの条件は10 MEMORIES'));
assert.ok(html.includes('8 / 10 MEMORIES'));
assert.ok(html.includes('1回の撮影につき原則1 MEMORY'));
assert.ok(html.includes('PHOTO GOODS 10% OFF FOREVER'));
assert.ok(html.includes('撮影料金は割引対象外'));
assert.ok(html.includes('自動付与・割引適用・購入処理を行いません'));

for (const name of destinationTabs) {
  assert.ok(html.includes('id="member-tab-' + name + '"'), 'missing tab ' + name);
  assert.ok(html.includes('id="member-panel-' + name + '"'), 'missing panel ' + name);
  assert.ok(html.includes('aria-controls="member-panel-' + name + '"'), 'missing aria-controls ' + name);
  assert.ok(html.includes('aria-labelledby="member-tab-' + name + '"'), 'missing aria-labelledby ' + name);
}
assert.equal((html.match(/<button[^>]*role="tab"/g) || []).length, 5);
assert.equal((html.match(/<section[^>]*role="tabpanel"/g) || []).length, 5);
assert.equal((html.match(/aria-selected="true"/g) || []).length, 1);

assert.match(html, /id="memory-index"/);
assert.match(html, /id="memory-detail"[^>]*hidden/);
assert.match(html, /id="memory-back"/);
assert.match(html, /id="memory-to-create"/);
assert.match(html, /id="create-editor"/);
assert.match(html, /id="create-preview"[^>]*hidden/);
assert.match(html, /id="create-preview-open"/);
assert.match(html, /id="create-preview-back"/);
assert.match(html, /id="create-memory-select"/);
for (const name of sampleMemories) {
  assert.ok(html.includes('data-memory="' + name + '"'), 'missing sample memory ' + name);
  assert.ok(html.includes('<option value="' + name + '">'), 'missing CREATE memory ' + name);
}
for (const name of sampleFormats) {
  assert.ok(html.includes('data-create-format="' + name + '"'), 'missing sample format ' + name);
}
assert.equal((html.match(/data-memory="/g) || []).length, 3);
assert.equal((html.match(/data-create-format="/g) || []).length, 3);
assert.equal((html.match(/aria-pressed="true"/g) || []).length, 1);

// Demonstration source must never read customer data or call production endpoints.
assert.doesNotMatch(html, /<script[^>]+src=|<img[^>]+src=|<iframe/i);
assert.doesNotMatch(html, /\bfetch\s*\(|XMLHttpRequest|WebSocket|EventSource|localStorage|sessionStorage|navigator\.sendBeacon|\/api\//i);
assert.doesNotMatch(html, /<input[^>]+type=["']file["']|<form\b/i);
const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)];
assert.equal(scripts.length, 1, 'exactly one local inline script');
new vm.Script(scripts[0][1], { filename: 'member-demo-inline.js' });
assert.doesNotMatch(scripts[0][1], /import\s*\(|\beval\s*\(|\bnew\s+Function\s*\(/);
console.log('MEMBER_DEMO_FIVE_TABS_AND_BLACK_CONTRACT=PASS');
console.log('MEMBER_DEMO_THREE_MEMORIES_THREE_FORMATS=PASS');
console.log('MEMBER_DEMO_LOCAL_ONLY_NO_CUSTOMER_DATA=PASS');
