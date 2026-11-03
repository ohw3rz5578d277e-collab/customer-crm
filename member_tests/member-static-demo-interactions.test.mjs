import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const html = readFileSync(new URL('../public/member-demo.html', import.meta.url), 'utf8');
const destinations = ['home', 'memories', 'create', 'shop', 'my'];

assert.match(html, /<meta name="robots" content="noindex,nofollow,noarchive">/);
assert.ok(html.includes('SAFE STATIC DEMO · NO CUSTOMER DATA'));
assert.ok(html.includes('8 / 10 MEMORIES'));
assert.ok(html.includes('BLACKの条件は10 MEMORIES'));
assert.ok(html.includes('1回の撮影につき原則1 MEMORY'));
assert.ok(html.includes('PHOTO GOODS 10% OFF FOREVER'));
assert.ok(html.includes('撮影料金は割引対象外'));
assert.ok(html.includes('自動付与・割引適用・購入処理を行いません'));
assert.equal((html.match(/<button[^>]*role="tab"/g) || []).length, 5);
assert.equal((html.match(/<section[^>]*role="tabpanel"/g) || []).length, 5);

for (const name of destinations) {
  assert.ok(html.includes('id="member-tab-' + name + '"'));
  assert.ok(html.includes('aria-controls="member-panel-' + name + '"'));
  assert.ok(html.includes('id="member-panel-' + name + '"'));
  assert.ok(html.includes('aria-labelledby="member-tab-' + name + '"'));
}
assert.equal((html.match(/aria-selected="true"/g) || []).length, 1);
assert.match(html, /\.tab\{[^}]*min-height:48px/);
assert.match(html, /\.tab:focus-visible\{/);
assert.doesNotMatch(html, /<script[^>]+src=/i);
assert.doesNotMatch(html, /\bfetch\s*\(|XMLHttpRequest|WebSocket|EventSource|localStorage|sessionStorage|\/api\//);

const scripts = [...html.matchAll(/<script>([\s\S]*?)<\/script>/g)];
assert.equal(scripts.length, 1, 'preview must remain a single local inline script');
const script = new vm.Script(scripts[0][1], { filename: 'member-demo-inline.js' });

let focused = null;
let scrollCalls = 0;
function element(name, active) {
  const classes = new Set(active ? ['active'] : []);
  return {
    dataset: { tab: name, panel: name },
    attrs: { 'aria-selected': String(active) },
    tabIndex: active ? 0 : -1,
    classList: {
      toggle(token, enabled) {
        if (enabled) classes.add(token); else classes.delete(token);
      },
      contains(token) { return classes.has(token); }
    },
    events: {},
    setAttribute(key, value) { this.attrs[key] = value; },
    addEventListener(key, handler) { this.events[key] = handler; },
    focus() { focused = this; }
  };
}
const tabs = destinations.map((name, i) => element(name, i === 0));
const panels = destinations.map((name, i) => element(name, i === 0));
const document = {
  querySelectorAll(selector) {
    if (selector === '.tabs [role="tab"]') return tabs;
    if (selector === '[role="tabpanel"]') return panels;
    throw new Error('unexpected selector: ' + selector);
  }
};
script.runInNewContext({ document, window: { scrollTo() { scrollCalls++; } } }, { timeout: 1000 });

function check(index) {
  tabs.forEach((tab, i) => {
    const selected = i === index;
    assert.equal(tab.classList.contains('active'), selected, 'active tab ' + i);
    assert.equal(tab.attrs['aria-selected'], String(selected), 'aria-selected tab ' + i);
    assert.equal(tab.tabIndex, selected ? 0 : -1, 'roving tabindex ' + i);
    assert.equal(panels[i].classList.contains('active'), selected, 'active panel ' + i);
  });
}
function key(index, name, expectedFocus, expectedActive, shouldPrevent = true) {
  let prevented = false;
  tabs[index].events.keydown({ key: name, preventDefault() { prevented = true; } });
  assert.equal(prevented, shouldPrevent, 'handled key: ' + name);
  if (expectedFocus !== null) assert.equal(focused, tabs[expectedFocus]);
  check(expectedActive);
}
check(0);
tabs[3].events.click();
check(3);
key(3, 'ArrowRight', 4, 4);
key(4, 'ArrowRight', 0, 0);
key(0, 'ArrowLeft', 4, 4);
key(4, 'Home', 0, 0);
key(0, 'End', 4, 4);
key(4, 'Escape', null, 4, false);
tabs[2].events.click();
check(2);
assert.ok(scrollCalls >= 7);
console.log('MEMBER_STATIC_DEMO_INTERACTIONS=PASS');
console.log('FIVE_TABS_AND_PANELS=PASS');
console.log('KEYBOARD_ARROW_HOME_END=PASS');
console.log('MEMBER_PASS_BLACK_DISPLAY_CONTRACT=PASS');
console.log('STATIC_DEMO_NO_NETWORK=PASS');
