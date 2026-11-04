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
    getAttribute(key) { return this.attrs[key]; },
    addEventListener(key, handler) { this.events[key] = handler; },
    focus() { focused = this; }
  };
}
const tabs = destinations.map((name, i) => element(name, i === 0));
const panels = destinations.map((name, i) => element(name, i === 0));
const memoryNames = ['shichigosan', 'family', 'birthday'];
const memoryToggles = memoryNames.map(name => {
  const item = element(name, false);
  item.attrs['aria-controls'] = 'memory-detail-' + name;
  item.attrs['aria-expanded'] = 'false';
  return item;
});
const memoryDetails = Object.fromEntries(memoryNames.map(name => ['memory-detail-' + name, { hidden: true }]));
const memoryActions = memoryNames.map(name => {
  const button = element(name, false);
  button.dataset.memoryCreate = name;
  return button;
});
const formats = ['wallpaper', 'movie', 'story'];
const formatButtons = formats.map(name => {
  const button = element(name, false);
  button.dataset.createFormat = name;
  button.attrs['aria-pressed'] = String(name === 'wallpaper');
  return button;
});
const createLabels = {
  shichigosan: '七五三 · 2026.10.03',
  family: 'Family · 2025.11.16',
  birthday: 'Birthday · 2024.10.20'
};
const createMemory = {
  value: 'shichigosan',
  events: {},
  get selectedOptions() { return [{ textContent: createLabels[this.value] }]; },
  addEventListener(name, fn) { this.events[name] = fn; },
  focus() { focused = this; }
};
const previewTitle = { textContent: '七五三 · 2026.10.03 × スマホ壁紙' };
const previewDescription = { textContent: '思い出の1枚をスマホで楽しむイメージです。' };
const document = {
  querySelectorAll(selector) {
    if (selector === '.tabs [role="tab"]') return tabs;
    if (selector === '[role="tabpanel"]') return panels;
    if (selector === '[data-memory-toggle]') return memoryToggles;
    if (selector === '[data-memory-create]') return memoryActions;
    if (selector === '[data-create-format]') return formatButtons;
    throw new Error('unexpected selector: ' + selector);
  },
  getElementById(id) {
    if (id === 'create-memory') return createMemory;
    if (id === 'create-preview-title') return previewTitle;
    if (id === 'create-preview-description') return previewDescription;
    if (memoryDetails[id]) return memoryDetails[id];
    throw new Error('unexpected id: ' + id);
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

memoryToggles[0].events.click();
assert.equal(memoryToggles[0].attrs['aria-expanded'], 'true');
assert.equal(memoryDetails['memory-detail-shichigosan'].hidden, false);
memoryToggles[1].events.click();
assert.equal(memoryDetails['memory-detail-shichigosan'].hidden, true);
assert.equal(memoryToggles[0].attrs['aria-expanded'], 'false');
assert.equal(memoryToggles[1].attrs['aria-expanded'], 'true');
assert.equal(memoryDetails['memory-detail-family'].hidden, false);
memoryToggles[1].events.click();
assert.equal(memoryToggles[1].attrs['aria-expanded'], 'false');
assert.equal(memoryDetails['memory-detail-family'].hidden, true);
memoryToggles[2].events.click();
memoryActions[2].events.click();
check(2);
assert.equal(createMemory.value, 'birthday');
assert.equal(focused, createMemory);
assert.ok(previewTitle.textContent.includes('Birthday · 2024.10.20'));
formatButtons[1].events.click();
assert.ok(previewTitle.textContent.includes('Memory Movie'));
assert.equal(formatButtons[1].attrs['aria-pressed'], 'true');
assert.equal(formatButtons[0].attrs['aria-pressed'], 'false');
createMemory.value = 'family';
createMemory.events.change();
assert.ok(previewTitle.textContent.includes('Family · 2025.11.16'));
formatButtons[2].events.click();
assert.ok(previewTitle.textContent.includes('Photo Story'));
assert.ok(previewDescription.textContent.includes('アルバム'));
assert.equal(formatButtons[2].attrs['aria-pressed'], 'true');

console.log('MEMBER_STATIC_DEMO_INTERACTIONS=PASS');
console.log('MEMORIES_EXPAND_COLLAPSE_AND_CREATE_HANDOFF=PASS');
console.log('CREATE_SAMPLE_AND_FORMAT_PREVIEW=PASS');
console.log('FIVE_TABS_AND_PANELS=PASS');
console.log('KEYBOARD_ARROW_HOME_END=PASS');
console.log('MEMBER_PASS_BLACK_DISPLAY_CONTRACT=PASS');
console.log('STATIC_DEMO_NO_NETWORK=PASS');
