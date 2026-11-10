import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { once } from 'node:events';
import { readFile, mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

const names = ['home', 'memories', 'create', 'shop', 'my'];
const html = await readFile(new URL('../public/member-demo.html', import.meta.url), 'utf8');
const binary = ['google-chrome', 'google-chrome-stable', 'chromium', 'chromium-browser']
  .find(cmd => { const r = spawnSync(cmd, ['--version'], { stdio: 'ignore', timeout: 3000 }); return !r.error && r.status === 0; });
assert.ok(binary, 'MEMBER_BROWSER_MISSING: Chromium/Chrome required');
const profile = await mkdtemp(join(tmpdir(), 'member-static-demo-'));
const child = spawn(binary, ['--headless=new', '--no-sandbox', '--disable-dev-shm-usage', '--disable-background-networking', '--no-first-run', '--remote-debugging-port=0', '--user-data-dir=' + profile, 'about:blank'], { stdio: 'ignore' });
let ws;
try {
  let port;
  for (let tries = 0; tries < 120; tries++) {
    if (child.exitCode !== null) throw Error('Chromium exited: ' + child.exitCode);
    try { port = Number((await readFile(join(profile, 'DevToolsActivePort'), 'utf8')).split('\n')[0]); if (port) break; }
    catch { /* Chrome still starting */ }
    await new Promise(r => setTimeout(r, 100));
  }
  assert.ok(port, 'MEMBER_BROWSER_DEVTOOLS_UNAVAILABLE');
  const targets = await (await fetch('http://127.0.0.1:' + port + '/json/list')).json();
  const page = targets.find(t => t.type === 'page' && t.webSocketDebuggerUrl);
  assert.ok(page, 'Chrome page target missing');
  ws = new WebSocket(page.webSocketDebuggerUrl);
  await new Promise((ok, fail) => { ws.addEventListener('open', ok, { once: true }); ws.addEventListener('error', fail, { once: true }); });
  let nextId = 0;
  const waits = new Map();
  ws.addEventListener('message', event => {
    const msg = JSON.parse(event.data);
    const wait = waits.get(msg.id);
    if (wait) { waits.delete(msg.id); msg.error ? wait.reject(Error(JSON.stringify(msg.error))) : wait.resolve(msg.result); }
  });
  const send = (method, params = {}) => new Promise((resolve, reject) => {
    const id = ++nextId;
    waits.set(id, { resolve, reject });
    ws.send(JSON.stringify({ id, method, params }));
  });
  const evaluate = async expression => {
    const r = await send('Runtime.evaluate', { expression, returnByValue: true });
    assert.ok(!r.exceptionDetails, 'Browser JavaScript error: ' + JSON.stringify(r.exceptionDetails));
    return r.result.value;
  };
  const pressKey = async (key, code, virtualKey) => {
    for (const type of ['keyDown', 'keyUp']) {
      const enterText = type === 'keyDown' && key === 'Enter' ? { text: '\r', unmodifiedText: '\r' } : {};
      await send('Input.dispatchKeyEvent', { type, key, code, windowsVirtualKeyCode: virtualKey, ...enterText });
    }
  };
  await send('Page.enable');
  await send('Runtime.enable');
  for (const width of [320, 375, 390, 768]) {
    await send('Emulation.setDeviceMetricsOverride', { width, height: 820, deviceScaleFactor: 1, mobile: true });
    await send('Page.navigate', { url: 'about:blank' });
    const frame = await send('Page.getFrameTree');
    await send('Page.setDocumentContent', { frameId: frame.frameTree.frame.id, html });
    let ready = false;
    for (let attempts = 0; attempts < 40; attempts++) {
      ready = await evaluate("document.readyState === 'complete' && !!document.getElementById('member-tab-home')");
      if (ready) break;
      await new Promise(r => setTimeout(r, 100));
    }
    assert.ok(ready, width + ': demo not ready');
    const state = () => evaluate("({viewport:document.documentElement.clientWidth,doc:document.documentElement.scrollWidth,body:document.body.scrollWidth,tabs:[...document.querySelectorAll('[role=tab]')].map(x=>({name:x.dataset.tab,height:x.getBoundingClientRect().height})),selected:document.querySelector('[role=tab][aria-selected=true]')?.dataset.tab,visible:[...document.querySelectorAll('[role=tabpanel]')].filter(x=>getComputedStyle(x).display!=='none').map(x=>x.dataset.panel)})");
    const initial = await state();
    assert.ok(Math.max(initial.doc, initial.body) <= initial.viewport + 1, width + ': horizontal overflow ' + JSON.stringify(initial));
    assert.deepEqual(initial.tabs.map(t => t.name), names, width + ': missing tabs');
    assert.ok(initial.tabs.every(t => t.height >= 44), width + ': small tap target');
    assert.equal(initial.selected, 'home');
    assert.deepEqual(initial.visible, ['home']);
    // HOME source-only journey: featured Family MEMORY -> detail -> HOME;
    // NEXT MEMORY sample guidance; then jump directly into CREATE.
    const homeTargets = await evaluate("({memory:document.getElementById('home-memory-open').getBoundingClientRect().height,next:document.querySelector('#home-next summary').getBoundingClientRect().height,create:document.getElementById('home-create-open').getBoundingClientRect().height})");
    assert.ok(Object.values(homeTargets).every(h => h >= 44), width + ': HOME controls must be accessible');
    await evaluate("document.getElementById('home-memory-open').focus()");
    await pressKey('Enter', 'Enter', 13);
    const homeToMemory = await evaluate("({tab:document.querySelector('[role=tab][aria-selected=true]')?.dataset.tab,detail:!document.getElementById('memory-detail').hidden,indexHidden:document.getElementById('memory-index').hidden,title:document.getElementById('memory-detail-title').textContent,focus:document.activeElement?.id})");
    assert.deepEqual(homeToMemory, {tab:'memories',detail:true,indexHidden:true,title:'Family',focus:'memory-detail'}, width + ': HOME featured memory handoff');
    await evaluate("document.getElementById('memory-back').focus()");
    await pressKey('Enter', 'Enter', 13);
    const backToHome = await evaluate("({tab:document.querySelector('[role=tab][aria-selected=true]')?.dataset.tab,detailHidden:document.getElementById('memory-detail').hidden,indexVisible:!document.getElementById('memory-index').hidden,focus:document.activeElement?.id})");
    assert.deepEqual(backToHome, {tab:'home',detailHidden:true,indexVisible:true,focus:'home-memory-open'}, width + ': HOME memory return/focus');
    await evaluate("document.querySelector('#home-next summary').focus()");
    await pressKey('Enter', 'Enter', 13);
    assert.equal(await evaluate("document.getElementById('home-next').open"), true, width + ': NEXT MEMORY expand');
    const nextMemory = await evaluate("({body:document.querySelector('#home-next .home-next-body').textContent,doc:Math.max(document.documentElement.scrollWidth,document.body.scrollWidth),viewport:document.documentElement.clientWidth})");
    assert.ok(nextMemory.body.includes('個別の記念日判定・通知・予約は行いません'), width + ': NEXT MEMORY demo notice');
    assert.ok(nextMemory.doc <= nextMemory.viewport + 1, width + ': NEXT MEMORY overflow');
    await pressKey(' ', 'Space', 32);
    assert.equal(await evaluate("document.getElementById('home-next').open"), false, width + ': NEXT MEMORY collapse');
    await evaluate("document.getElementById('home-create-open').focus()");
    await pressKey('Enter', 'Enter', 13);
    const homeToCreate = await evaluate("({tab:document.querySelector('[role=tab][aria-selected=true]')?.dataset.tab,editor:!document.getElementById('create-editor').hidden,previewHidden:document.getElementById('create-preview').hidden,focus:document.activeElement?.id})");
    assert.deepEqual(homeToCreate, {tab:'create',editor:true,previewHidden:true,focus:'create-memory-select'}, width + ': HOME CREATE shortcut');
    await evaluate("document.getElementById('member-tab-home').click()");
    const returnedHome = await state();
    assert.deepEqual(returnedHome.visible,['home'],width+': HOME should remain navigable after shortcuts');
    assert.ok(Math.max(returnedHome.doc,returnedHome.body)<=returnedHome.viewport+1,width+': HOME after shortcut overflow');
    console.log('MEMBER_HOME_JOURNEY_VIEWPORT_' + width + '=PASS');
    for (const name of names) {
      await evaluate("document.getElementById('member-tab-" + name + "').click()");
      const result = await state();
      assert.equal(result.selected, name, width + ': wrong selected tab');
      assert.deepEqual(result.visible, [name], width + ': wrong visible panel');
    }
    await evaluate("document.getElementById('member-tab-my').focus()");
    await send('Input.dispatchKeyEvent', { type: 'keyDown', key: 'ArrowRight', code: 'ArrowRight', windowsVirtualKeyCode: 39 });
    await send('Input.dispatchKeyEvent', { type: 'keyUp', key: 'ArrowRight', code: 'ArrowRight', windowsVirtualKeyCode: 39 });
    const afterKey = await state();
    const focus = await evaluate('document.activeElement?.dataset.tab');
    assert.equal(afterKey.selected, 'home', width + ': keyboard wrap failed');
    assert.equal(focus, 'home', width + ': keyboard focus failed');
    assert.ok(Math.max(afterKey.doc, afterKey.body) <= afterKey.viewport + 1, width + ': overflow after navigation');
    // MEMORIES detail -> CREATE transfer -> format preview; demo-only navigation.
    await evaluate("document.getElementById('member-tab-memories').click()");
    const memoryStart = await evaluate("({indexVisible:!document.getElementById('memory-index').hidden,detailHidden:document.getElementById('memory-detail').hidden,entryHeight:document.querySelector('[data-memory=family]').getBoundingClientRect().height})");
    assert.ok(memoryStart.indexVisible && memoryStart.detailHidden, width + ': MEMORIES initial state');
    assert.ok(memoryStart.entryHeight >= 44, width + ': MEMORIES tap target');
    await evaluate("document.querySelector('[data-memory=family]').focus()");
    await pressKey('Enter', 'Enter', 13);
    const detail = await evaluate("({indexHidden:document.getElementById('memory-index').hidden,detailVisible:!document.getElementById('memory-detail').hidden,title:document.getElementById('memory-detail-title').textContent,focus:document.activeElement?.id,actionHeight:document.getElementById('memory-to-create').getBoundingClientRect().height})");
    assert.ok(detail.indexHidden && detail.detailVisible, width + ': MEMORY detail visibility');
    assert.equal(detail.title, 'Family', width + ': MEMORY detail title');
    assert.equal(detail.focus, 'memory-detail', width + ': MEMORY detail focus');
    assert.ok(detail.actionHeight >= 44, width + ': MEMORY CTA target');
    await evaluate("document.getElementById('memory-to-create').focus()");
    await pressKey('Enter', 'Enter', 13);
    const transferred = await evaluate("({tab:document.querySelector('[role=tab][aria-selected=true]').dataset.tab,memory:document.getElementById('create-memory-select').value,editor:!document.getElementById('create-editor').hidden,focus:document.activeElement?.id})");
    assert.deepEqual(transferred, { tab: 'create', memory: 'family', editor: true, focus: 'create-memory-select' }, width + ': MEMORY to CREATE transfer');
    const picker = await evaluate("({selectHeight:document.getElementById('create-memory-select').getBoundingClientRect().height,formats:[...document.querySelectorAll('[data-create-format]')].map(x=>({name:x.dataset.createFormat,height:x.getBoundingClientRect().height,pressed:x.getAttribute('aria-pressed')}))})");
    assert.ok(picker.selectHeight >= 44, width + ': CREATE select target');
    assert.deepEqual(picker.formats.map(x=>x.name), ['wallpaper', 'movie', 'story'], width + ': CREATE formats');
    assert.ok(picker.formats.every(x=>x.height >= 44), width + ': CREATE format targets');
    assert.deepEqual(picker.formats.map(x=>x.pressed), ['true', 'false', 'false'], width + ': default format');
    await evaluate("document.querySelector('[data-create-format=movie]').focus()");
    await pressKey(' ', 'Space', 32);
    const newFormats = await evaluate("[...document.querySelectorAll('[data-create-format]')].map(x=>x.getAttribute('aria-pressed'))");
    assert.deepEqual(newFormats, ['false', 'true', 'false'], width + ': selected format');
    await evaluate("document.getElementById('create-preview-open').focus()");
    await pressKey('Enter', 'Enter', 13);
    const preview = await evaluate("({editorHidden:document.getElementById('create-editor').hidden,previewVisible:!document.getElementById('create-preview').hidden,title:document.getElementById('create-preview-title').textContent,summary:document.getElementById('create-preview-summary').textContent,focus:document.activeElement?.id})");
    assert.ok(preview.editorHidden && preview.previewVisible, width + ': CREATE preview visibility');
    assert.ok(preview.title.includes('Memory Movie') && preview.summary.includes('Family'), width + ': CREATE preview text');
    assert.equal(preview.focus, 'create-preview', width + ': CREATE preview focus');
    await evaluate("document.getElementById('create-preview-back').focus()");
    await pressKey('Enter', 'Enter', 13);
    const restored = await evaluate("({editorVisible:!document.getElementById('create-editor').hidden,previewHidden:document.getElementById('create-preview').hidden,focus:document.activeElement?.id})");
    assert.deepEqual(restored, { editorVisible: true, previewHidden: true, focus: 'create-preview-open' }, width + ': CREATE edit restore');
    await evaluate("document.getElementById('member-tab-memories').click();document.getElementById('memory-back').focus()");
    await pressKey('Enter', 'Enter', 13);
    const returned = await evaluate("({indexVisible:!document.getElementById('memory-index').hidden,detailHidden:document.getElementById('memory-detail').hidden,focus:document.activeElement?.dataset.memory,viewport:document.documentElement.clientWidth,scrollWidth:Math.max(document.documentElement.scrollWidth,document.body.scrollWidth)})");
    assert.ok(returned.indexVisible && returned.detailHidden, width + ': memory back');
    assert.equal(returned.focus, 'family', width + ': memory back focus');
    assert.ok(returned.scrollWidth <= returned.viewport + 1, width + ': detail/CREATE horizontal overflow');
    // SHOP and MY remain disclosure-only and work with keyboard in narrow viewports.
    await evaluate("document.getElementById('member-tab-shop').click()");
    const shopInitial = await evaluate("({items:[...document.querySelectorAll('#member-panel-shop details.shop-preview')].map(x=>x.open),targets:[...document.querySelectorAll('#member-panel-shop summary')].map(x=>x.getBoundingClientRect().height)})");
    assert.deepEqual(shopInitial.items, [false, false], width + ': SHOP starts collapsed');
    assert.ok(shopInitial.targets.every(x=>x>=44), width + ': SHOP summary tap target');
    await evaluate("document.querySelector('#member-panel-shop details.shop-preview summary').focus()");
    await pressKey('Enter', 'Enter', 13);
    assert.equal(await evaluate("document.querySelector('#member-panel-shop details.shop-preview').open"), true, width + ': SHOP keyboard expand');
    const shopSize = await state();
    assert.ok(Math.max(shopSize.doc,shopSize.body)<=shopSize.viewport+1,width+': SHOP overflow');
    await pressKey('Enter', 'Enter', 13);
    assert.equal(await evaluate("document.querySelector('#member-panel-shop details.shop-preview').open"), false, width + ': SHOP keyboard collapse');
    // The second sample product must also be keyboard operable without changing the first.
    await evaluate("document.querySelectorAll('#member-panel-shop details.shop-preview summary')[1].focus()");
    await pressKey(' ', 'Space', 32);
    const secondShopOpen = await evaluate("({expanded:[...document.querySelectorAll('#member-panel-shop details.shop-preview')].map(x=>x.open),focus:document.activeElement===document.querySelectorAll('#member-panel-shop details.shop-preview summary')[1]})");
    assert.deepEqual(secondShopOpen, { expanded: [false, true], focus: true }, width + ': second SHOP keyboard expand and focus');
    const secondShopSize = await state();
    assert.ok(Math.max(secondShopSize.doc,secondShopSize.body)<=secondShopSize.viewport+1,width+': second SHOP overflow');
    await pressKey(' ', 'Space', 32);
    assert.deepEqual(await evaluate("[...document.querySelectorAll('#member-panel-shop details.shop-preview')].map(x=>x.open)"), [false, false], width + ': second SHOP keyboard collapse');
    await evaluate("document.getElementById('member-tab-my').click();document.querySelector('#member-panel-my .my-info summary').focus()");
    await pressKey('Enter', 'Enter', 13);
    assert.equal(await evaluate("document.querySelector('#member-panel-my .my-info').open"), true, width + ': MY keyboard expand');
    const mySize = await state();
    assert.ok(Math.max(mySize.doc,mySize.body)<=mySize.viewport+1,width+': MY overflow');
    await pressKey('Enter', 'Enter', 13);
    assert.equal(await evaluate("document.querySelector('#member-panel-my .my-info').open"), false, width + ': MY keyboard collapse');
    console.log('MEMBER_SHOP_MY_BROWSER_VIEWPORT_' + width + '=PASS');
    console.log('MEMBER_MEMORIES_CREATE_VIEWPORT_' + width + '=PASS');
    console.log('MEMBER_BROWSER_VIEWPORT_' + width + '=PASS');
  }
  console.log('MEMBER_STATIC_DEMO_CHROMIUM=PASS');
} finally {
  try { ws?.close(); } catch {}
  if (child.exitCode === null && !child.killed) {
    child.kill('SIGTERM');
    try { await Promise.race([once(child, 'exit'), new Promise(r => setTimeout(r, 5000))]); } catch {}
  }
  await rm(profile, { recursive: true, force: true, maxRetries: 5, retryDelay: 150 });
}
