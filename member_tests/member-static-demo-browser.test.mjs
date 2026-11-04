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

    // MEMORY selection, preserved selection, and CREATE local-only samples.
    await evaluate("document.getElementById('member-tab-memories').click()");
    const initialMemory = await evaluate("({title:document.getElementById('memory-detail-title').textContent,pressed:[...document.querySelectorAll('[data-memory-key][aria-pressed=true]')].map(x=>x.dataset.memoryKey)})");
    assert.equal(initialMemory.title, '七五三', width + ': initial memory title');
    assert.deepEqual(initialMemory.pressed, ['shichigosan'], width + ': initial memory selection');
    await evaluate("document.querySelector('[data-memory-key=birthday]').click()");
    const birthday = await evaluate("({title:document.getElementById('memory-detail-title').textContent,pressed:[...document.querySelectorAll('[data-memory-key][aria-pressed=true]')].map(x=>x.dataset.memoryKey),photos:document.getElementById('memory-detail-meta').textContent})");
    assert.equal(birthday.title, 'Birthday', width + ': memory selection title');
    assert.deepEqual(birthday.pressed, ['birthday'], width + ': memory selection state');
    assert.ok(birthday.photos.includes('88 photos'), width + ': memory photo count');
    await evaluate("document.getElementById('memory-create-button').click()");
    const handoff = await evaluate("({selected:document.querySelector('[role=tab][aria-selected=true]')?.dataset.tab,memory:document.getElementById('create-memory-select').value,focus:document.activeElement?.id})");
    assert.equal(handoff.selected, 'create', width + ': MEMORY to CREATE tab');
    assert.equal(handoff.memory, 'birthday', width + ': selected memory handoff');
    assert.equal(handoff.focus, 'create-memory-select', width + ': memory selector focus');

    await evaluate("document.getElementById('create-preview-button').click()");
    const wallpaper = await evaluate("({heading:document.getElementById('create-preview-title').textContent,body:document.getElementById('create-preview-description').textContent,ready:document.querySelector('.create-output').classList.contains('is-ready')})");
    assert.equal(wallpaper.heading, 'Birthday · Wallpaper', width + ': Wallpaper preview');
    assert.ok(wallpaper.body.includes('実際の写真や動画は使用していません'), width + ': no customer photos');
    assert.equal(wallpaper.ready, true, width + ': preview ready state');

    await evaluate("document.getElementById('create-format-select').value='movie';document.getElementById('create-format-select').dispatchEvent(new Event('change',{bubbles:true}))");
    assert.equal(await evaluate("document.querySelector('.create-output').classList.contains('is-ready')"), false, width + ': invalidate stale preview');
    await evaluate("document.getElementById('create-preview-button').click()");
    const movie = await evaluate("({heading:document.getElementById('create-preview-title').textContent,movie:document.getElementById('create-output-art').classList.contains('is-movie')})");
    assert.equal(movie.heading, 'Birthday · Memory Movie', width + ': movie title');
    assert.equal(movie.movie, true, width + ': movie style');

    await evaluate("document.getElementById('create-format-select').value='story';document.getElementById('create-format-select').dispatchEvent(new Event('change',{bubbles:true}));document.getElementById('create-preview-button').click()");
    const story = await evaluate("({heading:document.getElementById('create-preview-title').textContent,story:document.getElementById('create-output-art').classList.contains('is-story'),movie:document.getElementById('create-output-art').classList.contains('is-movie')})");
    assert.equal(story.heading, 'Birthday · Photo Story', width + ': story title');
    assert.equal(story.story, true, width + ': story style');
    assert.equal(story.movie, false, width + ': stale movie style cleared');

    await evaluate("document.getElementById('create-memory-select').value='family';document.getElementById('create-memory-select').dispatchEvent(new Event('change',{bubbles:true}))");
    assert.equal(await evaluate("document.querySelector('.create-output').classList.contains('is-ready')"), false, width + ': memory change clears stale preview');
    await evaluate("document.getElementById('create-preview-button').click()");
    assert.equal(await evaluate("document.getElementById('create-preview-title').textContent"), 'Family · Photo Story', width + ': manual memory selection');
    const finalFlow = await state();
    assert.ok(Math.max(finalFlow.doc, finalFlow.body) <= finalFlow.viewport + 1, width + ': overflow in MEMORY/CREATE flow');
    console.log('MEMBER_BROWSER_CREATE_FLOW_' + width + '=PASS');
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
