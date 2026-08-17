#!/usr/bin/env node
// 🚦 مُشغّل بوابة ديار — يُقلع الخادم، يفحص عقد الـAPI، ويلتقط لقطات للصفحات (بما فيها المحميّة برمز).
// التشغيل من مجلّد الوحدة (server/):  node .claude/skills/run-dyar-gateway/driver.mjs
// المتطلّبات:  playwright-core مثبّت (npm i --no-save playwright-core) + كروميوم مُسبق في /opt/pw-browsers.
// المتغيّرات (اختياريّة):  PORT (افتراضي 8399) · PIN (افتراضي 1234) · SHOTS (مجلّد اللقطات، افتراضي ./_shots)
import { spawn } from 'node:child_process';
import { mkdirSync } from 'node:fs';
import { setTimeout as sleep } from 'node:timers/promises';

const PORT = process.env.PORT || '8399';
const PIN = process.env.PIN || '1234';
const SHOTS = process.env.SHOTS || './_shots';
const BASE = `http://127.0.0.1:${PORT}`;
mkdirSync(SHOTS, { recursive: true });
let fails = 0;
const ck = (name, ok, extra) => { if (!ok) fails++; console.log((ok ? '  ✓ ' : '  ✗ ') + name + (extra ? ` [${extra}]` : '')); };

// 1) أقلِع الخادم بإعداد تجريبيّ آمن (رمز موحّد، بلا مفاتيح خارجيّة)
console.log('🚀 إقلاع الخادم على المنفذ ' + PORT + ' …');
const srv = spawn('node', ['src/server.js'], {
  env: { ...process.env, PORT, OPS_PIN: PIN, DYAR_PIN: PIN, BRAIN_API_KEY: 'test-key', PIN_MAX_PER_MIN: '100000',
    BRAIN_PANEL_URL: '', BRAIN_WEBHOOK_URL: '', ANTHROPIC_API_KEY: '' },
  stdio: ['ignore', 'pipe', 'pipe'],
});
srv.stdout.on('data', () => {});
srv.stderr.on('data', d => process.stderr.write('[srv] ' + d));
const stop = () => { try { srv.kill('SIGKILL'); } catch {} };
process.on('exit', stop);

// انتظر جاهزيّة /api/health
let up = false;
for (let i = 0; i < 40; i++) {
  try { const r = await fetch(BASE + '/api/health'); if (r.ok) { up = true; break; } } catch {}
  await sleep(300);
}
if (!up) { console.error('✗ الخادم لم يُقلع — راجع مخرجات [srv] أعلاه'); stop(); process.exit(1); }

// 2) فحص عقد الـAPI (المصادقة + الحمولات العلنيّة + منع الاجتياز)
console.log('\n🔒 فحص عقد الـAPI:');
const health = await (await fetch(BASE + '/api/health')).json();
ck('GET /api/health = 200 ويكشف وسم البناء', !!health.v, 'v=' + health.v);
ck('لا يكشف قيمة أي رمز', !JSON.stringify(health).includes(PIN));
ck('GET /api/brain/summary بلا رمز = 401', (await fetch(BASE + '/api/brain/summary')).status === 401);
ck('GET /api/brain/summary برمز صحيح = 200', (await fetch(BASE + '/api/brain/summary', { headers: { 'x-kiosk-pin': PIN } })).status === 200);
ck('GET /api/track?ref=مجهول = 404', (await fetch(BASE + '/api/track?ref=999999')).status === 404);
ck('POST /api/health (طريقة خاطئة) = 404', (await fetch(BASE + '/api/health', { method: 'POST' })).status === 404);
ck('اجتياز المسار محجوب', (await fetch(BASE + '/../src/server.js')).status === 404);
const ask = await (await fetch(BASE + '/api/track/ask', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ q: 'شو مناطق التغطية؟' }) })).json();
ck('عقل العملاء (سارة) يجيب من قاعدة المعرفة', /البعنة|كرمئيل|نغط/.test(ask.answer || ''), (ask.answer || '').slice(0, 40));

// 3) لقطات الصفحات (playwright-core + كروميوم مُسبق) — بما فيها المحميّة برمز
console.log('\n📸 لقطات الصفحات → ' + SHOTS + ':');
let chromium;
try { ({ chromium } = await import('playwright-core')); }
catch { console.error('✗ playwright-core غير مثبّت. نفّذ: npm i --no-save playwright-core'); stop(); process.exit(1); }
// كروميوم مُسبق في /opt/pw-browsers — نوجّه إليه صراحةً (قد لا تطابق نسخته نسخة playwright-core)
const { existsSync, readdirSync } = await import('node:fs');
function findChrome() {
  if (process.env.PLAYWRIGHT_CHROMIUM && existsSync(process.env.PLAYWRIGHT_CHROMIUM)) return process.env.PLAYWRIGHT_CHROMIUM;
  const root = process.env.PLAYWRIGHT_BROWSERS_PATH || '/opt/pw-browsers';
  try { for (const d of readdirSync(root)) if (/^chromium-\d+$/.test(d)) {
    const p = `${root}/${d}/chrome-linux/chrome`; if (existsSync(p)) return p;
  } } catch {}
  return undefined;   // ندع playwright-core يحاول التنزيل/الاكتشاف
}
const exe = findChrome();
const browser = await chromium.launch(exe ? { executablePath: exe } : {});
const shot = async (name, path, prep) => {
  const pg = await browser.newPage({ viewport: { width: 1280, height: 800 } });
  const errs = [];
  pg.on('pageerror', e => errs.push(e.message));
  await pg.goto(BASE + path, { waitUntil: 'domcontentloaded' });
  if (prep) { await prep(pg); }
  await pg.waitForTimeout(2200);
  await pg.screenshot({ path: `${SHOTS}/${name}.png` });
  await pg.close();
  return errs;
};
// صفحة العميل العلنيّة (بلا رمز)
let e = await shot('track', '/track.html');
ck('لقطة track.html بلا أخطاء JS', e.length === 0, e[0] || '');
// اللوحة والشاشة محميّتان ببوّابة رمز — نحقن الرمز في التخزين ثم نعيد التحميل
e = await shot('dashboard', '/dashboard.html', async pg => { await pg.evaluate(p => sessionStorage.setItem('pin', p), PIN); await pg.reload({ waitUntil: 'domcontentloaded' }); });
ck('لقطة dashboard.html (بعد حقن الرمز)', e.filter(x => !/maplibre|Failed to fetch/.test(x)).length === 0, e[0] || '');
e = await shot('kiosk', '/kiosk.html', async pg => { await pg.evaluate(p => localStorage.setItem('kiosk_pin', p), PIN); await pg.reload({ waitUntil: 'domcontentloaded' }); });
ck('لقطة kiosk.html (بعد حقن الرمز)', e.length === 0, e[0] || '');

await browser.close();
stop();
console.log(`\n${fails === 0 ? '🟢 كل الفحوص نجحت' : '🔴 إخفاقات: ' + fails}. اللقطات في ${SHOTS}/`);
process.exit(fails ? 1 : 0);
