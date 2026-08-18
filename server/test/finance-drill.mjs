// 💰 بروفة المحاسبة والديمومة: طلب بقيمة وأجرة وهاتف ⟵ تسليم ⟵ قيد بالدفتر ⟵ تسوية وتحليلات
// + خصوصية: الهاتف والمال لا يتسرّبان للتتبع العلني · + لقطة القرص تُكتب ويُستعاد منها
import WebSocket from 'ws';
import { existsSync, readFileSync } from 'node:fs';
const P = process.env.DP || '8471';
const BASE = 'http://127.0.0.1:' + P;
const PIN = '1234', KEY = 'dyar-brain-key';
const STATE_DIR = process.env.STATE_DIR || '';
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
let n = 0, fails = 0;
const ck = (name, ok, x) => { n++; if (!ok) fails++; console.log((ok ? '  ✓ ' : '  ✗ ') + name + (x !== undefined ? ` [${String(x).slice(0, 80)}]` : '')); };
const api = (path, opts) => fetch(BASE + path, opts).then(r => r.json().then(j => ({ st: r.status, j })).catch(() => ({ st: r.status, j: {} })));

// موصل وهمي يقبل فوراً
const drv = new WebSocket(BASE.replace('http', 'ws') + '/ws');
drv.on('open', () => drv.send(JSON.stringify({ t: 'hello', role: 'driver', pin: PIN, deviceId: 'dev-fin-1', name: 'حسن', device: 'sim' })));
drv.on('message', (raw) => { const m = JSON.parse(raw);
  if (m.t === 'ok') drv.send(JSON.stringify({ t: 'gps', lat: 32.938, lng: 35.271, acc: 5 }));
  if (m.t === 'offer') drv.send(JSON.stringify({ t: 'offer_answer', orderId: m.order.id, accept: true })); });
await sleep(1200);

console.log('— 1) طلب تطبيق بقيمة وأجرة وهاتف ⟵ يُسند ويُسلَّم —');
const d = await api('/api/v1/dispatch', { method: 'POST', headers: { 'x-api-key': KEY, 'content-type': 'application/json' },
  body: JSON.stringify({ ref: '8801', title: 'طلب 8801 — شاورما ← البعنة', dest: { lat: 32.935, lng: 35.27 },
    price: 145.5, fee: 20, phone: '0501112233' }) });
ck('قُبل الطلب بحقول المحاسبة', d.st === 200 && d.j.ok && d.j.order.price === 145.5 && d.j.order.fee === 20, JSON.stringify({ p: d.j.order?.price, f: d.j.order?.fee }));
ck('هاتف الزبون حُفظ على الطلب (للموصّل والإشعار)', d.j.order.phone === '0501112233', d.j.order?.phone);
await sleep(3000);
const oid = d.j.order.id;
drv.send(JSON.stringify({ t: 'order_status', orderId: oid, status: 'picked' })); await sleep(600);
drv.send(JSON.stringify({ t: 'order_status', orderId: oid, status: 'delivered' })); await sleep(900);

console.log('— 2) الخصوصية: التتبع العلني لا يسرّب هاتفاً ولا مالاً —');
const tr = await api('/api/track?ref=8801');
ck('التتبع يعمل بعد التسليم', tr.st === 200 && tr.j.order?.status === 'delivered', tr.j.order?.status);
ck('لا phone/price/fee في الحمولة العلنية', !/phone|price|fee|0501112233/.test(JSON.stringify(tr.j)));

console.log('— 3) التسوية: القيد ظهر بالدفتر بالمبالغ الصحيحة —');
const f = await api('/api/brain/finance', { headers: { 'x-kiosk-pin': PIN } });
ck('finance محمي: بلا رمز = 401', (await api('/api/brain/finance')).st === 401);
ck('توصيلة واحدة مقيّدة اليوم', f.st === 200 && f.j.delivered === 1, f.j.delivered);
ck('أجور اليوم = 20', f.j.fees === 20, f.j.fees);
ck('المحصَّل من الزبائن = 145.5', f.j.collected === 145.5, f.j.collected);
const row = (f.j.drivers || [])[0];
ck('صف الموصّل حسن: مستحقّه 16 (٨٠٪ من الأجرة)', row && row.driver === 'حسن' && row.due === 16, JSON.stringify(row));
ck('يسلّم للمكتب 149.5 (المحصَّل + حصة الشركة 4)', row && row.handover === 149.5, row?.handover);

console.log('— 4) التحليلات: الاتجاه وأداء الموصّلين من قيود حقيقية —');
const a = await api('/api/brain/analytics?days=7', { headers: { 'x-kiosk-pin': PIN } });
ck('analytics محمي: بلا رمز = 401', (await api('/api/brain/analytics')).st === 401);
ck('إجمالي التسليم بالفترة = 1', a.st === 200 && a.j.totals?.delivered === 1, a.j.totals?.delivered);
ck('أداء الموصّل حسن ظاهر', (a.j.drivers || []).some(x => x.driver === 'حسن' && x.delivered === 1));
ck('اتجاه اليوم يحمل الأجور', (a.j.trend || []).some(t => t.fees === 20));

console.log('— 5) الديمومة: لقطة القرص كُتبت وتحوي الدفتر —');
await sleep(6000);                                   // ⚡ النسخ السريع المؤجَّل (٥ ثوانٍ) بعد التسليم
if (STATE_DIR) {
  const fpath = STATE_DIR + '/state.json';
  ck('ملف اللقطة موجود', existsSync(fpath), fpath);
  const snap = existsSync(fpath) ? JSON.parse(readFileSync(fpath, 'utf8')) : null;
  ck('اللقطة تحوي قيد المحاسبة', !!snap?.backup?.ledger?.some(e => e.ref === '8801' && e.fee === 20));
} else { ck('ملف اللقطة موجود', false, 'STATE_DIR غير مضبوط للاختبار'); ck('اللقطة تحوي قيد المحاسبة', false); }

drv.terminate();
console.log(`\n${fails === 0 ? '🟢 المحاسبة والديمومة تعملان' : '🔴 إخفاقات'}: ${n - fails}/${n}`);
process.exit(fails ? 1 : 0);
