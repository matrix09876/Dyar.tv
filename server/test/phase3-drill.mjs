// 🌟 بروفة المرحلة ٣: سجلّ الزبائن (عائد) + تقييم الزبون (+أولوية للتقييم المنخفض) + أداء الموصّل + تصدير/استيراد
import WebSocket from 'ws';
const P = process.env.DP || '8481';
const BASE = 'http://127.0.0.1:' + P;
const PIN = '1234', KEY = 'dyar-brain-key';
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
let n = 0, fails = 0;
const ck = (name, ok, x) => { n++; if (!ok) fails++; console.log((ok ? '  ✓ ' : '  ✗ ') + name + (x !== undefined ? ` [${String(x).slice(0, 90)}]` : '')); };
const api = (path, opts) => fetch(BASE + path, opts).then(r => r.json().then(j => ({ st: r.status, j })).catch(() => ({ st: r.status, j: {} })));
const post = (path, body, headers = {}) => api(path, { method: 'POST', headers: { 'content-type': 'application/json', ...headers }, body: JSON.stringify(body) });

// موصل وهمي يقبل فوراً
const drv = new WebSocket(BASE.replace('http', 'ws') + '/ws');
drv.on('open', () => drv.send(JSON.stringify({ t: 'hello', role: 'driver', pin: PIN, deviceId: 'dev-p3', name: 'كريم', device: 'sim' })));
drv.on('message', (raw) => { const m = JSON.parse(raw);
  if (m.t === 'ok') drv.send(JSON.stringify({ t: 'gps', lat: 32.938, lng: 35.271, acc: 5 }));
  if (m.t === 'offer') drv.send(JSON.stringify({ t: 'offer_answer', orderId: m.order.id, accept: true })); });
await sleep(1200);

const mkOrder = async (ref) => {
  const d = await post('/api/v1/dispatch', { ref, title: `طلب ${ref} — مشاوي ← البعنة`, dest: { lat: 32.935, lng: 35.27 },
    price: 100, fee: 20, phone: '0521112233' }, { 'x-api-key': KEY });
  await sleep(2500);
  drv.send(JSON.stringify({ t: 'order_status', orderId: d.j.order.id, status: 'picked' })); await sleep(500);
  drv.send(JSON.stringify({ t: 'order_status', orderId: d.j.order.id, status: 'delivered' })); await sleep(700);
  return d;
};

console.log('— 1) سجلّ الزبائن: أول طلب يُسجَّل، والثاني يُوسم «عائد» —');
const d1 = await mkOrder('8901');
ck('الطلب الأول بلا وسم عائد', d1.j.order.returning === undefined, d1.j.order.returning);
const d2 = await mkOrder('8902');
ck('الطلب الثاني موسوم عائداً (1 طلب سابق)', d2.j.order.returning === 1, d2.j.order.returning);
const cu = await api('/api/brain/customers', { headers: { 'x-kiosk-pin': PIN } });
ck('customers محمي: بلا رمز = 401', (await api('/api/brain/customers')).st === 401);
ck('زبون واحد معروف وهو عائد', cu.j.total === 1 && cu.j.returning === 1, JSON.stringify({ t: cu.j.total, r: cu.j.returning }));
ck('سجلّه: طلبان وتسليمان', cu.j.top[0]?.orders === 2 && cu.j.top[0]?.delivered === 2, JSON.stringify(cu.j.top[0]));

console.log('— 2) تقييم الزبون: 5★ يُقبل، التكرار لا يزدوج، و1★ يفتح أولوية خدمة —');
const r1 = await post('/api/track/rate', { ref: '8901', stars: 5 });
ck('تقييم 5★ قُبل', r1.st === 200 && r1.j.ok && r1.j.stars === 5, JSON.stringify(r1.j));
const r1b = already => already; const r2 = await post('/api/track/rate', { ref: '8901', stars: 1 });
ck('إعادة تقييم نفس الطلب = already (لا ازدواج)', r2.j.already === true, JSON.stringify(r2.j));
ck('تقييم بلا نجوم صالحة = 400', (await post('/api/track/rate', { ref: '8902', stars: 9 })).st === 400);
ck('تقييم طلب غير مُسلَّم = 404', (await post('/api/track/rate', { ref: '999777', stars: 5 })).st === 404);
const r3 = await post('/api/track/rate', { ref: '8902', stars: 1 });
ck('تقييم 1★ قُبل', r3.st === 200 && r3.j.ok);
await sleep(600);
const pr = await api('/api/brain/priorities', { headers: { 'x-kiosk-pin': PIN } });
ck('التقييم المنخفض فتح أولوية خدمة', (pr.j.open || []).some(p => p.type === 'low_rating' && /8902/.test(p.title)),
  (pr.j.open || []).map(p => p.type).join(','));

console.log('— 3) أداء الموصّل: عروض وقبول وتقييم —');
const pf = await api('/api/brain/performance', { headers: { 'x-kiosk-pin': PIN } });
ck('performance محمي: بلا رمز = 401', (await api('/api/brain/performance')).st === 401);
const kp = (pf.j.drivers || []).find(d => d.name === 'كريم');
ck('كريم: عرضان وقبولان (قبول 100٪)', kp && kp.offers === 2 && kp.accepted === 2 && kp.acceptPct === 100, JSON.stringify(kp));
ck('متوسط تقييم كريم = 3 (5★+1★)', kp && kp.rating === 3 && kp.ratings === 2, kp?.rating);
const an = await api('/api/brain/analytics?days=3', { headers: { 'x-kiosk-pin': PIN } });
ck('التحليلات تحمل تقييم الموصّل ونسبة قبوله', (an.j.drivers || []).some(d => d.driver === 'كريم' && d.rating === 3 && d.acceptPct === 100));

console.log('— 4) التصدير المستقل: محمي، وينزل نسخة تحوي كل شيء —');
ck('export بلا رمز = 401', (await fetch(BASE + '/api/brain/export')).status === 401);
const exR = await fetch(BASE + '/api/brain/export', { headers: { 'x-kiosk-pin': PIN } });
const exJ = await exR.json();
ck('التصدير ينجح بترويسة تنزيل', exR.status === 200 && /attachment/.test(exR.headers.get('content-disposition') || ''));
ck('النسخة تحوي الزبائن والتقييمات والدفتر والأداء',
  exJ.backup && exJ.backup.customers?.length === 1 && exJ.backup.ratings?.length === 2 && exJ.backup.ledger?.length === 2 && exJ.backup.perf?.length >= 1);
ck('import بلا رمز = 401', (await post('/api/brain/import', { backup: {} })).st === 401);
const im = await post('/api/brain/import', { pin: PIN, backup: { faq: [{ n: 999, text: 'معلومة مستوردة للاختبار', at: Date.now() }] } });
ck('الاستيراد يملأ الفارغ فقط ويستجيب بالأحجام', im.st === 200 && im.j.ok && im.j.customers === 1, JSON.stringify(im.j));

console.log('— 5) الخصوصية: التتبع العلني بلا هاتف حتى بعد كل ذلك —');
const tr = await api('/api/track?ref=8902');
ck('لا هاتف في الحمولة العلنية', !/0521112233|phone/.test(JSON.stringify(tr.j)));

drv.terminate();
console.log(`\n${fails === 0 ? '🟢 المرحلة ٣ تعمل كاملة' : '🔴 إخفاقات'}: ${n - fails}/${n}`);
process.exit(fails ? 1 : 0);
