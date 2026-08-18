// 🎖 البروفة الشاملة: كل المسارات والطلبات والوكلاء دفعة واحدة
// دورة لوحة كاملة · طلب تطبيق بمتجر · رفض ⟵ التالي · طابور ⟵ أولوية · SOS ⟵ P0 ·
// إسناد يدوي · تتبع الزبون · الوكلاء ينبضون · الرموز تُرفض
import WebSocket from 'ws';
const BASE = 'http://localhost:' + (process.env.DP || '8080');
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
let fails = 0, n = 0;
const check = (name, ok, extra) => { n++; console.log((ok ? '  ✓ ' : '  ✗ ') + name + (extra ? ` (${extra})` : '')); if (!ok) fails++; };
const api = (path, opts) => fetch(BASE + path, opts).then(r => r.json().then(j => ({ st: r.status, j })).catch(() => ({ st: r.status, j: {} })));

// ===== غرفة العمليات (تستمع للبث) =====
const ordersSeen = new Map(), prios = new Map(), talya = [], announces = [];
const ops = new WebSocket(BASE.replace('http', 'ws') + '/ws');
ops.on('open', () => ops.send(JSON.stringify({ t: 'hello', role: 'ops', pin: '1234', name: 'محمد — غرفة العمليات' })));
ops.on('message', (raw) => { const m = JSON.parse(raw);
  if (m.t === 'order_upd') ordersSeen.set(m.order.id, m.order);
  if (m.t === 'priority') prios.set(m.p.id, m.p);
  if (m.t === 'snapshot') (m.priorities || []).forEach(p => prios.set(p.id, p));
  if (m.t === 'talya') talya.push(m.text); });
// مراقب ثانٍ: البث يستثني المرسِل — فنتحقق من وصول الإعلان لغيره
const obs = new WebSocket(BASE.replace('http', 'ws') + '/ws');
obs.on('open', () => obs.send(JSON.stringify({ t: 'hello', role: 'ops', pin: '1234', name: 'مراقب' })));
obs.on('message', (raw) => { const m = JSON.parse(raw); if (m.t === 'announce') announces.push(m.text); });

// ===== موصلون وهميون =====
let devSeq = 0;
const asciiId = () => 'dev-sim-' + (++devSeq);                     // معرّف ASCII كما تولّده أجهزة السائقين الحقيقية
function mkDriver(id, lat, lng, mode /* accept|reject|ignore */) {
  const w = new WebSocket(BASE.replace('http', 'ws') + '/ws');
  const devId = asciiId();
  w._offers = []; w._id = id; w._devId = devId;
  w.on('open', () => w.send(JSON.stringify({ t: 'hello', role: 'driver', pin: '1234', deviceId: devId, name: id, device: 'sim' })));
  w.on('message', (raw) => { const m = JSON.parse(raw);
    if (m.t === 'ok') w.send(JSON.stringify({ t: 'gps', lat, lng, acc: 5 }));
    if (m.t === 'offer') { w._offers.push(m.order.id);
      if (mode === 'accept') w.send(JSON.stringify({ t: 'offer_answer', orderId: m.order.id, accept: true }));
      if (mode === 'reject') w.send(JSON.stringify({ t: 'offer_answer', orderId: m.order.id, accept: false })); } });
  w.setStatus = (orderId, status) => w.send(JSON.stringify({ t: 'order_status', orderId, status }));
  return w;
}

console.log('— 1) رموز خاطئة تُرفض في كل بوابة —');
const badWs = new WebSocket(BASE.replace('http', 'ws') + '/ws');
let badClosed = false; badWs.on('close', () => badClosed = true);
badWs.on('open', () => badWs.send(JSON.stringify({ t: 'hello', role: 'ops', pin: '9999', name: 'دخيل' })));
const b1 = await api('/api/brain/summary', { headers: { 'x-kiosk-pin': '9999' } });
const b2 = await api('/api/brain/memory');
const b3 = await api('/api/v1/orders', { headers: { 'x-api-key': 'wrong' } });
await sleep(800);
check('WS برمز خاطئ يُغلق', badClosed);
check('summary/memory/v1 كلها 401 بلا رمز صحيح', b1.st === 401 && b2.st === 401 && b3.st === 401);

console.log('— 2) الدورة الكاملة من اللوحة: إنشاء ⟵ إسناد للأقرب ⟵ التقاط ⟵ تسليم —');
const قريب = mkDriver('قريب', 32.938, 35.271, 'accept');
const بعيد = mkDriver('بعيد', 32.905, 35.300, 'accept');
const رافض = mkDriver('رافض', 32.939, 35.272, 'reject');
await sleep(1200);
ops.send(JSON.stringify({ t: 'order_create', title: 'بروفة: فلافل البعنة ← دير الأسد', dest: { lat: 32.935, lng: 35.27 } }));
await sleep(3500);
let o1 = [...ordersSeen.values()].find(o => o.title.includes('فلافل'));
check('أُنشئ وأُسند تلقائياً', o1 && o1.status === 'assigned', o1?.driverName);
check('ذهب لأحد القريبَين لا البعيد', o1 && o1.driverName !== 'بعيد', o1?.driverName);
const winner = o1?.driverName === 'قريب' ? قريب : رافض._offers.includes(o1?.id) ? null : قريب;
قريب.setStatus(o1.id, 'picked'); await sleep(700);
o1 = ordersSeen.get(o1.id);
const pickedOk = o1.status === 'picked';
قريب.setStatus(o1.id, 'delivered'); await sleep(700);
o1 = ordersSeen.get(o1.id);
check('التقاط ثم تسليم بالسجل الكامل', pickedOk && o1.status === 'delivered' && (o1.history || []).length >= 3,
  (o1.history || []).map(h => h.st).join('←'));

console.log('— 3) طلب تطبيق بمرجع ومتجر (رحلة بساقين) + تتبع الزبون —');
ops.send(JSON.stringify({ t: 'store_add', name: 'مطعم البروفة', lat: 32.94, lng: 35.268 }));
await sleep(400);
const d1 = await api('/api/v1/dispatch', { method: 'POST', headers: { 'x-api-key': 'dyar-brain-key', 'content-type': 'application/json' },
  body: JSON.stringify({ ref: '7001', title: 'طلب 7001 — مطعم البروفة ← كرمئيل', origin: { lat: 32.94, lng: 35.268 }, dest: { lat: 32.921, lng: 35.29 } }) });
check('جسر التطبيق قَبِل الطلب بالمرجع', d1.st === 200 && d1.j.ok);
await sleep(3000);
const o2 = [...ordersSeen.values()].find(o => o.ref === '7001');
check("أُسند طلب التطبيق تلقائياً", o2 && ["assigned","picked"].includes(o2.status), (o2?"st="+o2.status+" drv="+o2.driverName:"لا طلب 7001 في الغرفة"));
const tr = await api('/api/track?ref=7001');
check('تتبع الزبون العام يعمل بالمرجع', tr.st === 200 && tr.j.order?.stage, tr.j.order?.stage);
check('التتبع لا يسرّب: لا هاتف موصل ولا إحداثياته', !JSON.stringify(tr.j).match(/lat|lng|phone|deviceId/));
const trAsk = await api('/api/track/ask', { method: 'POST', headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ q: 'وين طلبي 7001؟' }) });
check('بوت التتبع يجيب عن رقم الطلب', trAsk.st === 200 && String(trAsk.j.answer || '').length > 5);

console.log('— 4) لا موصل متاح ⟵ طابور ⟵ أولوية مفتوحة ⟵ استلام وحل من الغرفة —');
[قريب, بعيد, رافض].forEach(w => w.terminate());          // الكل ينقطع
await sleep(2500);
ops.send(JSON.stringify({ t: 'order_create', title: 'بروفة: طلب بلا موصلين', dest: { lat: 32.93, lng: 35.26 } }));
await sleep(14_000);                                      // كنس الراصد كل 10ث يفتح أولوية طابور
const qPr = [...prios.values()].find(p => ['queue', 'stale_order', 'no_drivers'].includes(p.type) || /طابور|بلا موصل|ينتظر/.test(p.title));
check('فُتحت أولوية للطلب العالق تلقائياً', Boolean(qPr), qPr?.title?.slice(0, 40));
if (qPr) {
  ops.send(JSON.stringify({ t: 'priority_action', id: qPr.id, action: 'ack', who: 'محمد' }));
  await sleep(700);
  const acked = prios.get(qPr.id);
  check('الاستلام سُجّل (ackAt + الحالة acknowledged)', acked?.ackAt && acked?.status === 'acknowledged', acked?.assignedTo || acked?.status);
} else check('الاستلام سُجّل (ackAt + الحالة acknowledged)', false, 'لا أولوية');

console.log('— 5) عودة موصل ⟵ إسناد يدوي من الغرفة + SOS ⟵ P0 فوري —');
const عائد = mkDriver('عائد', 32.93, 35.262, 'ignore');
await sleep(1500);
const oQ = [...ordersSeen.values()].find(o => o.title.includes('بلا موصلين'));
ops.send(JSON.stringify({ t: 'order_assign', orderId: oQ?.id, driverId: عائد._devId, who: 'محمد' }));
await sleep(900);
check('الإسناد اليدوي من الغرفة يعمل', ordersSeen.get(oQ?.id)?.driverName === 'عائد');
عائد.send(JSON.stringify({ t: 'sos' }));
await sleep(1200);
const sosPr = [...prios.values()].find(p => p.severity === 'P0' && /طوارئ|استغاثه|استغاثة|SOS/i.test(p.title + p.type));
check('SOS فتح أولوية P0 فوراً', Boolean(sosPr), sosPr?.title?.slice(0, 40));

console.log('— 6) الوكلاء نبضوا فعلاً أثناء البروفة + الذاكرة والإعلان —');
const sum = await api('/api/brain/summary', { headers: { 'x-kiosk-pin': '1234' } });
const agents = Object.keys(sum.j.agents || {});
check('تاليا الموزعة نبضت', agents.some(a => a.includes('تاليا')));
check('الراصد نبض', agents.some(a => a.includes('الراصد')));
check('سجل النشاط الحي ممتلئ', (sum.j.feed || []).length >= 5, (sum.j.feed || []).length + ' نبضة');
check('حضور المتجر الجديد في الملخص/الذاكرة', (await api('/api/brain/memory', { headers: { 'x-kiosk-pin': '1234' } })).j.stores?.some(s => s.name === 'مطعم البروفة'));
ops.send(JSON.stringify({ t: 'announce', text: 'بروفة إعلان الغرفة' }));
await sleep(700);
check('إعلان الغرفة وصل لبقية الأجهزة', announces.some(t => t.includes('بروفة إعلان')));
const ask = await api('/api/brain/ask', { method: 'POST', headers: { 'content-type': 'application/json' },
  body: JSON.stringify({ pin: '1234', q: 'مين الوكلاء وشو وظيفة كل وكيل؟' }) });
check('العقل يجيب عن الوكلاء بحالتهم الحية', /وكلاء ديار الالي|وكلاء ديار الآلي/.test(ask.j.answer || '') && /نشط/.test(ask.j.answer || ''));

console.log(`\n${fails === 0 ? '🎖 البروفة الشاملة نجحت' : '⚠ إخفاقات: ' + fails}/${n} فحص`);
process.exit(fails ? 1 : 0);
