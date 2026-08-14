// اختبار حِمل: يحاكي يومًا تشغيليًا مكثفًا — 300 موصل + 10,000 طلب عبر تاليا
// تشغيل:  node loadtest.js        (والخادم يعمل على 8080)
import WebSocket from 'ws';

const WS = process.env.WS || 'ws://localhost:8080/ws';
const PIN = process.env.DYAR_PIN || '1234';
const OPS_PIN = process.env.OPS_PIN || '9999';
const N_DRIVERS = Number(process.env.DRIVERS || 300);
const N_ORDERS = Number(process.env.ORDERS || 10000);
const AREA = { lat: 32.92, lng: 35.29, spread: 0.15 };

let assigned = 0, delivered = 0, escalated = 0, offersRecv = 0, errors = 0, created = 0;
const t0 = Date.now();
const drivers = [];
let memStart = 0;

const rnd = (c, s) => c + (Math.random() - 0.5) * s;

function makeDriver(i) {
  return new Promise((resolve) => {
    const ws = new WebSocket(WS);
    const d = { i, ws, lat: rnd(AREA.lat, AREA.spread), lng: rnd(AREA.lng, AREA.spread), order: null };
    ws.on('open', () => ws.send(JSON.stringify({ t: 'hello', role: 'driver', name: 'موصل ' + i, pin: PIN, deviceId: 'load-' + i, device: 'UR5' })));
    ws.on('message', (raw) => {
      let m; try { m = JSON.parse(raw); } catch { return; }
      if (m.t === 'ok') { ws.send(JSON.stringify({ t: 'gps', lat: d.lat, lng: d.lng, acc: 5, spd: 20, bat: 80 })); resolve(d); }
      else if (m.t === 'offer') {
        offersRecv++;
        // 80% يقبلون بسرعة، 20% يتجاهلون (تختبر الانتقال للتالي)
        if (Math.random() < 0.8) setTimeout(() => ws.send(JSON.stringify({ t: 'offer_answer', orderId: m.order.id, accept: true })), 30 + Math.random() * 100);
      }
      else if (m.t === 'order' && m.order) {
        d.order = m.order;
        if (m.order.status === 'assigned') setTimeout(() => ws.send(JSON.stringify({ t: 'order_status', status: 'picked', orderId: m.order.id })), 40);
        else if (m.order.status === 'picked') setTimeout(() => ws.send(JSON.stringify({ t: 'order_status', status: 'delivered', orderId: m.order.id })), 40);
      }
      else if (m.t === 'order' && m.order === null) { delivered++; d.order = null; }   // الطلب أُغلق
    });
    ws.on('error', () => { errors++; resolve(d); });
  });
}

function makeOps() {
  return new Promise((resolve) => {
    const ws = new WebSocket(WS);
    ws.on('open', () => ws.send(JSON.stringify({ t: 'hello', role: 'ops', pin: OPS_PIN })));
    ws.on('message', (raw) => {
      let m; try { m = JSON.parse(raw); } catch { return; }
      if (m.t === 'snapshot') resolve(ws);
      else if (m.t === 'orders') { /* اللوحة تستقبل التحديثات */ }
      else if (m.t === 'talya' && /🔴/.test(m.text)) escalated++;
    });
    ws.on('error', () => resolve(ws));
  });
}

// عدّاد نهائي عبر REST
async function poll() {
  try {
    const r = await fetch('http://localhost:8080/api/v1/orders', { headers: { 'x-api-key': process.env.BRAIN_API_KEY || 'dyar-brain-key' } });
    const j = await r.json();
    const st = {};
    for (const o of j.orders) st[o.status] = (st[o.status] || 0) + 1;
    return st;
  } catch { return {}; }
}

(async () => {
  memStart = process.memoryUsage().rss;
  console.log(`تشغيل ${N_DRIVERS} موصل…`);
  for (let i = 0; i < N_DRIVERS; i++) { drivers.push(await makeDriver(i)); if (i % 50 === 49) process.stdout.write(`  ${i + 1}\r`); }
  console.log(`\n${drivers.length} موصل متصل. تشغيل ${N_ORDERS} طلب…`);
  const ops = await makeOps();

  const tCreate = Date.now();
  const RATE = Number(process.env.RATE || 0);   // طلبات/ثانية (0 = أقصى سرعة)
  for (let n = 0; n < N_ORDERS; n++) {
    ops.send(JSON.stringify({ t: 'order_create', title: 'طلب حِمل ' + n, dest: { lat: rnd(AREA.lat, AREA.spread), lng: rnd(AREA.lng, AREA.spread) } }));
    created++;
    if (RATE) { if (n % Math.max(1, Math.round(RATE / 20)) === 0) await new Promise(r => setTimeout(r, 50)); }
    else if (n % 100 === 99) await new Promise(r => setTimeout(r, 20));
  }
  const createMs = Date.now() - tCreate;
  console.log(`أُنشئت ${created} طلب في ${(createMs / 1000).toFixed(1)}s (${Math.round(created / (createMs / 1000))}/s). انتظار المعالجة…`);

  // انتظر التصريف الكامل: كل الطلبات النشطة تصل صفراً (لا new/assigned/picked متبقٍ)
  let st = {};
  for (let i = 0; i < 120; i++) {
    await new Promise(r => setTimeout(r, 2000));
    st = await poll();
    const inflight = (st.new || 0) + (st.assigned || 0) + (st.picked || 0);
    process.stdout.write(`  مُسلَّم:${delivered} · نشط متبقٍ:${inflight} (جديد ${st.new || 0}/مُسنَد ${st.assigned || 0}/استُلم ${st.picked || 0}) تصعيد:${escalated}  \r`);
    if (inflight === 0) break;                 // صُرف الطابور بالكامل
  }
  const memEnd = process.memoryUsage().rss;
  const totalMs = Date.now() - t0;

  console.log('\n\n═══════════ نتيجة اختبار الحِمل ═══════════');
  console.log(`الموصلون المتصلون:     ${drivers.length}/${N_DRIVERS}`);
  console.log(`الطلبات المُنشأة:       ${created}`);
  console.log(`عروض تاليا المُرسَلة:    ${offersRecv}`);
  console.log(`مُسلَّم (من الأجهزة):    ${delivered}`);
  console.log(`في المعالجة (متبقٍ):    مُسنَد ${st.assigned || 0} · استُلم ${st.picked || 0} · جديد ${st.new || 0}`);
  console.log(`تصعيدات تاليا:          ${escalated}`);
  console.log(`أخطاء اتصال:            ${errors}`);
  console.log(`معدل الإنشاء:           ${Math.round(created / (createMs / 1000))} طلب/ثانية`);
  console.log(`ذاكرة الاختبار:         ${(memStart / 1e6).toFixed(0)} → ${(memEnd / 1e6).toFixed(0)} MB`);
  console.log(`الزمن الكلي:            ${(totalMs / 1000).toFixed(1)}s`);
  console.log('═══════════════════════════════════════════');
  process.exit(0);
})();
