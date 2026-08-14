// Dyar Connect — Realtime Gateway
// حضور + GPS + بث صوتي PTT + طوارئ + جسر لوحة العقل (غرفة التشغيل)
//
// تشغيل:            npm start                        (منفذ 8080 افتراضيًا)
// متغيرات البيئة:   PORT=8080  DYAR_PIN=1234
//                   TLS_CERT=cert.pem TLS_KEY=key.pem     ← HTTPS (مطلوب لـGPS والمايك من الأجهزة)
//                   BRAIN_API_KEY=...                      ← مفتاح REST للوحة العقل
//                   BRAIN_WEBHOOK_URL=https://.../hook     ← دفع الأحداث للوحة العقل
//                   BRAIN_PANEL_URL=https://egint-support.onrender.com

import { createServer as createHttp } from 'node:http';
import { createServer as createHttps } from 'node:https';
import { readFileSync, existsSync } from 'node:fs';
import { networkInterfaces } from 'node:os';
import { extname, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';
import { WebSocketServer } from 'ws';

import { timingSafeEqual } from 'node:crypto';

const PORT = Number(process.env.PORT || 8080);
const PIN = process.env.DYAR_PIN || '1234';
const OPS_PIN = process.env.OPS_PIN || PIN;   // 🛡️ رمز غرفة العمليات منفصل — اضبطه في الإنتاج حتى لا يدخل موصل كمشرف
const pinOk = (got, want) => { const a = Buffer.from(String(got || '')), b = Buffer.from(String(want));
  return a.length === b.length && timingSafeEqual(a, b); };
const BRAIN_API_KEY = process.env.BRAIN_API_KEY || 'dyar-brain-key';
const BRAIN_WEBHOOK_URL = process.env.BRAIN_WEBHOOK_URL || '';
const BRAIN_PANEL_URL = process.env.BRAIN_PANEL_URL || 'https://egint-support.onrender.com';
const PUB = fileURLToPath(new URL('../public', import.meta.url));

// ---------- الحالة (MVP بالذاكرة — الإنتاج: PostgreSQL + Redis) ----------
/** deviceId -> {id, name, device, online, sos, lastSeen, last:{...}, trail:[[lng,lat]]} */
const drivers = new Map();
const opsClients = new Set();
const driverClients = new Map();          // ws -> driverId
const voiceArchive = [];                  // آخر 50 بثًا {from, role, ts, mime, data, dur}

// ---------- الطلبات: مسار مُدار (جديد ← مُسنَد ← استُلم ← سُلِّم) ----------
/** id -> {id, title, dest:{lat,lng}, driverId, driverName, status, createdAt, updatedAt} */
const orders = new Map();
let orderSeq = 18300;                     // أرقام قريبة من الواقع للعرض
const ORDER_STATUSES = new Set(['new', 'assigned', 'picked', 'delivered', 'cancelled']);
const orderPublic = ({ _offer, ...rest }) => rest;               // لا يُبث المؤقّت الداخلي أبداً
const ordersList = () => [...orders.values()].sort((a, b) => b.createdAt - a.createdAt).slice(0, 100).map(orderPublic);
const activeOrders = () => ordersList().filter(o => !['delivered', 'cancelled'].includes(o.status));
const driverWs = (id) => { for (const [ws, did] of driverClients) if (did === id) return ws; };
const driverActiveOrderRaw = (id) => [...orders.values()]
  .find(o => o.driverId === id && !['delivered', 'cancelled'].includes(o.status)) || null;
const driverActiveOrder = (id) => { const o = driverActiveOrderRaw(id); return o ? orderPublic(o) : null; };

const publicInfo = (d) => ({
  id: d.id, name: d.name, device: d.device, online: d.online, sos: !!d.sos,
  lastSeen: d.lastSeen, last: d.last, trail: d.trail,
});
const send = (ws, msg) => { if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg)); };
const broadcastOps = (msg) => { for (const ws of opsClients) send(ws, msg); };
const broadcastDrivers = (msg, except) => { for (const ws of driverClients.keys()) if (ws !== except) send(ws, msg); };
const broadcastAll = (msg, except) => { broadcastOps(msg); broadcastDrivers(msg, except); };
const onlineCount = () => [...drivers.values()].filter(d => d.online).length;
const pushStats = () => broadcastDrivers({ t: 'stats', online: onlineCount(), channel: 'العمليات العامة' });
const pushOrders = () => broadcastOps({ t: 'orders', orders: ordersList() });
const pushDriverOrder = (driverId) => { const ws = driverWs(driverId); if (ws) send(ws, { t: 'order', order: driverActiveOrder(driverId) }); };

function setOrder(o, patch) {
  Object.assign(o, patch, { updatedAt: Date.now() });
  pushOrders();
  brainEvent('order_' + o.status, { order: { ...o, _offer: undefined } });
}

// ================= 🧕 تاليا — الموزعة الآلية =================
// عند إنشاء طلب: تاليا تعرضه على أقرب موصل صوتياً ونصياً، مهلة للرد، رفض/صمت ← التالي، ٣ محاولات ← تصعيد للعمليات.
const OFFER_TIMEOUT_MS = 25_000;
const MAX_OFFERS = 3;
const havKm = (a, b) => { const R = 6371, dLa = (b.lat - a.lat) * Math.PI/180, dLo = (b.lng - a.lng) * Math.PI/180;
  const x = Math.sin(dLa/2)**2 + Math.cos(a.lat*Math.PI/180)*Math.cos(b.lat*Math.PI/180)*Math.sin(dLo/2)**2;
  return R * 2 * Math.atan2(Math.sqrt(x), Math.sqrt(1-x)); };

function talyaSay(driverId, text, extra = {}) {
  const w = driverWs(driverId);
  if (w) send(w, { t: 'announce', from: 'تاليا — ديار', text, speak: true, ...extra });
}
const talyaFeed = (text) => broadcastOps({ t: 'talya', text, at: Date.now() });

function dispatchCandidates(o) {
  return [...drivers.values()]
    .filter(d => d.online && d.last && !d.sos)
    .map(d => ({ id: d.id, name: d.name, km: havKm(d.last, o.dest), busy: !!driverActiveOrder(d.id) }))
    .sort((a, b) => (a.busy - b.busy) || (a.km - b.km))          // الحر الأقرب أولاً
    .slice(0, MAX_OFFERS);
}

function startDispatch(o) {
  if (o._offer) { clearTimeout(o._offer.timer); o._offer = null; }
  const cands = dispatchCandidates(o);
  if (!cands.length) {
    talyaFeed(`⚠️ ${o.id}: لا موصل متاحًا الآن — يحتاج إسنادًا يدويًا.`);
    brainEvent('dispatch_escalated', { order: { ...o, _offer: undefined }, reason: 'no_candidates' });
    return;
  }
  o._offer = { cands, idx: 0, timer: null };
  offerNext(o);
}

function offerNext(o) {
  const of = o._offer;
  if (!of || ['assigned', 'picked', 'delivered', 'cancelled'].includes(o.status)) return;
  if (of.idx >= of.cands.length) {                                // استُنفدت المحاولات — تصعيد بشري
    o._offer = null;
    setOrder(o, { offeredTo: null });
    talyaFeed(`🔴 ${o.id}: عرضتُه على ${of.cands.length} موصلين بلا قبول — أحتاج قرار العمليات.`);
    brainEvent('dispatch_escalated', { order: { ...o, _offer: undefined }, reason: 'all_declined' });
    return;
  }
  const c = of.cands[of.idx];
  setOrder(o, { offeredTo: c.name });
  const w = driverWs(c.id);
  if (!w) { of.idx++; return offerNext(o); }
  send(w, { t: 'offer', order: { id: o.id, title: o.title, dest: o.dest }, km: Math.round(c.km * 10) / 10, expiresInS: OFFER_TIMEOUT_MS / 1000 });
  talyaSay(c.id, `طلب جديد: ${o.title}. يبعد عنك ${c.km.toFixed(1)} كيلومتر. اضغط قبول خلال ${OFFER_TIMEOUT_MS / 1000} ثانية.`);
  talyaFeed(`🎙 ${o.id}: أعرضه الآن على ${c.name} (${c.km.toFixed(1)} كم)${of.idx ? ` — المحاولة ${of.idx + 1}` : ''}…`);
  of.timer = setTimeout(() => { of.idx++; offerNext(o); }, OFFER_TIMEOUT_MS);
}

function answerOffer(o, driverId, accept) {
  const of = o._offer;
  const c = of?.cands[of.idx];
  if (!c || c.id !== driverId || o.status !== 'new') return;      // ليس المعروض عليه حالياً
  clearTimeout(of.timer);
  if (accept) {
    o._offer = null;
    const d = drivers.get(driverId);
    setOrder(o, { driverId, driverName: d.name, status: 'assigned', offeredTo: null });
    pushDriverOrder(driverId);
    talyaSay(driverId, `تم، الطلب ${o.id} لك. بالسلامة.`);
    talyaFeed(`🟢 ${o.id}: قبله ${d.name} — أُسند.`);
  } else {
    talyaFeed(`⚪ ${o.id}: رفضه ${c.name}.`);
    of.idx++; offerNext(o);
  }
}

// ---------- جسر لوحة العقل: دفع الأحداث (fire-and-forget) ----------
function brainEvent(event, payload) {
  if (!BRAIN_WEBHOOK_URL) return;
  fetch(BRAIN_WEBHOOK_URL, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-api-key': BRAIN_API_KEY },
    body: JSON.stringify({ event, at: Date.now(), ...payload }),
  }).catch(() => {});
}

// ---------- HTTP: ملفات + REST للوحة العقل ----------
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.svg': 'image/svg+xml' };
const readBody = (req) => new Promise((res) => {
  let b = ''; req.on('data', c => { b += c; if (b.length > 1e6) req.destroy(); });
  req.on('end', () => { try { res(JSON.parse(b || '{}')); } catch { res({}); } });
});

async function handleHttp(req, res) {
  const url = new URL(req.url, 'http://x');
  const json = (code, obj) => { res.writeHead(code, { 'content-type': 'application/json', 'access-control-allow-origin': '*', 'access-control-allow-headers': 'content-type,x-api-key' }); res.end(JSON.stringify(obj)); };

  if (req.method === 'OPTIONS') return json(204, {});
  if (url.pathname === '/api/health')
    return json(200, { ok: true, drivers: drivers.size, online: onlineCount() });
  if (url.pathname === '/api/config')
    return json(200, { brainPanelUrl: BRAIN_PANEL_URL });

  // ===== REST للوحة العقل (مفتاح API) =====
  if (url.pathname.startsWith('/api/v1/')) {
    if (req.headers['x-api-key'] !== BRAIN_API_KEY) return json(401, { error: 'bad api key' });

    // مواقع وحالة كل الموصلين — تستهلكها صفحات الطلبات/المكالمات في غرفة التشغيل
    if (url.pathname === '/api/v1/drivers' && req.method === 'GET')
      return json(200, { drivers: [...drivers.values()].map(d => ({ ...publicInfo(d), trail: undefined })) });

    // الطلبات ومساراتها — لصفحة الطلبات في غرفة التشغيل
    if (url.pathname === '/api/v1/orders' && req.method === 'GET')
      return json(200, { orders: ordersList() });

    // إعلان من العقل إلى الأجهزة: {text, speak:true} — يظهر ويُنطق على جهاز الموصل
    if (url.pathname === '/api/v1/announce' && req.method === 'POST') {
      const b = await readBody(req);
      if (!b.text) return json(400, { error: 'text required' });
      broadcastAll({ t: 'announce', text: String(b.text).slice(0, 300), speak: b.speak !== false, from: 'غرفة التشغيل' });
      return json(200, { ok: true, delivered: onlineCount() });
    }
    return json(404, { error: 'unknown endpoint' });
  }

  let file = url.pathname === '/' ? '/dashboard.html' : url.pathname;
  file = normalize(file).replace(/^(\.\.[/\\])+/, '');
  const full = join(PUB, file);
  if (!full.startsWith(PUB) || !existsSync(full)) { res.writeHead(404); return res.end('not found'); }
  res.writeHead(200, { 'content-type': MIME[extname(full)] || 'application/octet-stream' });
  res.end(readFileSync(full));
}

// ---------- HTTP أو HTTPS ----------
const useTls = process.env.TLS_CERT && process.env.TLS_KEY && existsSync(process.env.TLS_CERT);
const server = useTls
  ? createHttps({ cert: readFileSync(process.env.TLS_CERT), key: readFileSync(process.env.TLS_KEY) }, handleHttp)
  : createHttp(handleHttp);

// ---------- رسائل مشتركة (صوت/نص/طوارئ) ----------
function handleShared(m, from, role, ws) {
  // بث صوتي PTT: يُرحَّل لكل الأطراف ويُؤرشف
  if (m.t === 'voice' && typeof m.data === 'string' && m.data.length < 2_000_000) {
    const entry = { from, role, ts: Date.now(), mime: String(m.mime || 'audio/webm'), data: m.data, dur: +m.dur || null };
    voiceArchive.push(entry); if (voiceArchive.length > 50) voiceArchive.shift();
    broadcastAll({ t: 'voice', ...entry }, ws);
    return true;
  }
  if (m.t === 'text' && m.text) {
    broadcastAll({ t: 'text', from, role, ts: Date.now(), text: String(m.text).slice(0, 500) }, ws);
    return true;
  }
  return false;
}

// ---------- WebSocket ----------
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 4 * 1024 * 1024 });

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });

  ws.once('message', (raw) => {
    let hello; try { hello = JSON.parse(raw); } catch { return ws.close(4000, 'bad json'); }
    if (hello.t !== 'hello') return ws.close(4000, 'bad hello');
    // 🛡️ رمزان منفصلان: OPS_PIN لغرفة العمليات، DYAR_PIN للأجهزة — مقارنة timing-safe
    if (!pinOk(hello.pin, hello.role === 'ops' ? OPS_PIN : PIN)) return ws.close(4001, 'bad pin');

    // ===== لوحة التحكم =====
    if (hello.role === 'ops') {
      opsClients.add(ws);
      send(ws, { t: 'snapshot', drivers: [...drivers.values()].map(publicInfo), orders: ordersList() });
      ws.on('message', (raw2) => {
        let m; try { m = JSON.parse(raw2); } catch { return; }
        if (handleShared(m, hello.name || 'العمليات', 'ops', ws)) return;
        if (m.t === 'sos_clear' && drivers.has(m.id)) {          // إغلاق تنبيه الطوارئ
          const d = drivers.get(m.id); d.sos = false;
          broadcastAll({ t: 'sos_clear', id: m.id });
          brainEvent('sos_cleared', { driver: publicInfo(d) });
          return;
        }
        if (m.t === 'announce' && m.text) {                      // إعلان من اللوحة — تبلغه تاليا للأجهزة
          broadcastAll({ t: 'announce', text: String(m.text).slice(0, 300), speak: m.speak !== false, from: 'تاليا — ديار' }, ws);
          return;
        }
        // ----- إدارة الأجهزة من اللوحة -----
        if (m.t === 'device_rename' && drivers.has(m.id) && m.name) {
          const d = drivers.get(m.id);
          d.name = String(m.name).slice(0, 40);
          broadcastOps({ t: 'driver', d: publicInfo(d) });
          const w = driverWs(m.id); if (w) send(w, { t: 'renamed', name: d.name });
          return;
        }
        if (m.t === 'device_kick' && drivers.has(m.id)) {         // فصل فوري (جهاز مفقود/مسروق)
          const w = driverWs(m.id);
          if (w) { send(w, { t: 'kicked' }); w.close(4003, 'kicked'); }
          console.log(`[×] فُصل الجهاز ${m.id} من اللوحة`);
          return;
        }
        if (m.t === 'device_remove' && drivers.has(m.id)) {       // حذف من السجل (غير متصل فقط)
          const d = drivers.get(m.id);
          if (!d.online) { drivers.delete(m.id); broadcastOps({ t: 'device_removed', id: m.id }); }
          return;
        }

        // ----- إدارة الطلبات -----
        if (m.t === 'order_create' && m.title && Number.isFinite(m.dest?.lat) && Number.isFinite(m.dest?.lng)
            && Math.abs(+m.dest.lat) <= 90 && Math.abs(+m.dest.lng) <= 180) {
          const o = { id: 'ORD-' + (++orderSeq), title: String(m.title).slice(0, 80),
            dest: { lat: +m.dest.lat, lng: +m.dest.lng }, driverId: null, driverName: null,
            status: 'new', offeredTo: null, createdAt: Date.now(), updatedAt: Date.now() };
          orders.set(o.id, o);
          setOrder(o, {});
          if (m.auto !== false) startDispatch(o);                 // 🧕 تاليا تعرضه على الأقرب تلقائياً
          console.log(`[o] طلب جديد ${o.id}: ${o.title}`);
          return;
        }
        const o = m.orderId && orders.get(m.orderId);
        const cancelOffer = (ord) => { if (!ord?._offer) return;
          clearTimeout(ord._offer.timer);
          const c = ord._offer.cands[ord._offer.idx];
          if (c) { const w = driverWs(c.id); if (w) send(w, { t: 'offer_cancel', orderId: ord.id }); }
          ord._offer = null; };
        if (m.t === 'order_assign' && o && drivers.has(m.driverId)) {
          cancelOffer(o);                                        // الإسناد اليدوي يوقف عرض تاليا الجاري
          const prev = o.driverId;
          const d = drivers.get(m.driverId);
          setOrder(o, { driverId: d.id, driverName: d.name, status: 'assigned', offeredTo: null });
          pushDriverOrder(d.id);
          if (prev && prev !== d.id) pushDriverOrder(prev);      // أبلغ الموصل السابق أن الطلب سُحب
          talyaSay(d.id, `أُسند إليك الطلب ${o.id}: ${o.title}.`);
          console.log(`[o] ${o.id} أُسند إلى ${d.name}`);
          return;
        }
        if (m.t === 'order_status' && o && ORDER_STATUSES.has(m.status)) {
          if (m.status === 'cancelled') cancelOffer(o);
          setOrder(o, { status: m.status, ...(m.status === 'cancelled' ? { offeredTo: null } : {}) });
          if (o.driverId) pushDriverOrder(o.driverId);
          return;
        }
        if (m.t === 'order_redispatch' && o && o.status === 'new') { startDispatch(o); return; }   // «أعيدي العرض يا تاليا»
      });
      ws.on('close', () => opsClients.delete(ws));
      return;
    }

    // ===== جهاز موصل =====
    if (hello.role === 'driver' && hello.deviceId && hello.name) {
      const id = String(hello.deviceId).slice(0, 64);
      const d = drivers.get(id) || { id, trail: [] };
      Object.assign(d, {
        name: String(hello.name).slice(0, 40),
        device: String(hello.device || 'غير معروف').slice(0, 60),
        online: true, lastSeen: Date.now(),
      });
      drivers.set(id, d);
      driverClients.set(ws, id);
      send(ws, { t: 'ok', id, channel: 'العمليات العامة', online: onlineCount(), order: driverActiveOrder(id) });
      broadcastOps({ t: 'driver', d: publicInfo(d) });
      brainEvent('driver_online', { driver: publicInfo(d) });
      pushStats();
      console.log(`[+] ${d.name} (${d.device}) متصل — الأجهزة: ${drivers.size}`);

      ws.on('message', (raw2) => {
        let m; try { m = JSON.parse(raw2); } catch { return; }
        if (handleShared(m, d.name, 'driver', ws)) return;

        if (m.t === 'gps' && Number.isFinite(m.lat) && Number.isFinite(m.lng)) {
          d.last = {
            lat: +m.lat, lng: +m.lng,
            acc: Number.isFinite(m.acc) ? Math.round(m.acc * 10) / 10 : null,
            spd: Number.isFinite(m.spd) ? Math.round(m.spd * 3.6) : null,
            hdg: Number.isFinite(m.hdg) ? Math.round(m.hdg) : null,
            bat: Number.isFinite(m.bat) ? Math.round(m.bat) : null,
            ts: Date.now(),
          };
          d.lastSeen = Date.now();
          const tail = d.trail[d.trail.length - 1];
          if (!tail || Math.abs(tail[0] - d.last.lng) > 1e-5 || Math.abs(tail[1] - d.last.lat) > 1e-5) {
            d.trail.push([d.last.lng, d.last.lat]);
            if (d.trail.length > 400) d.trail.shift();
          }
          broadcastOps({ t: 'gps', id, last: d.last, point: [d.last.lng, d.last.lat] });
        }

        if (m.t === 'sos') {                                     // زر الطوارئ
          d.sos = true;
          broadcastAll({ t: 'sos', id, name: d.name, last: d.last });
          brainEvent('sos', { driver: publicInfo(d) });
          console.log(`[!] طوارئ من ${d.name}`);
        }

        // الموصل يحدّث حالة طلبه الجاري: استلمت / سلّمت
        if (m.t === 'order_status' && ['picked', 'delivered'].includes(m.status)) {
          const o = driverActiveOrderRaw(id);
          if (o) { setOrder(o, { status: m.status }); pushDriverOrder(id); }
        }

        // رد الموصل على عرض تاليا: قبول أو رفض
        if (m.t === 'offer_answer' && m.orderId && orders.has(m.orderId)) {
          answerOffer(orders.get(m.orderId), id, m.accept === true);
        }
      });

      ws.on('close', () => {
        driverClients.delete(ws);
        d.online = false; d.lastSeen = Date.now();
        broadcastOps({ t: 'presence', id, online: false, lastSeen: d.lastSeen });
        brainEvent('driver_offline', { driver: publicInfo(d) });
        pushStats();
        console.log(`[-] ${d.name} انقطع`);
      });
      return;
    }

    ws.close(4002, 'bad hello');
  });
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false; ws.ping();
  }
}, 15_000);

// ---------- إقلاع ----------
server.listen(PORT, () => {
  const proto = useTls ? 'https' : 'http';
  const lans = Object.values(networkInterfaces()).flat()
    .filter(i => i && i.family === 'IPv4' && !i.internal).map(i => i.address);
  console.log('──────────────────────────────────────────────');
  console.log('  Dyar Connect Gateway يعمل ✓');
  console.log(`  لوحة التحكم:  ${proto}://localhost:${PORT}/`);
  for (const ip of lans) console.log(`  من الشبكة:    ${proto}://${ip}:${PORT}/`);
  console.log(`  صفحة السائق:  ${proto}://<العنوان>:${PORT}/driver.html`);
  console.log(`  رمز الأجهزة PIN: ${PIN}${OPS_PIN === PIN ? '  (⚠ اضبط OPS_PIN منفصلاً للوحة في الإنتاج)' : '  · رمز اللوحة OPS_PIN: مضبوط ✓'}`);
  console.log(`  لوحة العقل:    ${BRAIN_PANEL_URL}`);
  console.log(`  REST للعقل:    GET /api/v1/drivers · POST /api/v1/announce  (x-api-key)`);
  if (!useTls) console.log('  تنبيه: GPS والمايك من الأجهزة يتطلبان HTTPS — docs/quickstart.md');
  console.log('──────────────────────────────────────────────');
});
