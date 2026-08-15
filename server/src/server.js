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
const BRAIN_PANEL_URL = process.env.BRAIN_PANEL_URL || 'https://egint-support.onrender.com';
const BRAIN_WEBHOOK_URL = process.env.BRAIN_WEBHOOK_URL
  || (BRAIN_PANEL_URL ? BRAIN_PANEL_URL.replace(/\/+$/, '') + '/webhooks/dyar-connect' : '');
// عنوان هذه الخدمة العلني — Render يوفره تلقائياً (RENDER_EXTERNAL_URL) — لازم للتسجيل الذاتي لدى غرفة التشغيل
const SELF_URL = (process.env.PUBLIC_URL || process.env.RENDER_EXTERNAL_URL || '').replace(/\/+$/, '');
const PUB = fileURLToPath(new URL('../public', import.meta.url));

// طاقم المكتب (للترحيب وإسناد الأولويات) — يُضبط KIOSK_STAFF كـJSON: [{"name","title","role"}]
let STAFF = [{ name: 'أمين', title: 'المدير', role: 'management' }, { name: 'محمد', title: 'الأستاذ', role: 'operations' }];
try { if (process.env.KIOSK_STAFF) { const j = JSON.parse(process.env.KIOSK_STAFF); if (Array.isArray(j) && j.length) STAFF = j.slice(0, 30); } }
catch { console.warn('[⚙] KIOSK_STAFF ليس JSON صالحاً — أبقيت الطاقم الافتراضي'); }

// ---------- الحالة (بالذاكرة، مفهرسة ومحدودة — تتحمّل 10آلاف طلب/يوم بلا تدهور) ----------
/** deviceId -> {id, name, device, online, sos, lastSeen, last:{...}, trail:[[lng,lat]]} */
// 🛡️ حواجز أمان عامة: خطأ غير متوقع في أي معالِج لا يُسقط البوابة وكل الموصلين
process.on('uncaughtException', (e) => console.error('[uncaught]', e?.stack || e));
process.on('unhandledRejection', (e) => console.error('[unhandledRejection]', e?.stack || e));

const drivers = new Map();
const opsClients = new Set();
const driverClients = new Map();          // ws -> driverId
const driverToWs = new Map();             // driverId -> ws   (فهرس عكسي O(1) بدل مسح خطي)
const onlineIds = new Set();              // معرّفات المتصلين — عدّ O(1)
const TERMINAL = new Set(['delivered', 'cancelled']);
const OFFLINE_TTL_MS = 24 * 3600_000;     // موصل غير متصل يُنسى بعد يوم

// ---------- الطلبات: مسار مُدار مفهرس، مع إخلاء الطلبات المنتهية ----------
/** id -> {id, title, dest, driverId, driverName, status, createdAt, updatedAt, _offer?} */
const orders = new Map();
const refIndex = new Map();               // مرجع خارجي (رقم طلب التطبيق) -> معرّف داخلي — منع التكرار
const driverToOrderId = new Map();        // driverId -> معرّف طلبه النشط (فهرس O(1))
const ACTIVE = new Map();                 // معرّفات الطلبات النشطة فقط — تُبث كاملة للوحة
const ORDER_RETAIN_MS = 5 * 60_000;       // يُحتفظ بالطلب المنتهي 5 دقائق ثم يُخلى
// بذرة تسلسل من الوقت لتفادي تصادم الأرقام بعد إعادة التشغيل (Render redeploy)
let orderSeq = 100000 + Math.floor((Date.now() / 1000) % 800000);
const ORDER_STATUSES = new Set(['new', 'assigned', 'picked', 'delivered', 'cancelled']);
// انتقالات حالة مشروعة فقط — تمنع القفز (delivered→new) أو التخطي غير المنطقي
const NEXT_OK = { new: ['assigned', 'cancelled'], assigned: ['picked', 'cancelled', 'new'],
  picked: ['delivered', 'cancelled'], delivered: [], cancelled: [] };

const orderPublic = ({ _offer, _reminded, _evictAt, ...rest }) => rest;   // لا تُبث الحقول الداخلية أبداً
const activeOrdersList = () => [...ACTIVE.values()].sort((a, b) => b.createdAt - a.createdAt).map(orderPublic);
const driverWs = (id) => driverToWs.get(id);
const driverActiveOrderRaw = (id) => { const oid = driverToOrderId.get(id); return oid ? orders.get(oid) || null : null; };
const driverActiveOrder = (id) => { const o = driverActiveOrderRaw(id); return o ? orderPublic(o) : null; };
const driverBusy = (id) => driverToOrderId.has(id);

const publicInfo = (d) => ({
  id: d.id, name: d.name, device: d.device, online: d.online, sos: !!d.sos,
  lastSeen: d.lastSeen, last: d.last, trail: d.trail,
});
// إرسال آمن مع ضغط خلفي: يتجاوز العميل المتباطئ بدل تكديس ميغابايتات في ذاكرته
const MAX_BUFFER = 4 * 1024 * 1024;
const send = (ws, msg) => { if (ws.readyState === ws.OPEN && ws.bufferedAmount < MAX_BUFFER) ws.send(JSON.stringify(msg)); };
const broadcastOps = (msg) => { const s = JSON.stringify(msg); for (const ws of opsClients) if (ws.readyState === ws.OPEN && ws.bufferedAmount < MAX_BUFFER) ws.send(s); };
const broadcastDrivers = (msg, except) => { const s = JSON.stringify(msg); for (const ws of driverClients.keys()) if (ws !== except && ws.readyState === ws.OPEN && ws.bufferedAmount < MAX_BUFFER) ws.send(s); };
const broadcastAll = (msg, except) => { broadcastOps(msg); broadcastDrivers(msg, except); };
const onlineCount = () => onlineIds.size;
const pushStats = () => broadcastDrivers({ t: 'stats', online: onlineCount(), channel: 'العمليات العامة' });
const pushOrderDelta = (o) => broadcastOps({ t: 'order_upd', order: orderPublic(o) });   // تحديث مفرد O(1)
const pushDriverOrder = (driverId) => { const ws = driverWs(driverId); if (ws) send(ws, { t: 'order', order: driverActiveOrder(driverId) }); };

// يحدّث فهارس النشاط عند تغيّر حالة الطلب أو موصله
function reindexOrder(o, prevDriverId) {
  if (prevDriverId && driverToOrderId.get(prevDriverId) === o.id) driverToOrderId.delete(prevDriverId);
  if (TERMINAL.has(o.status)) {
    ACTIVE.delete(o.id);
    pending.delete(o.id);
    if (o.driverId && driverToOrderId.get(o.driverId) === o.id) { driverToOrderId.delete(o.driverId); setImmediate(pumpQueue); }  // تفرّغ موصل → اصرف الطابور
    o._evictAt = Date.now() + ORDER_RETAIN_MS;                  // يُخلى من orders لاحقاً
  } else {
    ACTIVE.set(o.id, o);
    if (o.driverId) driverToOrderId.set(o.driverId, o.id);
  }
}

// ---------- 🏪 المتاجر: نقاط التقاط حية على الخريطة (مراقب المتجر) ----------
const stores = new Map();                 // ST-x -> {id, name, lat, lng}
let storeSeq = 0;
const matchStore = (title) => { for (const s of stores.values()) if (s.name && String(title).includes(s.name)) return s; return null; };
// ربط الطلب بمتجره: يعطي مسار التقاط (الموصل ← المتجر ← الزبون) وإحصاء حي لكل متجر
function linkStore(o) {
  const st = (o.title && matchStore(o.title)) || null;
  if (st) { o.storeId = st.id; o.origin ||= { lat: st.lat, lng: st.lng }; }
}

const closedOrders = [];                  // آخر 100 طلب مغلق (اليوم) — للوحة «المغلقة» بسجلها الكامل
function setOrder(o, patch) {
  const prevDriver = o.driverId, prevStatus = o.status;
  Object.assign(o, patch, { updatedAt: Date.now() });
  if (o.status !== prevStatus) {                                 // 📜 سجل تتبع كامل: من فعل ماذا ومتى
    (o.history ||= []).push({ st: o.status, at: Date.now(), d: o.driverName || null });
    if (o.history.length > 15) o.history.shift();
  }
  if (o.status !== prevStatus && (o.status === 'new' || TERMINAL.has(o.status))) o.riskLate = false;   // زال خطر التأخير بزوال الرحلة
  if (o.status !== prevStatus && TERMINAL.has(o.status)) {
    closedOrders.push(orderPublic(o));
    if (closedOrders.length > 100) closedOrders.shift();
    statBump(o.status);                                          // إحصاء يومي عند الإغلاق
    if (o.status === 'cancelled') {                              // نمط إلغاءات متكرر ← أولوية مراجعة
      const c = statsFor(0).cancelled;
      if (c >= 3) prOpen('cancels:' + dateKey(), { type: 'cancels', score: Math.min(70, 48 + c * 2),
        title: `${c} إلغاءات اليوم — نمط يحتاج مراجعة`, assignedRole: 'management',
        recommendedAction: 'راجعوا أسباب الإلغاء مع الفريق والمتاجر' });
    }
  }
  reindexOrder(o, patch.driverId !== undefined ? prevDriver : undefined);
  pushOrderDelta(o);
  brainEvent('order_' + o.status, { order: orderPublic(o) });
}

// ---------- إحصاء يومي لعقل ديار (اليوم/أمس) — لا يتجاوز 8 أيام ----------
const dailyStats = new Map();             // 'YYYY-MM-DD' -> {created, delivered, cancelled, escalated, sos}
const dateKey = (off = 0) => new Date(Date.now() - off * 86400_000).toISOString().slice(0, 10);
function statBump(kind, off = 0) {
  const k = dateKey(off);
  const s = dailyStats.get(k) || { created: 0, delivered: 0, cancelled: 0, escalated: 0, sos: 0 };
  if (kind in s) s[kind]++;
  dailyStats.set(k, s);
  if (dailyStats.size > 8) { const oldest = [...dailyStats.keys()].sort()[0]; dailyStats.delete(oldest); }
  backupDirty = true;                                            // 💾 يُنسخ لغرفة التشغيل خلال دقيقة
}
const statsFor = (off) => dailyStats.get(dateKey(off)) || { created: 0, delivered: 0, cancelled: 0, escalated: 0, sos: 0 };

// إخلاء دوري: طلبات منتهية تجاوزت مدة الاحتفاظ + موصلون غير متصلين منذ يوم
setInterval(() => {
  const now = Date.now();
  for (const [id, o] of orders) if (o._evictAt && now > o._evictAt) { if (o.ref) refIndex.delete(o.ref); orders.delete(id); }
  for (const [id, d] of drivers) if (!d.online && now - (d.lastSeen || 0) > OFFLINE_TTL_MS && !driverToOrderId.has(id)) { unindexDriver(id); drivers.delete(id); }
}, 60_000).unref?.();

// ================= 🗺 الفهرس الجغرافي الحي — خلايا سداسية (نمط H3 الذي تعتمده Uber) =================
// بدل مسح كل الموصلين لكل طلب (O(N))، المدينة مقسومة خلايا سداسية والموصل مفهرس بخليته لحظياً
// مع كل نبضة GPS («من خرج من الخلية ومن دخل») — والبحث عند الطلب: خلية الوجهة + جيرانها فقط.
const HEX_KM = Number(process.env.HEX_CELL_KM || 0.7);          // حجم الخلية بالكيلومتر
const KM_LAT = 110.574, KM_LON = 111.320 * Math.cos(32.9 * Math.PI / 180);   // إسقاط محلي (الجليل)
const SQ3 = Math.sqrt(3);
function cellOf(lat, lng) {                                     // إحداثيات محورية سداسية + تقريب مكعبي
  const x = lng * KM_LON, y = lat * KM_LAT;
  const q = (SQ3 / 3 * x - y / 3) / HEX_KM, r = (2 / 3 * y) / HEX_KM;
  let rq = Math.round(q), rr = Math.round(r); const ry = Math.round(-q - r);
  const dq = Math.abs(rq - q), dr = Math.abs(rr - r), dy = Math.abs(ry - (-q - r));
  if (dq > dr && dq > dy) rq = -ry - rr; else if (dr > dy) rr = -rq - ry;
  return rq + ',' + rr;
}
function cellDisk(key, k) {                                     // كل الخلايا حتى k حلقات حول المركز
  const [q, r] = key.split(',').map(Number), out = [];
  for (let dq = -k; dq <= k; dq++)
    for (let dr = Math.max(-k, -dq - k); dr <= Math.min(k, -dq + k); dr++)
      out.push((q + dq) + ',' + (r + dr));
  return out;
}
const cellDrivers = new Map();    // خلية -> Set(driverId) للمتصلين بمواقع معلومة
const driverCell = new Map();     // driverId -> خليته الحالية
function indexDriver(id, lat, lng) {
  const key = cellOf(lat, lng), prev = driverCell.get(id);
  if (prev === key) return;
  if (prev) { const s = cellDrivers.get(prev); if (s) { s.delete(id); if (!s.size) cellDrivers.delete(prev); } }
  driverCell.set(id, key);
  let s = cellDrivers.get(key); if (!s) { s = new Set(); cellDrivers.set(key, s); }
  s.add(id);
}
function unindexDriver(id) {
  const prev = driverCell.get(id);
  if (prev) { const s = cellDrivers.get(prev); if (s) { s.delete(id); if (!s.size) cellDrivers.delete(prev); } driverCell.delete(id); }
}
const liveIndex = () => [...cellDrivers.entries()].map(([cell, s]) => ({ cell, drivers: s.size }))
  .sort((a, b) => b.drivers - a.drivers);

// ---------- «طرق لا خطوط»: الترتيب بوقت الوصول الحقيقي عبر الشوارع (OSRM) لا بالمسافة المستقيمة ----------
// الأقرب بالأمتار قد يكون أبعد بالدقائق (أزمة/طريق أطول) — فالإسناد بالـETA. عند تعذر OSRM:
// تقدير حتمي فوري (سرعة بلدات) وقاطع دارة يوقف المحاولات دقيقة كاملة — الإسناد لا ينتظر الشبكة أبداً.
const OSRM_URL = (process.env.OSRM_URL ?? 'https://router.project-osrm.org').replace(/\/+$/, '');
const AVG_KMH = 28;
const etaEst = (km) => Math.max(1, Math.round((km / AVG_KMH) * 60));
let osrmDownUntil = 0;
async function rankByEta(cands, dest) {
  const live = cands.filter(c => drivers.get(c.id)?.last);
  for (const c of live) c.etaMin = etaEst(c.km);
  if (OSRM_URL && live.length >= 2 && Date.now() >= osrmDownUntil) {
    try {
      const coords = live.map(c => { const l = drivers.get(c.id).last; return `${l.lng},${l.lat}`; })
        .join(';') + `;${dest.lng},${dest.lat}`;
      const r = await fetch(`${OSRM_URL}/table/v1/driving/${coords}?destinations=${live.length}`,
        { signal: AbortSignal.timeout(2500) });
      if (!r.ok) throw new Error('http ' + r.status);
      const j = await r.json();
      live.forEach((c, i) => { const s = j.durations?.[i]?.[0]; if (Number.isFinite(s)) c.etaMin = Math.max(1, Math.round(s / 60)); });
    } catch { osrmDownUntil = Date.now() + 60_000; }
  }
  return live.sort((a, b) => a.etaMin - b.etaMin || a.km - b.km);
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

// أقرب موصلين **أحرار** عبر الفهرس السداسي: خلية الوجهة ثم حلقات الجيران — لا مسح كامل
const MAX_RING = 7;                                              // ~5 كم بحثاً بالخلايا قبل مظلة الأمان
function dispatchCandidates(o) {
  const T = o.origin || o.dest;                                  // نقطة الالتقاط أولاً إن وُجد متجر (نمط Uber)
  const cands = [], destKey = cellOf(T.lat, T.lng), visited = new Set();
  const pick = (id) => { const d = drivers.get(id);
    if (d?.last && d.online && !d.sos && !driverBusy(id)) cands.push({ id: d.id, name: d.name, km: havKm(d.last, T) }); };
  for (let k = 0; k <= MAX_RING && cands.length < MAX_OFFERS * 2; k++)
    for (const key of cellDisk(destKey, k)) {
      if (visited.has(key)) continue; visited.add(key);
      const s = cellDrivers.get(key); if (s) for (const id of s) pick(id);
    }
  if (!cands.length) for (const id of onlineIds) pick(id);       // مظلة أمان: موصلون خارج نطاق الحلقات
  return cands.sort((a, b) => a.km - b.km).slice(0, MAX_OFFERS * 2);   // مرشحون أكثر ⟵ ترتيب ETA يختار
}

// بدء العرض: الترتيب النهائي بالـETA الحقيقي ثم النداء — والإلغاء أثناء الترتيب آمن (فحص الهوية)
function beginOffer(o, cands) {
  const of = { cands: cands.slice(0, MAX_OFFERS), idx: 0, timer: null, ranking: true };
  o._offer = of;
  rankByEta(cands, o.origin || o.dest)
    .then((ranked) => { if (o._offer === of) { if (ranked.length) of.cands = ranked.slice(0, MAX_OFFERS); of.ranking = false; offerNext(o); } })
    .catch(() => { if (o._offer === of) { of.ranking = false; offerNext(o); } });
}

// ---------- طابور الانتظار: طلبات لم تجد موصلاً حرًّا، تُعاد المحاولة عند تفرّغ أي موصل ----------
const pending = new Map();                 // orderId -> وقت الدخول للطابور
const STALE_ESCALATE_MS = Number(process.env.STALE_ESCALATE_MIN || 5) * 60_000;   // تصعيد بشري (والمحاولة لا تتوقف)
function enqueue(o) {
  if (o.status !== 'new') return;
  o._offer = null;
  if (!pending.has(o.id)) { pending.set(o.id, Date.now()); setOrder(o, { offeredTo: null }); }
}
// يصرف الطابور: يسند أقدم الطلبات المنتظرة إلى الموصلين الأحرار المتاحين الآن
function pumpQueue() {
  if (!pending.size) return;
  for (const id of [...pending.keys()]) {
    const o = orders.get(id);
    if (!o || o.status !== 'new') { pending.delete(id); continue; }
    if (o._offer) continue;
    // عالق طويلاً رغم وجود موصلين ⟵ تصعيد بشري واحد، والمحاولة الآلية **لا تتوقف** (طيار آلي)
    if (Date.now() - pending.get(id) > STALE_ESCALATE_MS && onlineIds.size && !prByKey.has('escalated:' + o.id)) {
      statBump('escalated');
      talyaFeed(`🔴 ${o.id}: انتظر طويلاً بلا قبول — صعّدت للعمليات وأواصل المحاولة.`);
      brainEvent('dispatch_escalated', { order: orderPublic(o), reason: 'stale' });
      prOpen('escalated:' + o.id, { type: 'dispatch', score: 82, orderId: o.id, source: 'dispatch',
        title: `${o.id} (${o.title}) بلا قبول رغم وجود موصلين`,
        recommendedAction: 'تاليا تواصل المحاولة آلياً — تدخلوا يدوياً إن لزم أو كلموا الموصلين' });
    }
    const cands = dispatchCandidates(o);
    if (!cands.length) return;                                   // لا موصل حرّ الآن — نتوقف حتى تفرّغ أحدهم
    pending.delete(id);
    beginOffer(o, cands);
  }
}
setInterval(pumpQueue, 3000).unref?.();     // ضمان تقدّم دوري حتى بلا أحداث

function startDispatch(o) {
  if (o._offer) { clearTimeout(o._offer.timer); o._offer = null; }
  const cands = dispatchCandidates(o);
  if (!cands.length) { enqueue(o); return; }                     // لا موصل حرّ → طابور (لا إهمال)
  beginOffer(o, cands);
}

function offerNext(o) {
  const of = o._offer;
  if (!of || of.ranking || ['assigned', 'picked', 'delivered', 'cancelled'].includes(o.status)) return;
  if (of.idx >= of.cands.length) {                                // كل من عُرض عليهم رفضوا/انشغلوا → أعِد للطابور
    o._offer = null;
    enqueue(o);
    return;
  }
  const c = of.cands[of.idx];
  setOrder(o, { offeredTo: c.name });
  const w = driverWs(c.id);
  if (!w) { of.idx++; return offerNext(o); }
  send(w, { t: 'offer', order: { id: o.id, title: o.title, dest: o.dest }, km: Math.round(c.km * 10) / 10,
    etaMin: c.etaMin || null, expiresInS: OFFER_TIMEOUT_MS / 1000 });
  talyaSay(c.id, `طلب جديد: ${o.title}. ${c.etaMin ? `يبعد عنك ${c.etaMin} دقيقة بالطريق` : `يبعد عنك ${c.km.toFixed(1)} كيلومتر`}. اضغط قبول خلال ${OFFER_TIMEOUT_MS / 1000} ثانية.`);
  talyaFeed(`🎙 ${o.id}: أعرضه الآن على ${c.name} (${c.etaMin ? c.etaMin + ' د · ' : ''}${c.km.toFixed(1)} كم)${of.idx ? ` — المحاولة ${of.idx + 1}` : ''}…`);
  of.timer = setTimeout(() => { of.idx++; offerNext(o); }, OFFER_TIMEOUT_MS);
}

function answerOffer(o, driverId, accept) {
  const of = o._offer;
  const c = of?.cands[of.idx];
  if (!c || c.id !== driverId || o.status !== 'new') return;      // ليس المعروض عليه حالياً
  clearTimeout(of.timer);
  if (accept) {
    // 🛡️ منع الإسناد المزدوج: إن صار للموصل طلب نشط بين العرض والقبول، ننتقل للتالي
    if (driverBusy(driverId)) { talyaFeed(`⚪ ${o.id}: ${c.name} انشغل بطلب آخر — أنتقل للتالي.`); of.idx++; return offerNext(o); }
    const d = drivers.get(driverId);
    if (!d) { of.idx++; return offerNext(o); }
    o._offer = null;
    setOrder(o, { driverId, driverName: d.name, status: 'assigned', offeredTo: null,
      etaMin: c.etaMin || null, etaAt: c.etaMin ? Date.now() : null });
    pushDriverOrder(driverId);
    talyaSay(driverId, `تم، الطلب ${o.id} لك. بالسلامة.`);
    talyaFeed(`🟢 ${o.id}: قبله ${d.name} — أُسند.`);
  } else {
    talyaFeed(`⚪ ${o.id}: رفضه ${c.name}.`);
    of.idx++; offerNext(o);
  }
}

// إلغاء عرض تاليا الجاري وإبلاغ الموصل المعروض عليه (يُستدعى عند الإسناد اليدوي/الإلغاء)
function cancelOffer(o) {
  if (!o?._offer) return;
  clearTimeout(o._offer.timer);
  const c = o._offer.cands[o._offer.idx];
  if (c) { const w = driverWs(c.id); if (w) send(w, { t: 'offer_cancel', orderId: o.id }); }
  o._offer = null;
}

// ---------- جسر لوحة العقل: دفع الأحداث (fire-and-forget) ----------
function brainEvent(event, payload) {
  if (!BRAIN_WEBHOOK_URL) return;
  fetch(BRAIN_WEBHOOK_URL, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-api-key': BRAIN_API_KEY },
    body: JSON.stringify({ event, at: Date.now(), ...payload }),
    signal: AbortSignal.timeout(8000),                            // لا تكديس وعود عند تعثّر الطرف الآخر
  }).catch(() => {});
}

// ---------- 💾 ديمومة الحالة عبر غرفة التشغيل (تخزينها دائم) — تنجو من إعادة النشر ----------
// المتاجر والإحصاء اليومي يُنسخان احتياطياً إلى غرفة التشغيل عند كل تغيّر، ويُستعادان عند الإقلاع.
let backupDirty = false;
function backupState() {
  if (!BRAIN_WEBHOOK_URL || !process.env.BRAIN_API_KEY) return;
  brainEvent('state_backup', { backup: { stores: [...stores.values()], dailyStats: [...dailyStats.entries()], storeSeq } });
  backupDirty = false;
}
setInterval(() => { if (backupDirty) backupState(); }, 60_000).unref?.();
async function restoreState() {
  if (!BRAIN_PANEL_URL || !process.env.BRAIN_API_KEY) return;
  try {
    const r = await fetch(BRAIN_PANEL_URL.replace(/\/+$/, '') + '/webhooks/dyar-connect/backup',
      { headers: { 'x-api-key': BRAIN_API_KEY }, signal: AbortSignal.timeout(8000) });
    if (!r.ok) return;
    const b = (await r.json())?.backup;
    if (!b) return;
    if (!stores.size && Array.isArray(b.stores)) {
      for (const s of b.stores) if (s?.id && s.name && Number.isFinite(+s.lat) && Number.isFinite(+s.lng))
        stores.set(s.id, { id: s.id, name: String(s.name).slice(0, 40), lat: +s.lat, lng: +s.lng });
      storeSeq = Math.max(storeSeq, +b.storeSeq || 0, ...[...stores.keys()].map(k => +String(k).split('-')[1] || 0));
    }
    if (Array.isArray(b.dailyStats))
      for (const [k, v] of b.dailyStats) if (!dailyStats.has(k) && v && typeof v === 'object') dailyStats.set(k, v);
    console.log(`[💾] استُعيدت الحالة من غرفة التشغيل: ${stores.size} متجر · ${dailyStats.size} يوم إحصاء`);
  } catch { /* أفضل-جهد — يعمل بلا استعادة */ }
}

// ---------- تسجيل ذاتي لدى غرفة التشغيل (Zero-Config) ----------
// نرسل عنواننا ومفتاحنا عند الإقلاع ثم كنبض كل 10 دقائق — فترتبط الخدمتان بلا أي إعداد يدوي.
// يعمل فقط عندما يكون BRAIN_API_KEY مضبوطاً بالبيئة (على Render يولَّد تلقائياً من blueprint).
let brainRegistered = false;
async function registerWithBrain() {
  if (!SELF_URL || !BRAIN_PANEL_URL || !process.env.BRAIN_API_KEY) return;
  try {
    const r = await fetch(BRAIN_PANEL_URL.replace(/\/+$/, '') + '/webhooks/dyar-connect-register', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ url: SELF_URL, key: BRAIN_API_KEY }),
      signal: AbortSignal.timeout(8000),
    });
    const j = await r.json().catch(() => ({}));
    if (r.ok && !brainRegistered) {
      brainRegistered = true;
      console.log(`[🧠] مسجّل لدى غرفة التشغيل${j.first ? ' (ربط أول)' : ''}: ${BRAIN_PANEL_URL}`);
    }
    if (!r.ok) console.warn('[🧠] رفضت غرفة التشغيل التسجيل:', j.reason || r.status);
  } catch (e) { if (!brainRegistered) console.warn('[🧠] تعذر التسجيل لدى غرفة التشغيل:', e.message); }
}
setInterval(registerWithBrain, 10 * 60_000);

// ================= ⚡ محرك الأولويات والتنفيذ — Dyar Priority & Action Engine =================
// كل حدث مهم يتحول إلى عنصر عمل مُدار حتى النهاية:
//   اكتشاف → فهم → تسعير (حسابي لا لغوي) → إسناد → تنبيه → ACK → تصعيد → تحقق آلي → إغلاق بنتيجة → تعلم
// ممنوع اعتبار المشكلة «عولجت» لمجرد إرسال تنبيه: بلا ACK تتصعد، وبلا زوال السبب فعلياً تُعاد للفتح.
const priorities = new Map();     // id -> الأولوية المفتوحة
const prByKey = new Map();        // مفتاح الارتباط -> id  (أولوية واحدة مفتوحة لكل حالة — لا تكرار)
const prClosed = [];              // آخر 200 مغلقة — للمراجعة والتعلم اليومي
let prSeq = 0;
const PR_OPEN = new Set(['detected', 'notified', 'acknowledged', 'in_progress', 'escalated']);
const SEV_RANK = { P0: 0, P1: 1, P2: 2, P3: 3, P4: 4 };
// سياسة التصعيد بلا ACK لكل مستوى: [تذكير، تصعيد للاحتياط، تصعيد للإدارة] منذ الإنشاء (0 = لا تصعيد)
const ESCALATION = { P0: [60_000, 120_000, 240_000], P1: [120_000, 300_000, 600_000],
                     P2: [900_000, 0, 0], P3: [0, 0, 0], P4: [0, 0, 0] };
const ESC_LABEL = ['', 'تذكير — لم يُؤكَّد الاستلام', 'تصعيد للموظف الاحتياط', 'تصعيد للإدارة'];

const sevFromScore = (s) => s >= 90 ? 'P0' : s >= 75 ? 'P1' : s >= 50 ? 'P2' : s >= 25 ? 'P3' : 'P4';
const prPublic = (p) => p;                         // لا حقول داخلية حالياً — نقطة تمدد مستقبلية
const openPriorities = () => [...priorities.values()].filter(p => PR_OPEN.has(p.status))
  .sort((a, b) => b.score - a.score || a.createdAt - b.createdAt).map(prPublic);
const openCounts = () => { const c = { P0: 0, P1: 0, P2: 0, P3: 0, P4: 0 };
  for (const p of priorities.values()) if (PR_OPEN.has(p.status)) c[p.severity]++; return c; };
// حمل الفريق: أولويات مفتوحة لكل مالك (أو دور إن لم تُستلم بعد)
const teamLoad = () => { const m = {};
  for (const p of priorities.values()) if (PR_OPEN.has(p.status)) {
    const k = p.assignedTo || p.assignedRole;
    (m[k] ||= { P0: 0, P1: 0, P2: 0, P3: 0, P4: 0, total: 0 }); m[k][p.severity]++; m[k].total++;
  } return m; };

function prLog(p, ev, by, note) {
  p.log.push({ at: Date.now(), ev, ...(by ? { by } : {}), ...(note ? { note } : {}) });
  if (p.log.length > 30) p.log.shift();
}
const prPush = (p) => broadcastOps({ t: 'priority', p: prPublic(p) });   // اللوحة + شاشة المكتب (WS)

/** فتح أولوية أو تحديث درجة المفتوحة بنفس المفتاح (تسعير ديناميكي — الأولوية ليست ثابتة) */
function prOpen(key, data) {
  const exId = prByKey.get(key), ex = exId && priorities.get(exId);
  if (ex && PR_OPEN.has(ex.status)) {
    const sev = sevFromScore(data.score);
    if (ex.score !== data.score || ex.severity !== sev) {
      const up = SEV_RANK[sev] < SEV_RANK[ex.severity];
      Object.assign(ex, { score: data.score, severity: sev, updatedAt: Date.now() });
      if (data.title) ex.title = String(data.title).slice(0, 120);          // العمر/الوصف يتجددان مع التسعير
      if (data.description) ex.description = data.description;
      prLog(ex, 'rescored', null, `${ex.severity} (${ex.score})`);
      prPush(ex);
      if (up && SEV_RANK[sev] <= 1) brainEvent('priority_escalated', { priority: prPublic(ex) });
    }
    return ex;
  }
  const p = {
    id: 'PR-' + (++prSeq), key, type: data.type, title: String(data.title).slice(0, 120),
    description: String(data.description || '').slice(0, 300),
    severity: sevFromScore(data.score), score: data.score, source: data.source || 'observer',
    orderId: data.orderId || null, driverId: data.driverId || null,
    assignedRole: data.assignedRole || 'operations', assignedTo: null,
    recommendedAction: String(data.recommendedAction || '').slice(0, 200),
    status: 'notified', escalationLevel: 0, verified: false,
    createdAt: Date.now(), updatedAt: Date.now(), ackAt: null, resolvedAt: null, closedAt: null,
    outcome: null, log: [],
  };
  prLog(p, 'detected', null, data.type);
  prLog(p, 'notified', null, p.assignedRole);
  priorities.set(p.id, p); prByKey.set(key, p.id);
  prPush(p);
  brainEvent('priority_created', { priority: prPublic(p) });
  if (SEV_RANK[p.severity] <= 1) talyaFeed(`⚡ ${p.severity} ${p.id}: ${p.title} → ${p.assignedRole}`);
  return p;
}

function prClose(p, outcome, by, verified = true) {
  p.status = 'closed'; p.closedAt = Date.now(); p.verified = verified;
  p.outcome = String(outcome || p.outcome || 'بلا تفاصيل').slice(0, 200);
  prLog(p, 'closed', by, p.outcome);
  priorities.delete(p.id); prByKey.delete(p.key);
  prClosed.push(prPublic(p)); if (prClosed.length > 200) prClosed.shift();
  prPush(p);
  brainEvent('priority_closed', { priority: prPublic(p) });
}
const prCloseByKey = (key, outcome, by) => { const id = prByKey.get(key), p = id && priorities.get(id); if (p && PR_OPEN.has(p.status)) prClose(p, outcome, by); };

/** إجراء بشري من اللوحة/الشاشة/الهاتف: ack | start | resolve | reassign — انتقالات مشروعة فقط */
function prAction(id, action, by, note) {
  const p = priorities.get(id);
  if (!p || !PR_OPEN.has(p.status)) return null;
  const who = String(by || 'العمليات').slice(0, 40);
  if (action === 'ack') { if (!p.ackAt) { p.ackAt = Date.now(); p.status = 'acknowledged'; p.assignedTo ||= who; prLog(p, 'acknowledged', who); } }
  else if (action === 'start') { p.status = 'in_progress'; p.ackAt ||= Date.now(); p.assignedTo ||= who; prLog(p, 'started', who); }
  else if (action === 'resolve') {          // «حُلّت» بشرياً — تبقى قيد التحقق الآلي حتى يثبت زوال السبب
    p.status = 'resolved'; p.resolvedAt = Date.now(); p.assignedTo ||= who;
    p.outcome = String(note || 'عولجت').slice(0, 200);
    prLog(p, 'resolved', who, p.outcome);
    brainEvent('priority_resolved', { priority: prPublic(p) });
  }
  else if (action === 'reassign' && note) { p.assignedTo = String(note).slice(0, 40); prLog(p, 'reassigned', who, p.assignedTo); }
  else return null;
  p.updatedAt = Date.now(); prPush(p);
  return p;
}

/** هل سبب الأولوية ما زال قائماً؟ true=قائم، false=زال، null=لا يُتحقق آلياً (يُقفل بقرار بشري) */
function prConditionActive(p) {
  switch (p.type) {
    case 'sos':         return !!drivers.get(p.driverId)?.sos;
    case 'no_drivers':  return ACTIVE.size > 0 && onlineCount() === 0;
    case 'queue':       return pending.has(p.orderId);
    case 'dispatch':    return orders.get(p.orderId)?.status === 'new';
    case 'stuck':       { const o = orders.get(p.orderId); return !!o && o.status === 'assigned' && Date.now() - o.updatedAt > STUCK_ASSIGNED_MS; }
    case 'late':        { const o = orders.get(p.orderId); return !!o && o.status === 'picked' && Date.now() - o.updatedAt > LATE_PICKED_MS; }
    case 'driver_lost': { const d = drivers.get(p.driverId); return !!d && !d.online && driverToOrderId.has(p.driverId); }
    case 'eta_risk':    { const o = orders.get(p.orderId); return !!o && !TERMINAL.has(o.status) && o.riskLate === true; }
    default:            return null;
  }
}

// ---------- الراصد (Observer): كشف استباقي + تحقق + تصعيد — كنس كل 10 ثوانٍ ----------
const STUCK_ASSIGNED_MS = 15 * 60_000;    // مُسند بلا استلام (أولوية)
const LATE_PICKED_MS = 45 * 60_000;       // مستلَم بلا تسليم (أولوية — لا سحب آلي: البضاعة معه)
const DRIVER_LOST_MS = 3 * 60_000;        // موصل معه طلب وانقطع (أولوية)
// 🤖 الطيار الآلي: تاليا تتعافى وحدها قبل أن يتدخل أحد — البشر للتصعيد فقط
const REMIND_ASSIGNED_MS = Number(process.env.REMIND_ASSIGNED_MIN || 8) * 60_000;      // تذكير صوتي آلي
const REASSIGN_ASSIGNED_MS = Number(process.env.REASSIGN_ASSIGNED_MIN || 18) * 60_000; // سحب: مُسند بلا استلام
const REASSIGN_LOST_MS = Number(process.env.REASSIGN_LOST_MIN || 5) * 60_000;          // سحب: موصل منقطع قبل الاستلام
function autoRecover(now) {
  for (const o of [...ACTIVE.values()]) {
    if (o.status !== 'assigned' || !o.driverId) continue;        // بعد الاستلام لا سحب آلي — تصعيد بشري فقط
    const d = drivers.get(o.driverId);
    const idleMs = now - o.updatedAt;
    const lostMs = d && !d.online ? now - (d.lastSeen || 0) : 0;
    if (lostMs > REASSIGN_LOST_MS || idleMs > REASSIGN_ASSIGNED_MS) {
      const reason = lostMs > REASSIGN_LOST_MS
        ? `انقطاع ${o.driverName || 'الموصل'} قبل الاستلام` : `بلا استلام منذ ${Math.round(idleMs / 60_000)} د`;
      const prev = o.driverId;
      cancelOffer(o);
      setOrder(o, { driverId: null, driverName: null, status: 'new', offeredTo: null });
      o._reminded = false;
      pushDriverOrder(prev);
      talyaSay(prev, `سُحب الطلب ${o.id} منك وأعيد توزيعه.`);
      talyaFeed(`🔁 ${o.id}: سحبته آلياً (${reason}) — أعيد عرضه على الأقرب.`);
      startDispatch(o);
    } else if (idleMs > REMIND_ASSIGNED_MS && !o._reminded && d?.online) {
      o._reminded = true;
      talyaSay(o.driverId, `تذكير: الطلب ${o.id} بانتظار استلامك منذ ${Math.round(idleMs / 60_000)} دقائق.`);
      talyaFeed(`⏰ ${o.id}: ذكّرت ${o.driverName} بالاستلام — أسحبه آلياً إن لم يستلم.`);
    }
  }
}
function prSweep() {
  const now = Date.now();
  try { autoRecover(now); } catch (e) { console.error('[autoRecover]', e?.message); }
  // (1) كواشف استباقية من الحالة الحية — تفتح وتعيد التسعير ديناميكياً
  if (ACTIVE.size > 0 && onlineCount() === 0)
    prOpen('no_drivers', { type: 'no_drivers', score: 92, title: `${ACTIVE.size} طلب نشط بلا أي موصل متصل`,
      recommendedAction: 'شغّلوا أجهزة الموصلين فوراً أو نادوا موصلاً احتياطياً', assignedRole: 'operations' });
  for (const [oid, since] of pending) {
    const ageMin = (now - since) / 60_000;
    const o = orders.get(oid); if (!o) continue;
    if (prByKey.has('escalated:' + oid)) continue;              // مُتابع أصلاً كتصعيد — لا ازدواج
    prOpen('queue:' + oid, { type: 'queue', score: Math.min(88, 55 + Math.round(ageMin * 4)),
      title: `${oid} (${o.title}) ينتظر موصلاً حرّاً منذ ${Math.round(ageMin)} د`, orderId: oid,
      recommendedAction: 'أسندوه يدوياً أو تأكدوا من تفرّغ موصل قريب', source: 'dispatch' });
  }
  for (const o of ACTIVE.values()) {
    if (o.status === 'assigned' && now - o.updatedAt > STUCK_ASSIGNED_MS)
      prOpen('stuck:' + o.id, { type: 'stuck', score: 78, orderId: o.id, driverId: o.driverId,
        title: `${o.id} مُسند لـ${o.driverName} منذ ${Math.round((now - o.updatedAt) / 60_000)} د بلا استلام`,
        recommendedAction: 'كلّموا الموصل — قد يحتاج إعادة إسناد' });
    else if (o.status === 'picked' && now - o.updatedAt > LATE_PICKED_MS)
      prOpen('late:' + o.id, { type: 'late', score: 72, orderId: o.id, driverId: o.driverId,
        title: `${o.id} مع ${o.driverName} منذ ${Math.round((now - o.updatedAt) / 60_000)} د بلا تسليم`,
        recommendedAction: 'اطمئنوا على الموصل وأبلغوا العميل بالتأخير' });
  }
  for (const [did, oid] of driverToOrderId) {
    const d = drivers.get(did);
    if (d && !d.online && now - (d.lastSeen || 0) > DRIVER_LOST_MS)
      prOpen('driver_lost:' + did, { type: 'driver_lost', score: 80, driverId: did, orderId: oid,
        title: `انقطع ${d.name} ومعه الطلب ${oid} منذ ${Math.round((now - d.lastSeen) / 60_000)} د`,
        recommendedAction: 'اتصلوا به هاتفياً — وإن تعذر أعيدوا إسناد الطلب' });
  }
  // (2) تحقق آلي: زال السبب ← إغلاق موثّق | «حُلّت» والسبب قائم ← إعادة فتح
  for (const p of [...priorities.values()]) {
    const active = prConditionActive(p);
    if (PR_OPEN.has(p.status)) {
      if (active === false) { prClose(p, 'زال السبب — تحقق آلي', 'الراصد'); continue; }
      // (3) تصعيد غير المستلمة حسب السياسة
      const th = ESCALATION[p.severity] || [0, 0, 0];
      if (!p.ackAt && p.escalationLevel < 3 && th[p.escalationLevel] && now - p.createdAt > th[p.escalationLevel]) {
        p.escalationLevel++; p.status = 'escalated'; p.updatedAt = now;
        prLog(p, 'escalated', null, ESC_LABEL[p.escalationLevel]);
        prPush(p);
        talyaFeed(`⏫ ${p.id} (${p.severity}): ${ESC_LABEL[p.escalationLevel]} — ${p.title}`);
        brainEvent('priority_escalated', { priority: prPublic(p) });
      }
    } else if (p.status === 'resolved') {
      if (active !== true) prClose(p, p.outcome, p.assignedTo, active === false);
      else if (now - p.resolvedAt > 60_000) {      // قيل «حُلّت» لكن السبب ما زال قائماً
        p.status = 'notified'; p.resolvedAt = null; p.updatedAt = now;
        prLog(p, 'reopened', 'الراصد', 'السبب ما زال قائماً');
        prPush(p);
        talyaFeed(`↩️ ${p.id}: أُعيد فتحها — التحقق أظهر أن السبب لم يزل.`);
      }
    }
  }
}
setInterval(prSweep, 10_000).unref?.();

// ---------- 🔮 مراقب ETA: التنبؤ بالتأخير قبل وقوعه (لا يملكه المنافسون لغرفة العمليات) ----------
// كل طلب جارٍ يُعاد حساب وصوله عبر الطرق دورياً؛ إن تجاوز التوقع وعدَ التسليم ⟵ أولوية وقائية
// «سيتأخر بعد X دقيقة إن لم نتدخل» — قبل أن يتأخر فعلاً، لا بعده.
const PROMISE_MIN = Number(process.env.DELIVERY_PROMISE_MIN || 45);   // وعد التسليم منذ إنشاء الطلب
const ETA_REFRESH_MS = 90_000, ETA_BATCH = 8;                         // لطف مع OSRM العام
async function etaMonitor() {
  const now = Date.now();
  const batch = [...ACTIVE.values()]
    .filter(o => o.driverId && ['assigned', 'picked'].includes(o.status)
      && drivers.get(o.driverId)?.last && (!o.etaAt || now - o.etaAt > ETA_REFRESH_MS))
    .sort((a, b) => (a.etaAt || 0) - (b.etaAt || 0)).slice(0, ETA_BATCH);
  for (const o of batch) {
    const l = drivers.get(o.driverId)?.last; if (!l) continue;
    // قبل الاستلام والمتجر معروف: الرحلة رجلان (موصل ← متجر ← زبون)؛ بعده: مباشرة للزبون
    const legs = (o.status === 'assigned' && o.origin) ? [l, o.origin, o.dest] : [l, o.dest];
    let km = 0; for (let i = 0; i < legs.length - 1; i++) km += havKm(legs[i], legs[i + 1]);
    let etaMin = etaEst(km);
    if (OSRM_URL && Date.now() >= osrmDownUntil) {
      try {
        const r = await fetch(`${OSRM_URL}/route/v1/driving/${legs.map(p => `${p.lng},${p.lat}`).join(';')}?overview=false`,
          { signal: AbortSignal.timeout(2500) });
        if (!r.ok) throw new Error('http ' + r.status);
        const s = (await r.json())?.routes?.[0]?.duration;
        if (Number.isFinite(s)) etaMin = Math.max(1, Math.round(s / 60));
      } catch { osrmDownUntil = Date.now() + 60_000; }
    }
    const lateBy = Math.round((Date.now() + etaMin * 60_000 - (o.createdAt + PROMISE_MIN * 60_000)) / 60_000);
    Object.assign(o, { etaMin, etaAt: Date.now(), riskLate: lateBy > 0 });   // بلا لمس updatedAt — كواشف العلوق تعتمد عليه
    pushOrderDelta(o);
    if (lateBy > 0)
      prOpen('eta_risk:' + o.id, { type: 'eta_risk', score: Math.min(85, 58 + lateBy * 2),
        orderId: o.id, driverId: o.driverId, source: 'eta',
        title: `${o.id} متوقع يتأخر ${lateBy} د عن الوعد (${PROMISE_MIN} د) — الوصول بعد ${etaMin} د`,
        recommendedAction: 'كلّموا الموصل أو أبلغوا العميل قبل وقوع التأخير — تدخل وقائي' });
  }
}
setInterval(etaMonitor, 15_000).unref?.();

// ================= 🛠 بوابة الأدوات التنفيذية — Executive Tool Gateway =================
// الـLLM لا يلمس الحالة مباشرة أبداً: كل معرفة تمر عبر أدوات typed للقراءة فقط،
// وكل أمر تنفيذي يمر عبر prAction/مسارات اللوحة المحمية (هوية → صلاحية → تحقق → توثيق).
const EXEC_TOOLS = {
  getCurrentOperations: { desc: 'الوضع التشغيلي الآن: عدد الطلبات النشطة بحالاتها والطابور والموصلين المتصلين',
    fn: () => { const byStatus = { new: 0, assigned: 0, picked: 0 };
      for (const o of ACTIVE.values()) byStatus[o.status] = (byStatus[o.status] || 0) + 1;
      return { asOf: Date.now(), active: ACTIVE.size, byStatus, waiting: pending.size, online: onlineCount(), devices: drivers.size }; } },
  getTodayPerformance: { desc: 'أداء اليوم: الطلبات المُستقبلة والمُنجزة والملغاة والتصعيدات والطوارئ',
    fn: () => ({ date: dateKey(0), ...statsFor(0) }) },
  getYesterdayPerformance: { desc: 'أداء أمس كاملاً بالأرقام',
    fn: () => ({ date: dateKey(1), ...statsFor(1) }) },
  getFleetStatus: { desc: 'الأسطول: كل موصل متصل، هل هو مشغول بطلب، وهل عنده طوارئ، وآخر موقع وسرعة وبطارية',
    fn: () => ({ asOf: Date.now(), online: [...onlineIds].map(id => { const d = drivers.get(id);
      return { id, name: d?.name, busy: driverBusy(id), sos: !!d?.sos, last: d?.last || null }; }) }) },
  getPriorities: { desc: 'الأولويات التشغيلية المفتوحة الآن (مرتبة بالدرجة) وآخر المغلقة بنتائجها',
    fn: () => ({ asOf: Date.now(), open: openPriorities(), counts: openCounts(), recentClosed: prClosed.slice(-10) }) },
  getTeamLoad: { desc: 'حمل الفريق: عدد الأولويات المفتوحة على كل موظف/دور، وقائمة طاقم المكتب',
    fn: () => ({ load: teamLoad(), staff: STAFF }) },
  getLiveIndex: { desc: 'الفهرس الجغرافي الحي (نمط H3): الخلايا السداسية وعدد الموصلين المتصلين في كل خلية — يكشف فجوات التغطية',
    fn: () => ({ asOf: Date.now(), cellKm: HEX_KM, cells: liveIndex() }) },
  getOrder: { desc: 'تفاصيل طلب واحد بمعرّفه (مثل ORD-123456)', params: { orderId: 'معرّف الطلب' },
    fn: ({ orderId }) => { const o = orders.get(String(orderId || '').trim()); return o ? orderPublic(o) : { error: 'لا يوجد طلب بهذا المعرّف' }; } },
  getDriver: { desc: 'تفاصيل موصل واحد بمعرّفه أو اسمه: حالته وموقعه وطلبه الجاري', params: { driver: 'المعرّف أو الاسم' },
    fn: ({ driver }) => { const q = String(driver || '').trim();
      const d = drivers.get(q) || [...drivers.values()].find(x => x.name === q);
      return d ? { ...publicInfo(d), trail: undefined, order: driverActiveOrder(d.id) } : { error: 'لا يوجد موصل بهذا الاسم/المعرّف' }; } },
};
const execToolDefs = () => Object.entries(EXEC_TOOLS).map(([name, t]) => ({
  name, description: t.desc,
  input_schema: { type: 'object',
    properties: Object.fromEntries(Object.entries(t.params || {}).map(([k, d]) => [k, { type: 'string', description: d }])),
    required: Object.keys(t.params || {}) },
}));

// ================= 📺 عقل ديار للتلفاز: ملخص حي + إجابات صوتية =================
// شاشة المكتب (kiosk.html) تسأل صوتياً؛ الخادم يجيب من الحالة الحية فوراً،
// وإن ضُبط ANTHROPIC_API_KEY يصيغ الجواب «عقل كلاودي» (Claude) بذكاء أعمق — مع رجوع محلي عند أي تعثّر.
function brainSummary() {
  const byStatus = { new: 0, assigned: 0, picked: 0 };
  for (const o of ACTIVE.values()) byStatus[o.status] = (byStatus[o.status] || 0) + 1;
  const sosDrivers = [], onlineNames = [];
  for (const id of onlineIds) { const d = drivers.get(id); if (d) onlineNames.push(d.name); }
  for (const d of drivers.values()) if (d.sos) sosDrivers.push(d.name);
  return {
    now: Date.now(),
    today: statsFor(0), yesterday: statsFor(1),
    online: onlineCount(), devices: drivers.size,
    active: ACTIVE.size, waiting: pending.size, byStatus,
    sosDrivers, onlineNames: onlineNames.slice(0, 30),
    prOpen: openPriorities().slice(0, 15), prCounts: openCounts(),
    teamLoad: teamLoad(), staff: STAFF,
    cells: cellDrivers.size, topCells: liveIndex().slice(0, 5),      // الفهرس الجغرافي الحي
  };
}

// أولويات اللحظة نصياً — من محرك الأولويات الحقيقي أولاً ثم قراءات عامة
const SEV_ICON = { P0: '🔴', P1: '🔴', P2: '🟠', P3: '🟡', P4: '🟢' };
function brainPriorities(s) {
  const p = (s.prOpen || []).slice(0, 5).map(x =>
    `${SEV_ICON[x.severity]} ${x.severity} ${x.id}: ${x.title}${x.assignedTo ? ` — عند ${x.assignedTo}` : ` — بانتظار ${x.assignedRole}`}${x.recommendedAction ? `. ${x.recommendedAction}` : ''}`);
  if (s.today.cancelled > Math.max(2, s.today.delivered * 0.15))
    p.push(`🟠 إلغاءات اليوم مرتفعة (${s.today.cancelled}) — راجعوا الأسباب مع الفريق.`);
  if (!p.length) p.push('🟢 لا أولويات مفتوحة — الإيقاع طبيعي، تابعوا الجودة وسرعة التسليم.');
  return p;
}

const fmtDayAr = (off = 0) => new Intl.DateTimeFormat('ar', { weekday: 'long', day: 'numeric', month: 'long' })
  .format(new Date(Date.now() - off * 86400_000));

// إجابة محلية فورية (بلا إنترنت/مفتاح) — تفهم أسئلة المكتب المتوقعة بالكلمات المفتاحية
function brainAnswer(q, s, staff) {
  const t = String(q || '').replace(/[أإآ]/g, 'ا').replace(/ة/g, 'ه');
  const has = (...ws) => ws.some(w => t.includes(w));
  const y = s.yesterday, d = s.today;
  const fmtPr = (x, i) => `${i + 1}) ${x.severity} ${x.title}${x.recommendedAction ? ` — ${x.recommendedAction}` : ''}`;
  if (has('شو عندي', 'ماذا اعمل', 'ماذا افعل', 'مهامي', 'وش اسوي')) {          // طابور مهام الموظف
    const mine = (s.prOpen || []).filter(x => !staff || !x.assignedTo || staff.includes(x.assignedTo) || x.assignedTo.includes(staff));
    return mine.length
      ? `لديك ${mine.length} أولوية. ` + mine.slice(0, 3).map(fmtPr).join(' ') + (mine.length > 1 ? ` أنصح أن تبدأ بالأولى.` : '')
      : 'لا أولويات مفتوحة عليك الآن — تابع الإيقاع الطبيعي وراقب اللوحة.';
  }
  if (has('ضغط', 'مشغول', 'حمل الفريق', 'مين عليه')) {                          // حمل الفريق
    const L = Object.entries(s.teamLoad || {});
    return L.length
      ? 'حمل الفريق الآن: ' + L.map(([k, v]) => `${k}: ${v.total} (منها ${v.P0 + v.P1} عاجلة)`).join('، ') + '.'
      : 'لا أولويات مفتوحة على أحد — الفريق متفرغ.';
  }
  if (has('كيف حالك', 'كيفك', 'شلونك'))
    return `أنا بخير وجاهز للعمل. عندنا الآن ${s.active} طلب نشط و${s.online} موصل متصل. اسألني عن أي شيء.`;
  if (has('امس', 'البارحه', 'مبارح'))
    return `أمس ${fmtDayAr(1)}: استقبلنا ${y.created} طلبية، أُنجز منها ${y.delivered}، وأُلغي ${y.cancelled}.` +
      (y.escalated ? ` وكان هناك ${y.escalated} تصعيد يحتاج مراجعة.` : ' بلا أي تصعيد — يوم نظيف.');
  if (has('مشكل', 'مشاكل', 'خلل', 'تصعيد', 'طوارئ', 'انتباه', 'يحتاج تدخل')) {
    const open = s.prOpen || [];
    if (!open.length) return 'لا مشاكل مفتوحة الآن — لا طوارئ ولا تصعيدات ولا طلبات عالقة.';
    const crit = open.filter(x => SEV_RANK[x.severity] <= 1);
    return `عندنا ${open.length} أولوية مفتوحة${crit.length ? ` منها ${crit.length} عاجلة` : ''}. الأهم: ` +
      open.slice(0, 3).map(fmtPr).join(' ');
  }
  if (has('برنامج', 'اولوي', 'خطه', 'ماذا نفعل', 'شو نعمل'))
    return `برنامج اليوم ${fmtDayAr(0)}: ` + brainPriorities(s).join(' ثم ') +
      ` والهدف: إنجاز أعلى من أمس (${y.delivered} مُنجز).`;
  if (has('متصل', 'موصلين', 'سائق', 'فريق', 'مين موجود'))
    return s.online ? `${s.online} موصل متصل الآن${s.onlineNames.length ? ': ' + s.onlineNames.join('، ') : ''}. المسجّلون كلهم ${s.devices} جهازاً.`
      : 'لا يوجد موصل متصل الآن — الأجهزة كلها خارج الخدمة.';
  if (has('نشط', 'جاري', 'قيد', 'الان كم', 'حاليا'))
    return `الآن: ${s.active} طلب نشط — ${s.byStatus.new} جديد، ${s.byStatus.assigned} مُسند، ${s.byStatus.picked} قيد التوصيل، و${s.waiting} في طابور الانتظار.`;
  if (has('كم طلب', 'الطلبات', 'طلبيه', 'انجز', 'سلمنا', 'وصلنا'))
    return `اليوم ${fmtDayAr(0)}: ${d.created} طلبية جديدة، أُنجز ${d.delivered}، وأُلغي ${d.cancelled}. والآن ${s.active} طلب نشط قيد المتابعة.`;
  if (has('شكرا', 'يعطيك العافيه', 'ممتاز'))
    return 'على الرحب والسعة — أنا هنا دائماً. بالتوفيق لفريق ديار.';
  return `ملخص سريع: اليوم ${d.created} طلبية (${d.delivered} مُنجز)، أمس ${y.created} (${y.delivered} مُنجز). ` +
    `الآن ${s.active} نشط و${s.online} موصل متصل. اسألني: كم أمس؟ ما المشاكل؟ ما برنامج اليوم؟`;
}

// «عقل كلاودي» — طبقة الاستدلال: Claude يفكر ويخطط، لكن كل معرفة تمر عبر بوابة الأدوات
// (EXEC_TOOLS للقراءة فقط) — ممنوع الإجابة عن حالة الشركة من ذاكرة النموذج، والأرقام من النظام حصراً.
async function claudeCall(body) {
  const r = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'x-api-key': process.env.ANTHROPIC_API_KEY, 'anthropic-version': '2023-06-01', 'content-type': 'application/json' },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(45_000),
  });
  if (!r.ok) throw new Error('claude http ' + r.status);
  return r.json();
}
async function askClaude(q, s, staff) {
  const system = 'أنت «عقل ديار التنفيذي» (Dyar Executive Brain) — مدير العمليات الحي في مكتب شركة ديار للتوصيل ' +
    '(مناطق الخدمة: البعنة، دير الأسد، مجد الكروم، كرمئيل). لست Chatbot: تتحدث كمدير محترف — مختصر، واضح، عملي، تبدأ بالأهم. ' +
    'كل رقم تشغيلي يجب أن يأتي من الأدوات المتاحة، وممنوع منعاً باتاً اختلاق أي رقم أو حالة من ذاكرتك. ' +
    'استعمل الأدوات لجلب ما تحتاجه ثم أجب بالعربية الواضحة بإيجاز مناسب للنطق الصوتي (جملتان إلى خمس جمل، ' +
    'وللإحاطات الصباحية أو الأسئلة المركبة حتى عشر جمل مرتبة بالأهم أولاً). ' +
    'عند سؤال عن الأولويات أو البرنامج: رتّبها بالأثر، واذكر المالك والإجراء الموصى به. ' +
    'خاطب المتحدث بلقبه إن ذُكر. أنت طبقة قيادة: تقترح ولا تنفّذ — التنفيذ يمر عبر اللوحة المحمية.';
  const messages = [{ role: 'user', content:
    `لمحة سريعة (استعمل الأدوات للتفاصيل): اليوم ${fmtDayAr(0)} — ${s.active} طلب نشط، ${s.online} موصل متصل، ` +
    `${(s.prOpen || []).length} أولوية مفتوحة.\n` + (staff ? `المتحدث: ${String(staff).slice(0, 60)}\n` : '') + `السؤال: ${q}` }];
  const base = { model: process.env.BRAIN_MODEL || 'claude-opus-5', max_tokens: 1200,
    thinking: { type: 'adaptive' }, system, tools: execToolDefs() };
  let j = await claudeCall({ ...base, messages });
  for (let round = 0; round < 4 && j.stop_reason === 'tool_use'; round++) {   // حلقة الأدوات — 4 جولات كحد أقصى
    const uses = (j.content || []).filter(c => c.type === 'tool_use');
    messages.push({ role: 'assistant', content: j.content });
    messages.push({ role: 'user', content: uses.map(u => ({
      type: 'tool_result', tool_use_id: u.id,
      content: JSON.stringify((() => { try { return EXEC_TOOLS[u.name] ? EXEC_TOOLS[u.name].fn(u.input || {}) : { error: 'أداة غير معروفة' }; }
        catch (e) { return { error: String(e.message || e) }; } })()),
    })) });
    j = await claudeCall({ ...base, messages });
  }
  const text = (j.content || []).filter(c => c.type === 'text').map(c => c.text).join(' ').trim();
  if (!text) throw new Error('claude empty');
  return text;
}

// ---------- HTTP: ملفات + REST للوحة العقل ----------
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.svg': 'image/svg+xml' };
const readBody = (req) => new Promise((res) => {
  let b = '', done = false; const fin = (v) => { if (!done) { done = true; res(v); } };
  req.on('data', c => { b += c; if (b.length > 1e6) { req.destroy(); fin({}); } });   // لا يعلّق الطلب عند تجاوز الحجم
  req.on('end', () => { try { fin(JSON.parse(b || '{}')); } catch { fin({}); } });
  req.on('error', () => fin({}));
});

async function handleHttp(req, res) {
  const url = new URL(req.url, 'http://x');
  const json = (code, obj) => { res.writeHead(code, { 'content-type': 'application/json', 'access-control-allow-origin': '*', 'access-control-allow-headers': 'content-type,x-api-key' }); res.end(JSON.stringify(obj)); };

  if (req.method === 'OPTIONS') return json(204, {});
  if (url.pathname === '/api/health')
    return json(200, { ok: true, drivers: drivers.size, online: onlineCount() });
  if (url.pathname === '/api/config')
    return json(200, { brainPanelUrl: BRAIN_PANEL_URL, hexKm: HEX_KM });

  // ===== 📺 شاشة عقل ديار (kiosk) — محمية برمز اللوحة OPS_PIN =====
  if (url.pathname === '/api/brain/summary' && req.method === 'GET') {
    if (!pinOk(req.headers['x-kiosk-pin'], OPS_PIN)) return json(401, { error: 'bad pin' });
    const s = brainSummary();
    return json(200, { ...s, priorities: brainPriorities(s), ai: Boolean(process.env.ANTHROPIC_API_KEY) });
  }
  if (url.pathname === '/api/brain/ask' && req.method === 'POST') {
    const b = await readBody(req);
    if (!pinOk(b.pin, OPS_PIN)) return json(401, { error: 'bad pin' });
    const q = String(b.q || '').slice(0, 300);
    const s = brainSummary();
    if (!q) return json(400, { error: 'q required' });
    if (process.env.ANTHROPIC_API_KEY && b.fast !== true) {
      try { return json(200, { answer: await askClaude(q, s, b.staff), source: 'claude', asOf: Date.now(), summary: s }); }
      catch (e) { console.warn('[📺] Claude تعذّر — إجابة محلية:', e.message); }
    }
    return json(200, { answer: brainAnswer(q, s, b.staff), source: 'local', asOf: Date.now(), summary: s });
  }
  // أولويات مفتوحة + مغلقة حديثاً + حمل الفريق — لأي واجهة (تلفاز/حاسوب/هاتف)
  if (url.pathname === '/api/brain/priorities' && req.method === 'GET') {
    if (!pinOk(req.headers['x-kiosk-pin'], OPS_PIN)) return json(401, { error: 'bad pin' });
    return json(200, { asOf: Date.now(), open: openPriorities(), counts: openCounts(),
      recentClosed: prClosed.slice(-20).reverse(), load: teamLoad(), staff: STAFF });
  }
  // إجراء على أولوية من أي واجهة: {pin, id, action: ack|start|resolve|reassign, who, note}
  if (url.pathname === '/api/brain/priority-action' && req.method === 'POST') {
    const b = await readBody(req);
    if (!pinOk(b.pin, OPS_PIN)) return json(401, { error: 'bad pin' });
    const p = prAction(String(b.id || ''), String(b.action || ''), b.who, b.note);
    return p ? json(200, { ok: true, priority: prPublic(p) }) : json(400, { error: 'إجراء أو معرّف غير صالح' });
  }

  // ===== REST للوحة العقل (مفتاح API) =====
  if (url.pathname.startsWith('/api/v1/')) {
    if (!pinOk(req.headers['x-api-key'], BRAIN_API_KEY)) return json(401, { error: 'bad api key' });

    // مواقع وحالة كل الموصلين — تستهلكها صفحات الطلبات/المكالمات في غرفة التشغيل
    if (url.pathname === '/api/v1/drivers' && req.method === 'GET')
      return json(200, { drivers: [...drivers.values()].map(d => ({ ...publicInfo(d), trail: undefined })) });

    // الطلبات النشطة ومساراتها — لصفحة الطلبات في غرفة التشغيل
    if (url.pathname === '/api/v1/orders' && req.method === 'GET')
      return json(200, { orders: activeOrdersList() });

    // 🎙 جسر التطبيق ⟵ تاليا: غرفة التشغيل تدفع طلب التطبيق هنا فتتولاه الموزّعة فوراً
    // {ref, title, dest:{lat,lng}} إنشاء (idempotent بالمرجع) · {ref, action:'cancelled'|'delivered'} مزامنة حالة
    if (url.pathname === '/api/v1/dispatch' && req.method === 'POST') {
      const b = await readBody(req);
      const ref = b.ref != null ? String(b.ref).slice(0, 40) : null;
      if (ref && b.action) {                                   // مزامنة حالة من التطبيق (إلغاء/تسليم خارجي)
        const oid = refIndex.get(ref), o = oid && orders.get(oid);
        if (!o) return json(404, { error: 'ref غير معروف' });
        if (b.action === 'cancelled' && !TERMINAL.has(o.status)) {
          cancelOffer(o);
          const prev = o.driverId;
          setOrder(o, { status: 'cancelled', offeredTo: null });
          if (prev) pushDriverOrder(prev);
          talyaFeed(`⚪ ${o.id}: أُلغي من تطبيق ديار (${ref}).`);
        } else if (b.action === 'delivered' && !TERMINAL.has(o.status)) {
          cancelOffer(o);
          if (o.status === 'new') { setOrder(o, { status: 'cancelled', offeredTo: null }); talyaFeed(`⚪ ${o.id}: سُلّم خارج المنظومة (${ref}) — أُغلق.`); }
          else { if (o.status === 'assigned') setOrder(o, { status: 'picked' });
                 if (o.status === 'picked') setOrder(o, { status: 'delivered' });
                 if (o.driverId) pushDriverOrder(o.driverId); }
        }
        return json(200, { ok: true, order: orderPublic(o) });
      }
      const lat = +b.dest?.lat, lng = +b.dest?.lng;
      if (!b.title || !Number.isFinite(lat) || !Number.isFinite(lng) || Math.abs(lat) > 90 || Math.abs(lng) > 180)
        return json(400, { error: 'title و dest{lat,lng} مطلوبة' });
      if (ref && refIndex.has(ref)) {                          // نفس الطلب وصل مرتين — لا ازدواج
        const ex = orders.get(refIndex.get(ref));
        if (ex) return json(200, { ok: true, dedup: true, order: orderPublic(ex) });
      }
      if (ACTIVE.size >= 5000) return json(429, { error: 'حد الطلبات النشطة' });
      const o = { id: 'ORD-' + (++orderSeq), ref, title: String(b.title).slice(0, 80),
        dest: { lat, lng }, driverId: null, driverName: null,
        status: 'new', offeredTo: null, etaMin: null, etaAt: null, riskLate: false,
        history: [{ st: 'new', at: Date.now(), d: 'التطبيق' }],
        createdAt: Date.now(), updatedAt: Date.now() };
      const gl = +b.origin?.lat, gg = +b.origin?.lng;           // موقع متجر التطبيق إن أُرسل — وإلا مطابقة بالاسم
      if (Number.isFinite(gl) && Number.isFinite(gg) && Math.abs(gl) <= 90 && Math.abs(gg) <= 180) o.origin = { lat: gl, lng: gg };
      linkStore(o);
      orders.set(o.id, o); ACTIVE.set(o.id, o);
      if (ref) refIndex.set(ref, o.id);
      statBump('created');
      setOrder(o, {});
      if (b.auto !== false) startDispatch(o);                  // 🧕 تاليا تعرضه على الأقرب فوراً
      talyaFeed(`📲 ${o.id}: وصل من تطبيق ديار${ref ? ` (رقم ${ref})` : ''} — أتولاه الآن.`);
      return json(200, { ok: true, order: orderPublic(o) });
    }

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
  // بث صوتي PTT: يُرحَّل لكل الأطراف (لا أرشيف ثقيل في الذاكرة — الضغط الخلفي في send يحمي البطيئين)
  if (m.t === 'voice' && typeof m.data === 'string' && m.data.length < 700_000) {   // ~بث قصير معقول
    broadcastAll({ t: 'voice', from, role, ts: Date.now(), mime: String(m.mime || 'audio/webm'), data: m.data, dur: +m.dur || null }, ws);
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
      send(ws, { t: 'snapshot', drivers: [...drivers.values()].map(publicInfo), orders: activeOrdersList(),
        closed: closedOrders.slice(-30), stores: [...stores.values()], priorities: openPriorities(), staff: STAFF });
      ws.on('message', (raw2) => {
        let m; try { m = JSON.parse(raw2); } catch { return; }
        if (handleShared(m, hello.name || 'العمليات', 'ops', ws)) return;
        // إجراء على أولوية من اللوحة/الشاشة: هوية → انتقال مشروع → بث → توثيق
        if (m.t === 'priority_action' && m.id && ['ack', 'start', 'resolve', 'reassign'].includes(m.action)) {
          prAction(String(m.id), m.action, String(m.who || hello.name || 'العمليات').slice(0, 40),
            m.note ? String(m.note).slice(0, 200) : undefined);
          return;
        }
        if (m.t === 'sos_clear' && drivers.has(m.id)) {          // إغلاق تنبيه الطوارئ
          const d = drivers.get(m.id); d.sos = false;
          broadcastAll({ t: 'sos_clear', id: m.id });
          brainEvent('sos_cleared', { driver: publicInfo(d) });
          prCloseByKey('sos:' + m.id, 'أُغلق الإنذار من العمليات', hello.name || 'العمليات');
          return;
        }
        if (m.t === 'announce' && m.text) {                      // إعلان من اللوحة — تبلغه تاليا للأجهزة
          broadcastAll({ t: 'announce', text: String(m.text).slice(0, 300), speak: m.speak !== false, from: 'تاليا — ديار' }, ws);
          return;
        }
        // ----- 🏪 المتاجر على الخريطة -----
        if (m.t === 'store_add' && m.name && Number.isFinite(+m.lat) && Number.isFinite(+m.lng)
            && Math.abs(+m.lat) <= 90 && Math.abs(+m.lng) <= 180 && stores.size < 200) {
          const s = { id: 'ST-' + (++storeSeq), name: String(m.name).slice(0, 40), lat: +m.lat, lng: +m.lng };
          stores.set(s.id, s);
          broadcastOps({ t: 'store', s });
          backupState();                                        // 💾 المتاجر تنجو من إعادة النشر
          return;
        }
        if (m.t === 'store_rename' && stores.has(m.id) && m.name) {
          const s = stores.get(m.id); s.name = String(m.name).slice(0, 40);
          broadcastOps({ t: 'store', s });
          backupState();
          return;
        }
        if (m.t === 'store_remove' && stores.has(m.id)) {
          stores.delete(m.id);
          broadcastOps({ t: 'store_removed', id: m.id });
          backupState();
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
          if (!d.online) { unindexDriver(m.id); drivers.delete(m.id); broadcastOps({ t: 'device_removed', id: m.id }); }
          return;
        }

        // ----- إدارة الطلبات -----
        if (m.t === 'order_create' && m.title && Number.isFinite(m.dest?.lat) && Number.isFinite(m.dest?.lng)
            && Math.abs(+m.dest.lat) <= 90 && Math.abs(+m.dest.lng) <= 180) {
          if (ACTIVE.size >= 5000) return;                        // حد أمان يمنع إغراق الذاكرة
          const o = { id: 'ORD-' + (++orderSeq), title: String(m.title).slice(0, 80),
            dest: { lat: +m.dest.lat, lng: +m.dest.lng }, driverId: null, driverName: null,
            status: 'new', offeredTo: null, etaMin: null, etaAt: null, riskLate: false,
            history: [{ st: 'new', at: Date.now(), d: null }],
            createdAt: Date.now(), updatedAt: Date.now() };
          linkStore(o);                                         // متجر مذكور بالعنوان ⟵ التقاط ثنائي الأرجل
          orders.set(o.id, o);
          ACTIVE.set(o.id, o);
          statBump('created');
          setOrder(o, {});
          if (m.auto !== false) startDispatch(o);                 // 🧕 تاليا تعرضه على الأقرب تلقائياً
          return;
        }
        const o = m.orderId && orders.get(m.orderId);
        if (m.t === 'order_assign' && o && !TERMINAL.has(o.status) && drivers.has(m.driverId)) {
          cancelOffer(o);                                        // الإسناد اليدوي يوقف عرض تاليا الجاري
          const prev = o.driverId;
          if (prev === m.driverId) return;                       // مُسنَد له أصلاً
          const d = drivers.get(m.driverId);
          setOrder(o, { driverId: d.id, driverName: d.name, status: 'assigned', offeredTo: null });
          pushDriverOrder(d.id);
          if (prev && prev !== d.id) pushDriverOrder(prev);      // أبلغ الموصل السابق أن الطلب سُحب
          talyaSay(d.id, `أُسند إليك الطلب ${o.id}: ${o.title}.`);
          return;
        }
        if (m.t === 'order_status' && o && ORDER_STATUSES.has(m.status)) {
          if (m.status !== o.status && !(NEXT_OK[o.status] || []).includes(m.status)) return;   // انتقال غير مشروع
          if (m.status === 'cancelled') cancelOffer(o);
          const prevDriver = o.driverId;
          setOrder(o, { status: m.status, ...(m.status === 'cancelled' ? { offeredTo: null } : {}) });
          if (prevDriver) pushDriverOrder(prevDriver);           // حدّث بطاقة الموصل (قد تكون أُخليت)
          return;
        }
        if (m.t === 'order_redispatch' && o && o.status === 'new' && !o._offer) { startDispatch(o); return; }
      });
      ws.on('close', () => opsClients.delete(ws));
      return;
    }

    // ===== جهاز موصل =====
    if (hello.role === 'driver' && hello.deviceId && hello.name) {
      const id = String(hello.deviceId).slice(0, 64);
      // إعادة اتصال: افصل المقبس القديم لنفس الجهاز بلا قلب الحالة إلى «غير متصل»
      const oldWs = driverToWs.get(id);
      if (oldWs && oldWs !== ws) { oldWs._superseded = true; driverClients.delete(oldWs); try { oldWs.close(4004, 'superseded'); } catch {} }
      const d = drivers.get(id) || { id, trail: [] };
      Object.assign(d, {
        name: String(hello.name).slice(0, 40),
        device: String(hello.device || 'غير معروف').slice(0, 60),
        online: true, lastSeen: Date.now(),
      });
      drivers.set(id, d);
      driverClients.set(ws, id);
      driverToWs.set(id, ws);
      onlineIds.add(id);
      if (d.last) indexDriver(id, d.last.lat, d.last.lng);     // عودة اتصال بموقع معروف ⟵ يعود للفهرس
      send(ws, { t: 'ok', id, channel: 'العمليات العامة', online: onlineCount(), order: driverActiveOrder(id) });
      broadcastOps({ t: 'driver', d: publicInfo(d) });
      brainEvent('driver_online', { driver: publicInfo(d) });
      pushStats();
      if (!driverBusy(id)) setImmediate(pumpQueue);              // موصل حرّ جديد → اصرف الطابور
      console.log(`[+] ${d.name} (${d.device}) متصل — الأجهزة: ${drivers.size}`);

      ws.on('message', (raw2) => {
        let m; try { m = JSON.parse(raw2); } catch { return; }
        if (handleShared(m, d.name, 'driver', ws)) return;

        if (m.t === 'gps' && Number.isFinite(m.lat) && Number.isFinite(m.lng)
            && Math.abs(+m.lat) <= 90 && Math.abs(+m.lng) <= 180) {   // إحداثيات صالحة فقط
          d.last = {
            lat: +m.lat, lng: +m.lng,
            acc: Number.isFinite(m.acc) ? Math.round(m.acc * 10) / 10 : null,
            spd: Number.isFinite(m.spd) ? Math.round(m.spd * 3.6) : null,
            hdg: Number.isFinite(m.hdg) ? Math.round(m.hdg) : null,
            bat: Number.isFinite(m.bat) ? Math.round(m.bat) : null,
            ts: Date.now(),
          };
          d.lastSeen = Date.now();
          indexDriver(id, d.last.lat, d.last.lng);             // الفهرس الحي: يتحدث ذاتياً مع كل نبضة
          const tail = d.trail[d.trail.length - 1];
          if (!tail || Math.abs(tail[0] - d.last.lng) > 1e-5 || Math.abs(tail[1] - d.last.lat) > 1e-5) {
            d.trail.push([d.last.lng, d.last.lat]);
            if (d.trail.length > 400) d.trail.shift();
          }
          broadcastOps({ t: 'gps', id, last: d.last, point: [d.last.lng, d.last.lat] });
        }

        if (m.t === 'sos') {                                     // زر الطوارئ
          if (!d.sos) statBump('sos');                           // يُحصى مرة واحدة لكل حالة، لا لكل ضغطة
          d.sos = true;
          broadcastAll({ t: 'sos', id, name: d.name, last: d.last });
          brainEvent('sos', { driver: publicInfo(d) });
          prOpen('sos:' + id, { type: 'sos', score: 95, driverId: id, source: 'device',
            title: `طوارئ من ${d.name}`, recommendedAction: 'اتصلوا بالموصل فوراً وتأكدوا من سلامته' });
          console.log(`[!] طوارئ من ${d.name}`);
        }

        // الموصل يحدّث حالة طلبه الجاري: استلمت / سلّمت — على طلبه النشط حصراً بانتقال مشروع
        if (m.t === 'order_status' && ['picked', 'delivered'].includes(m.status)) {
          const o = driverActiveOrderRaw(id);
          if (o && (m.orderId ? o.id === m.orderId : true) && (NEXT_OK[o.status] || []).includes(m.status)) {
            setOrder(o, { status: m.status }); pushDriverOrder(id);
          }
        }

        // رد الموصل على عرض تاليا: قبول أو رفض
        if (m.t === 'offer_answer' && m.orderId && orders.has(m.orderId)) {
          answerOffer(orders.get(m.orderId), id, m.accept === true);
        }
      });

      ws.on('close', () => {
        driverClients.delete(ws);
        // 🛡️ لا تقلب الحالة إلى «غير متصل» إن كان الجهاز أعاد الاتصال بمقبس أحدث
        if (ws._superseded || driverToWs.get(id) !== ws) return;
        driverToWs.delete(id);
        onlineIds.delete(id);
        unindexDriver(id);                                     // خرج من الفهرس — لا يُعرض عليه شيء
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
// فشل الاستماع (منفذ مشغول/صلاحيات) قاتل — لا نتركه لحارس uncaught فيبقى المسار «حيّاً» بلا خدمة
server.on('error', (e) => { console.error('[fatal] تعذر الاستماع:', e.message); process.exit(1); });
server.listen(PORT, () => {
  const proto = useTls ? 'https' : 'http';
  const lans = Object.values(networkInterfaces()).flat()
    .filter(i => i && i.family === 'IPv4' && !i.internal).map(i => i.address);
  console.log('──────────────────────────────────────────────');
  console.log('  Dyar Connect Gateway يعمل ✓');
  console.log(`  لوحة التحكم:  ${proto}://localhost:${PORT}/`);
  for (const ip of lans) console.log(`  من الشبكة:    ${proto}://${ip}:${PORT}/`);
  console.log(`  صفحة السائق:  ${proto}://<العنوان>:${PORT}/driver.html`);
  console.log(`  شاشة المكتب:  ${proto}://<العنوان>:${PORT}/kiosk.html  (عقل ديار التنفيذي — ${process.env.ANTHROPIC_API_KEY ? 'استدلال Claude ⚡' : 'استدلال محلي'})`);
  console.log(`  رمز الأجهزة PIN: ${PIN}${OPS_PIN === PIN ? '  (⚠ اضبط OPS_PIN منفصلاً للوحة في الإنتاج)' : '  · رمز اللوحة OPS_PIN: مضبوط ✓'}`);
  console.log(`  لوحة العقل:    ${BRAIN_PANEL_URL}`);
  console.log(`  REST للعقل:    GET /api/v1/drivers · POST /api/v1/announce  (x-api-key)`);
  if (!useTls) console.log('  تنبيه: GPS والمايك من الأجهزة يتطلبان HTTPS — docs/quickstart.md');
  console.log('──────────────────────────────────────────────');
  registerWithBrain();   // ربط ذاتي فوري بغرفة التشغيل
  restoreState();        // 💾 استعادة المتاجر والإحصاء من النسخة الاحتياطية (إن وُجدت)
});
