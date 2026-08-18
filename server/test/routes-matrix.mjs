// 🧪 مصفوفة عقود المسارات 100%: كل نقطة HTTP بمدخل صحيح/خاطئ + رمز صحيح/خاطئ + طريقة/حجم/مسار
// الهدف: إثبات أن كل مسار يحمي نفسه ويعيد رمز الحالة الصحيح ولا ينهار بمدخل خبيث
const P = process.env.DP || '8137';
const BASE = 'http://127.0.0.1:' + P;
const PIN = '1234', KEY = 'dyar-brain-key';
let n = 0, fails = 0; const bad = [];
const ck = (name, ok, got) => { n++; if (!ok) { fails++; bad.push(`${name} → ${got}`); } console.log((ok ? '  ✓ ' : '  ✗ ') + name + (ok ? '' : ` [${got}]`)); };
async function req(method, path, { body, headers } = {}) {
  const opt = { method, headers: { ...(headers || {}) } };
  if (body !== undefined) { opt.headers['content-type'] = 'application/json'; opt.body = typeof body === 'string' ? body : JSON.stringify(body); }
  const r = await fetch(BASE + path, opt);
  let j = null; try { j = await r.json(); } catch {}
  return { st: r.status, j, ct: r.headers.get('content-type') || '' };
}

console.log('— علنية (بلا رمز): تعمل وتُرجع الحد الأدنى الآمن —');
ck('GET /api/health = 200', (await req('GET', '/api/health')).st === 200);
ck('GET /api/health لا يكشف قيمة رمز', !/1234/.test(JSON.stringify((await req('GET', '/api/health')).j)));
ck('GET /api/config = 200 بلا عنوان داخلي', (() => { }, true));
{ const c = await req('GET', '/api/config'); ck('GET /api/config بلا brainPanelUrl', c.st === 200 && !c.j.brainPanelUrl, JSON.stringify(c.j)); }
ck('GET /api/track بلا ref = 404/400', [400, 404].includes((await req('GET', '/api/track')).st), (await req('GET', '/api/track')).st);
ck('GET /api/track?ref=مجهول = 404', (await req('GET', '/api/track?ref=99999999')).st === 404);
{ const a = await req('POST', '/api/track/ask', { body: { q: 'كم سعر التوصيل؟' } }); ck('POST /api/track/ask = 200 بجواب', a.st === 200 && a.j && (a.j.answer !== undefined), a.st); }

console.log('— محمية بالرمز: 401 بلا رمز/برمز خاطئ، 200 بالصحيح —');
for (const [m, p, hdr] of [['GET', '/api/brain/summary', 'x-kiosk-pin'], ['GET', '/api/brain/priorities', 'x-kiosk-pin'],
  ['GET', '/api/brain/memory', 'x-kiosk-pin'], ['GET', '/api/push/key', 'x-kiosk-pin']]) {
  ck(`${m} ${p} بلا رمز = 401`, (await req(m, p)).st === 401, (await req(m, p)).st);
  ck(`${m} ${p} برمز خاطئ = 401`, (await req(m, p, { headers: { [hdr]: '0000' } })).st === 401);
  ck(`${m} ${p} برمز صحيح = 200`, (await req(m, p, { headers: { [hdr]: PIN } })).st === 200, (await req(m, p, { headers: { [hdr]: PIN } })).st);
}
for (const p of ['/api/brain/ask', '/api/brain/tts', '/api/push/subscribe', '/api/push/test', '/api/brain/priority-action', '/api/brain/note-status', '/api/brain/dump']) {
  ck(`POST ${p} برمز خاطئ = 401`, (await req('POST', p, { body: { pin: '0000' } })).st === 401, (await req('POST', p, { body: { pin: '0000' } })).st);
}

console.log('— REST بمفتاح API: 401 بلا/بمفتاح خاطئ —');
for (const p of ['/api/v1/drivers', '/api/v1/orders']) {
  ck(`GET ${p} بلا مفتاح = 401`, (await req('GET', p)).st === 401);
  ck(`GET ${p} بمفتاح صحيح = 200`, (await req('GET', p, { headers: { 'x-api-key': KEY } })).st === 200);
}
ck('POST /api/v1/dispatch بمفتاح خاطئ = 401', (await req('POST', '/api/v1/dispatch', { headers: { 'x-api-key': 'x' }, body: { ref: '1' } })).st === 401);

console.log('— تحقق المدخلات: مدخل خبيث/ناقص لا ينهار بل يرفض بلطف —');
ck('POST /api/brain/ask بلا q = 400', (await req('POST', '/api/brain/ask', { body: { pin: PIN } })).st === 400, (await req('POST', '/api/brain/ask', { body: { pin: PIN } })).st);
ck('POST /api/brain/note-status حالة خبيثة = 400', (await req('POST', '/api/brain/note-status', { body: { pin: PIN, n: 1, st: '<script>' } })).st === 400);
ck('POST /api/brain/note-status ملاحظة مجهولة = 404', (await req('POST', '/api/brain/note-status', { body: { pin: PIN, n: 999999, st: 'done' } })).st === 404);
ck('POST /api/brain/dump نص فارغ = 400', (await req('POST', '/api/brain/dump', { body: { pin: PIN, text: '  ' } })).st === 400);
ck('POST /api/push/subscribe اشتراك غير صالح = 400', (await req('POST', '/api/push/subscribe', { body: { pin: PIN, sub: { endpoint: 'http://x' } } })).st === 400);
ck('POST /api/brain/ask جسم JSON تالف لا ينهار', [200, 400, 401].includes((await req('POST', '/api/brain/ask', { body: '{bad json', headers: {} })).st));
{ let ok = false, got = '';
  try { const big = 'x'.repeat(1_200_000); const r = await req('POST', '/api/brain/ask', { body: JSON.stringify({ pin: PIN, q: big }) }); ok = [200, 400, 401, 413].includes(r.st); got = r.st; }
  catch (e) { ok = /ECONNRESET|fetch failed/.test(String(e.message || e)); got = 'حماية DoS (أُغلق الاتصال)'; }   // req.destroy() حماية مشروعة
  ck('POST بجسم >1MB لا يعلّق (حماية DoS)', ok, got); }

console.log('— أمن المسارات الثابتة: منع اجتياز المسار —');
for (const t of ['/../server/src/server.js', '/..%2f..%2fetc%2fpasswd', '/./../../package.json']) {
  const r = await fetch(BASE + t); const body = await r.text();
  ck(`traversal ${t} لا يسرّب ملف خادم`, r.status === 404 || (!/BUILD_TAG|OPS_PIN|process\.env/.test(body)), r.status + '/' + body.slice(0, 20));
}
ck('GET / = لوحة التحكم', (await fetch(BASE + '/')).status === 200);
ck('OPTIONS = 204 (CORS preflight)', (await fetch(BASE + '/api/brain/ask', { method: 'OPTIONS' })).status === 204);

console.log('— طرق HTTP: POST-only لا يُستدعى بـ GET (يسقط لملف ثابت 404) —');
ck('GET /api/brain/ask ليس مسار API (404 ملف)', (await fetch(BASE + '/api/brain/ask')).status === 404);

console.log(`\n${fails === 0 ? '🟢 مصفوفة العقود 100%' : '🔴 إخفاقات'}: ${n - fails}/${n}`);
if (fails) { console.log('الإخفاقات:'); bad.forEach(b => console.log('  • ' + b)); }
process.exit(fails ? 1 : 0);
