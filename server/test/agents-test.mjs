// 🧠 اختبار طبقة الوكلاء: عقل العملاء (سارة) + الشخصيات + قاعدة المعرفة — مسار السقوط الآمن (بلا مفتاح Claude)
const P = process.env.DP || '8230';
const BASE = 'http://127.0.0.1:' + P;
const KEY = 'dyar-brain-key';
let n = 0, fails = 0;
const ck = (name, ok, x) => { n++; if (!ok) fails++; console.log((ok ? '  ✓ ' : '  ✗ ') + name + (x ? ` [${String(x).slice(0,70)}]` : '')); };
const ask = (q) => fetch(BASE + '/api/track/ask', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ q }) }).then(r => r.json());
const brainAsk = (q) => fetch(BASE + '/api/brain/ask', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ pin: '1234', q }) }).then(r => r.json());

console.log('— عقل العملاء (سارة) — سقوط آمن على قاعدة المعرفة —');
const a1 = await ask('شو مناطق التغطية عندكم؟');
ck('يجيب عن التغطية من قاعدة المعرفة', /البعنة|دير الاسد|دير الأسد|كرمئيل|نغط/.test(a1.answer), a1.answer);
const a2 = await ask('كيف أطلب من ديار؟');
ck('يجيب عن كيفية الطلب', /تطبيق ديار|اختر|متجرك/.test(a2.answer), a2.answer);
const a3 = await ask('عندي شكوى على طلبي المتأخر');
ck('يوجّه الشكوى بشكل صحيح', /شكوى|نعالج|رقم طلب|نور/.test(a3.answer), a3.answer);
const a4 = await ask('سؤال غريب لا علاقة له بشيء إطلاقا xyz');
ck('سؤال بلا تطابق ⟵ ترحيب + توجيه (لا اختلاق)', /رقم طلب|مكتب ديار|ديار/.test(a4.answer), a4.answer);
ck('كل ردود سارة تحمل اسمها', a1.by === 'سارة', a1.by);

console.log('— تتبّع مباشر برقم (يبقى يعمل) —');
// أنشئ طلب تطبيق لنتتبّعه
await fetch(BASE + '/api/v1/dispatch', { method: 'POST', headers: { 'x-api-key': KEY, 'content-type': 'application/json' },
  body: JSON.stringify({ ref: '9501', title: 'طلب تجربة', dest: { lat: 32.94, lng: 35.27 } }) });
await new Promise(r => setTimeout(r, 500));
const t1 = await ask('وين طلبي 9501؟');
ck('تتبّع مباشر برقم الطلب', /9501/.test(t1.answer) && t1.order, t1.answer);

console.log('— توجيه الشخصيّات الداخليّة (بلا مفتاح ⟵ يسقط للمحلي بلا انهيار) —');
const p1 = await brainAsk('جاوب كالتسويق: شو حملة اليوم؟');
ck('سؤال تسويق يُعالَج بلا انهيار', p1.answer && p1.answer.length > 5, p1.source);
const p2 = await brainAsk('كخدمة العملاء: كيف نرضي عميل زعلان؟');
ck('سؤال خدمة عملاء يُعالَج بلا انهيار', p2.answer && p2.answer.length > 5, p2.source);

console.log('— حماية النقطة العامّة —');
const st = [];
for (let i = 0; i < 20; i++) st.push((await fetch(BASE + '/api/track/ask', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ q: 'test ' + i }) })).status);
ck('محدّد المعدل يحمي /api/track/ask (429 بعد التجاوز)', st.includes(429), st.filter(x=>x===429).length + ' مرة 429');

console.log(`\n${fails === 0 ? '🟢 طبقة الوكلاء تعمل (مسار السقوط الآمن)' : '🔴 إخفاقات'}: ${n - fails}/${n}`);
process.exit(fails ? 1 : 0);
