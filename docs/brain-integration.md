# دمج Dyar Connect مع لوحة العقل (غرفة التشغيل)

لوحة العقل: **ديار — غرفة التشغيل** (`https://egint-support.onrender.com` — الرئيسية، المكالمات، الطلبات، العملاء).
الدمج ثلاثي الاتجاهات:

```
 غرفة التشغيل (Render) ◄──── REST ────  Dyar Connect Gateway
        ▲                                      │
        └────────── Webhook (أحداث) ───────────┘
 غرفة التشغيل ──── Announce ────► أجهزة الموصلين (نص + نطق صوتي)
```

ولوحة تحكم Connect فيها زر **«🧠 لوحة العقل ↗»** يفتح غرفة التشغيل مباشرة (يُضبط بـ `BRAIN_PANEL_URL`).

## 1. REST — العقل يسحب حالة الموصلين لحظيًا

كل الطلبات بترويسة `x-api-key` (قيمة `BRAIN_API_KEY` من بيئة الخادم):

```
GET /api/v1/drivers
→ { "drivers": [ { "id", "name", "device", "online", "sos",
                   "last": { "lat", "lng", "acc", "spd", "hdg", "bat", "ts" } } ] }
```

مثال من كود غرفة التشغيل (لصفحة الطلبات — أقرب موصل لطلب):

```js
const r = await fetch('https://connect.dyar.tv/api/v1/drivers', {
  headers: { 'x-api-key': process.env.CONNECT_KEY }
});
const { drivers } = await r.json();   // مواقع حية بدقة بالمتر
```

## 2. Announce — العقل يتكلم لأجهزة الموصلين

```
POST /api/v1/announce
Body: { "text": "مطعم الشام أغلق الطلبات مؤقتًا", "speak": true }
```

يظهر النص على كل جهاز موصل فورًا، **ويُنطق صوتيًا** بمحرك النطق العربي في الجهاز (`speak:true`).
هذا أساس «AI يتكلم عبر اللاسلكي» — لاحقًا يستبدل نطقُ الخادم (TTS) نطقَ الجهاز.

## 3. Webhook — الأحداث تُدفع للعقل

اضبط `BRAIN_WEBHOOK_URL` في بيئة الخادم؛ يصل POST بصيغة:

```json
{ "event": "driver_online" | "driver_offline" | "sos" | "sos_cleared",
  "at": 1755180000000,
  "driver": { "id", "name", "device", "online", "sos", "last": { ... } } }
```

أنشئ في تطبيق Render مسارًا مثل `POST /api/connect-hook` يستقبلها ويحدّث
شاشات الطلبات/المكالمات (طوارئ موصل ⟵ تنبيه في غرفة التشغيل فورًا).

## 4. الربط الذاتي (Zero-Config) — لا إعداد يدوي

عند النشر على Render لا تحتاج ضبط أي شيء في خدمة غرفة التشغيل:

1. خدمة Connect تعرف عنوانها العلني تلقائيًا (`RENDER_EXTERNAL_URL`) ومفتاحها (`BRAIN_API_KEY` المولّد من الـblueprint).
2. عند إقلاعها (ثم كنبض كل 10 دقائق) ترسل `{url, key}` إلى `POST /webhooks/dyar-connect-register` في غرفة التشغيل.
3. غرفة التشغيل تقبل **أول ربط** (Trust-On-First-Use)، تحفظه في مخزن الربط الدائم، توثقه في سجل التدقيق،
   **وتنبه الأدمن** («ارتبطت خدمة Dyar Connect — إن لم يكن متوقعًا امسحها من الربط والمفاتيح»).
4. بعد الربط لا يُقبل إلا المفتاح المطابق (timing-safe)، ومتغيرات البيئة إن ضُبطت تتقدم على المسجَّل دائمًا.

## 5. متغيرات البيئة (اختيارية — تتقدم على الربط الذاتي)

| المتغير | الجهة | الافتراضي | الوظيفة |
|---|---|---|---|
| `BRAIN_API_KEY` | Connect | `dyar-brain-key` | المفتاح المشترك — يولّده Render تلقائيًا |
| `BRAIN_PANEL_URL` | Connect | `https://egint-support.onrender.com` | رابط غرفة التشغيل (زر «العقل» + وجهة التسجيل) |
| `BRAIN_WEBHOOK_URL` | Connect | يُشتق من `BRAIN_PANEL_URL` | وجهة دفع الأحداث |
| `PUBLIC_URL` | Connect | `RENDER_EXTERNAL_URL` | العنوان العلني للتسجيل الذاتي |
| `CONNECT_URL` / `CONNECT_API_KEY` | غرفة التشغيل | عبر التسجيل الذاتي | ضبط يدوي يتجاوز التسجيل |

## 5. الخطوة التالية للدمج العميق

عندما نصل لمرحلة الموزّع الذكي (docs/ai-dispatcher.md): غرفة التشغيل ترسل الطلب الجديد
إلى `POST /api/v1/dispatch` (يُبنى لاحقًا)، وConnect يتولى اختيار أقرب موصل ونداءه صوتيًا
وإرجاع نتيجة الإسناد عبر الـWebhook نفسه — فتصبح شاشة الطلبات في العقل هي المُشغّل، وConnect هو الصوت والموقع.
