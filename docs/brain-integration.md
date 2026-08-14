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

## 4. متغيرات البيئة

| المتغير | الافتراضي | الوظيفة |
|---|---|---|
| `BRAIN_API_KEY` | `dyar-brain-key` | مفتاح REST — **غيّره في الإنتاج** |
| `BRAIN_WEBHOOK_URL` | (فارغ = معطل) | عنوان استقبال الأحداث في غرفة التشغيل |
| `BRAIN_PANEL_URL` | `https://egint-support.onrender.com` | رابط زر «لوحة العقل» في الواجهة |

## 5. الخطوة التالية للدمج العميق

عندما نصل لمرحلة الموزّع الذكي (docs/ai-dispatcher.md): غرفة التشغيل ترسل الطلب الجديد
إلى `POST /api/v1/dispatch` (يُبنى لاحقًا)، وConnect يتولى اختيار أقرب موصل ونداءه صوتيًا
وإرجاع نتيجة الإسناد عبر الـWebhook نفسه — فتصبح شاشة الطلبات في العقل هي المُشغّل، وConnect هو الصوت والموقع.
