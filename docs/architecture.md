# البنية التقنية التفصيلية — Dyar Connect

كل مكوّن هنا مفتوح المصدر، مجاني، ويُستضاف على خوادمنا. لا يوجد أي بند "لكل مستخدم شهريًا".

## 1. المخطط العام

```
 [UR5 / Android]   [iPhone]   [لوحة التحكم Web]
       │ Kotlin        │ Swift        │ React
       └───────┬───────┴──────┬───────┘
               │              │
        WebRTC (Opus)   WebSocket (JSON)
               │              │
       ┌───────▼──────┐ ┌─────▼──────────┐
       │   LiveKit    │ │ Dyar Realtime  │
       │  SFU  الصوت  │ │    Gateway     │
       │ (self-host)  │ │ Node.js/TS     │
       └───────┬──────┘ │ حضور/رسائل/GPS │
               │        └─────┬──────────┘
          تسجيل البث           │
               │        ┌─────┴─────┬──────────┬─────────┐
       ┌───────▼──┐ ┌───▼───┐ ┌────▼─────┐ ┌──▼──────────┐
       │  MinIO   │ │ Redis │ │PostgreSQL│ │ Dyar Brain  │
       │ الأرشيف  │ │ لحظي  │ │  دائم    │ │ AI + الطلبات│
       └──────────┘ └───────┘ └──────────┘ └─────────────┘
                                    ▲
                          faster-whisper (تفريغ صوتي محلي)
```

مكوّن مساند: **coturn** ‏(TURN/STUN) لعبور NAT، و**FCM/APNs** لإيقاظ الأجهزة.

## 2. مسار الصوت (PTT)

1. عند فتح التطبيق ينضم الجهاز لغرفة LiveKit الخاصة بقنواته، **مكتوم** ولا يستهلك رفع بيانات.
2. ضغط الزر ⟵ طلب "حجز الكلام" من الـ Gateway عبر WebSocket.
3. الـ Gateway يطبق قواعد الأولوية (§4) ويمنح الإذن خلال ميليثوانٍ ⟵ الجهاز يفعّل مساره الصوتي (unmute + publish).
4. ترحيل فوري لكل أعضاء القناة عبر SFU (لا مزج على الخادم ⟵ زمن استجابة أدنى).
5. ترك الزر ⟵ كتم + تحرير الحجز. بالتوازي يُنسخ البث إلى MinIO (Opus خام، ~2KB/ثانية).
6. Whisper يفرّغ التسجيل نصيًا في الخلفية ويفهرسه للبحث، ويمرر النص لـ Dyar Brain لتحليل النية.

**الهدف القياسي:** ضغط ⟶ سماع < 500ms على 4G. ‏Opus بمعدل 16kbps mono للكلام (جودة ممتازة، ساعة صوت ≈ 7MB).

## 3. بروتوكول الـ Gateway (WebSocket)

رسائل JSON موقّعة بجلسة JWT من نظام Dyar الحالي:

```
C→S  auth        {token}
C→S  ptt_request {channel, priority}
S→C  ptt_grant   {channel, room, ttl}   | ptt_deny {reason, holder}
C→S  ptt_release {channel}
C→S  gps         {lat, lng, speed, heading, battery, net}   كل 5–15ث حسب الحركة
S→C  presence    {user, state}          online/busy/offline
C↔S  msg         {channel|user, kind: text|image|voice, payload}
S→C  emergency   {user, location, stream}
```

الحضور: Redis يحفظ `presence:{userId}` مع TTL ‏30ث؛ انقطاع القلب (heartbeat) يقلب الحالة تلقائيًا ويبثها للوحة.
مواقع GPS تُبث للوحة عبر نفس القناة وتُعيَّن (sample) إلى PostgreSQL كل دقيقة لسجل المسارات.

## 4. الأولويات (Priority Voice)

```
P1 عادي        ⟵ طابور FIFO داخل القناة
P2 مشكلة طلب   ⟵ يتقدم الطابور، إشعار Dispatcher، يُربط بـ order_id
P3 طوارئ (SOS) ⟵ revoke فوري لأي بث جارٍ، بث مفتوح من السائق،
                  فتح GPS مباشر، تسجيل إجباري، تنبيه كل المشرفين
```

قطع البث في P3 يتم بإلغاء إذن النشر من LiveKit فورًا (server-side mute) — لا يعتمد على تعاون جهاز المتكلم.

## 5. قاعدة البيانات (مخطط مبدئي)

```sql
users(id, dyar_user_id, role, display_name, region, active)        -- مرآة لحساب Dyar
devices(id, user_id, kind{android,ios,ur5,web}, model, app_ver, push_token, last_seen)
channels(id, name, type{static,order,emergency}, region, settings jsonb)
channel_members(channel_id, user_id | rule jsonb)                  -- عضوية صريحة أو قاعدة ("كل سائقي البعنة")
transmissions(id, channel_id, user_id, started_at, dur_ms, priority,
              object_key, transcript, intent, order_id)
messages(id, channel_id|peer_id, user_id, kind, payload, created_at)
locations(user_id, at, lat, lng, speed, heading, battery, net)     -- مُقسّمة شهريًا (partition)
sessions(id, user_id, device_id, ip, signed_in_at, signed_out_at, reason)   -- تقرير الدخول
audit_log(id, actor_id, action, target, meta jsonb, at)            -- append-only
incidents(id, user_id, order_id, kind{delay,sos,...}, opened_at, closed_at, closed_by)
```

## 6. تكامل الطلبات (Dyar Brain)

- الـ Gateway يسأل خدمة الطلبات: "ما الطلب الجاري للسائق X؟" ⟵ يُرفق `order_id` بكل بث تلقائيًا.
- تحليل النية بعد التفريغ: `RESTAURANT_DELAY / WRONG_ADDRESS / ACCIDENT / CUSTOMER_UNREACHABLE …`
- أفعال آلية: تحديث ETA، تسجيل حادثة، إنشاء قناة `ORDER-xxxxx`، رد صوتي مُولّد (TTS) للسائق.
- كل فعل آلي يُسجّل في audit_log باسم `ai-dispatcher` ويقبل التراجع اليدوي.

## 7. تطبيقات الأجهزة

**Android / UR5 ‏(Kotlin, minSdk 26):**
- ‏Foreground Service دائم (صوت + WebSocket + GPS) مع إشعار ثابت.
- التقاط زر PTT الفيزيائي: تجربة KeyEvent codes وBroadcast Intents الخاصة بـ Siyata أول أسبوع (مهمة استكشاف).
- نسخة UR5: بدون خرائط، شاشة واحدة، خطوط كبيرة، استهلاك ذاكرة مستهدف < 150MB.
- تحديث ذاتي: فحص إصدار من الخادم وتنزيل APK — لا حاجة لـ Google Play.

**iPhone ‏(Swift):** إطار **PushToTalk** الرسمي (iOS 16+) — مصمم تحديدًا لتطبيقات PTT في الخلفية، مع زر النظام الأزرق المعروف.

**لوحة التحكم (React):** ‏PWA، خرائط MapLibre، صوت المتصفح عبر LiveKit JS SDK (مسطرة = PTT).

## 8. النشر والتشغيل

- ‏Docker Compose على خادم واحد بداية: `livekit, coturn, gateway, postgres, redis, minio, whisper-worker, caddy`.
- ‏Caddy/Traefik لـ TLS تلقائي (Let's Encrypt — مجاني).
- مراقبة: Prometheus + Grafana (مجاني) — زمن الاستجابة، الغرف النشطة، معدل فقد الحزم.
- نسخ احتياطي يومي: `pg_dump` + مزامنة MinIO إلى تخزين خارجي رخيص.
- التوسع لاحقًا: LiveKit يدعم توزيع الغرف على عدة عقد عند الحاجة — لا إعادة كتابة.

## 9. الأمان

- كل الاتصالات TLS/WSS/SRTP (الصوت مشفر أثناء النقل).
- JWT قصيرة العمر من نظام مصادقة Dyar الحالي؛ إبطال فوري للجلسات من لوحة التحكم.
- صلاحيات القنوات تُفرض على الخادم (الـ Gateway هو الحكم، لا التطبيق).
- سجل تدقيق لكل وصول للأرشيف — من استمع لماذا ومتى.
