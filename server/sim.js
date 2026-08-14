// محاكي أجهزة — يوصل 4 موصلين وهميين يتحركون بإحداثيات حقيقية في منطقة الجليل
// تشغيل:  npm run sim          (والخادم يعمل في نافذة أخرى)
// بيئة:   SERVER=ws://localhost:8080/ws  DYAR_PIN=1234

import WebSocket from 'ws';

const SERVER = process.env.SERVER || 'ws://localhost:8080/ws';
const PIN = process.env.DYAR_PIN || '1234';

// نقاط انطلاق حقيقية: البعنة، دير الأسد، مجد الكروم، كرمئيل
const SIM = [
  { name: 'محمد (محاكاة)', lat: 32.9286, lng: 35.2683, bat: 72 },
  { name: 'أحمد (محاكاة)', lat: 32.9339, lng: 35.2702, bat: 88 },
  { name: 'سارة (محاكاة)', lat: 32.9182, lng: 35.2560, bat: 64 },
  { name: 'يوسف (محاكاة)', lat: 32.9171, lng: 35.3050, bat: 55 },
];

for (const [i, d] of SIM.entries()) {
  const ws = new WebSocket(SERVER, { rejectUnauthorized: false });
  let hdg = Math.random() * 360;
  ws.on('open', () => {
    ws.send(JSON.stringify({ t: 'hello', role: 'driver', name: d.name, pin: PIN,
      deviceId: 'sim-' + i, device: 'محاكي' }));
    setInterval(() => {
      hdg += (Math.random() - 0.5) * 40;                       // انعطافات عشوائية
      const spd = 6 + Math.random() * 8;                        // ~20–50 كم/س
      d.lat += Math.cos(hdg * Math.PI/180) * spd * 2 / 111_000; // خطوة كل ثانيتين
      d.lng += Math.sin(hdg * Math.PI/180) * spd * 2 / (111_000 * Math.cos(d.lat * Math.PI/180));
      d.bat = Math.max(5, d.bat - 0.01);
      ws.send(JSON.stringify({ t: 'gps', lat: d.lat, lng: d.lng,
        acc: 3 + Math.random() * 9, spd, hdg: ((hdg % 360) + 360) % 360, bat: d.bat }));
    }, 2000);
  });
  ws.on('close', () => console.log(d.name, 'انقطع'));
  ws.on('error', e => console.error(d.name, e.message));
}
console.log('محاكي يرسل 4 موصلين إلى', SERVER);
