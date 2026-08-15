// عامل خدمة ديار: استقبال تنبيهات الهاتف (Web Push) وفتح اللوحة عند النقر
self.addEventListener('install', () => self.skipWaiting());
self.addEventListener('activate', (e) => e.waitUntil(self.clients.claim()));

self.addEventListener('push', (e) => {
  let d = {};
  try { d = e.data.json(); } catch { d = { title: 'ديار', body: e.data ? e.data.text() : '' }; }
  e.waitUntil(self.registration.showNotification(d.title || 'ديار', {
    body: d.body || '',
    tag: d.tag || undefined,
    renotify: true,
    dir: 'rtl', lang: 'ar',
    icon: '/icon.svg', badge: '/icon.svg',
    vibrate: [200, 100, 200],
  }));
});

self.addEventListener('notificationclick', (e) => {
  e.notification.close();
  e.waitUntil(self.clients.matchAll({ type: 'window', includeUncontrolled: true }).then((ws) => {
    for (const w of ws) if ('focus' in w) return w.focus();
    return self.clients.openWindow('/');
  }));
});
