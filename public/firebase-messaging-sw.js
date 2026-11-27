importScripts('https://www.gstatic.com/firebasejs/10.13.2/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/10.13.2/firebase-messaging-compat.js');

firebase.initializeApp({
  apiKey: 'AIzaSyC2X7W_nycElgVRAtO_8B2T3xVbnDtDUhw',
  authDomain: 'fxh-synthetic-alerts.firebaseapp.com',
  projectId: 'fxh-synthetic-alerts',
  storageBucket: 'fxh-synthetic-alerts.firebasestorage.app',
  messagingSenderId: '1022304339792',
  appId: '1:1022304339792:web:a5647cd988b69112420e55',
});

const messaging = firebase.messaging();

messaging.onBackgroundMessage((payload) => {
  if (payload && payload.notification) return;
  const data = (payload && payload.data) || {};
  return self.registration.showNotification(data.title || '🚨 FXH Price Alert Hit!', {
    body: data.body || 'Your target level was reached.',
    icon: '/icons/Icon-192.png',
    badge: '/icons/Icon-192.png',
    tag: data.symbol || 'fxh-alert',
    renotify: true,
    data: data,
  });
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  event.waitUntil(
    clients.matchAll({ type: 'window', includeUncontrolled: true }).then((clientList) => {
      for (const client of clientList) {
        if ('focus' in client) {
          return client.focus();
        }
      }
      if (clients.openWindow) {
        return clients.openWindow('/');
      }
    })
  );
});
