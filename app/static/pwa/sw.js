/**
 * Employee Portal PWA – minimal service worker.
 * Enables "Add to home screen" and app-like launch. No offline cache (always network).
 */
const CACHE_NAME = 'portal-v1';

self.addEventListener('install', (event) => {
  self.skipWaiting();
});

self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.map((k) => (k !== CACHE_NAME ? caches.delete(k) : Promise.resolve())))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', (event) => {
  // Network-only: no caching. Keeps installability without stale content.
  event.respondWith(fetch(event.request));
});
