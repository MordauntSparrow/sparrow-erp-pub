# Code canvas: Employee portal PWA + Web Push (Flask server-rendered)

**Stack assumption:** Sparrow ERP **website Flask app** — Jinja templates, session-based **contractor** auth, blueprint `public_employee_portal` at `/employee-portal`. No separate SPA owns this surface; PWA assets and APIs are **first-class Flask routes + static files** under the same origin as the portal.

**Non-goals:** PWA/service worker for admin or unrelated blueprints. Do not register SW globally from `create_app` unless scoped by URL in the SW script itself.

---

## 1. Problem statement

- Users install a **PWA** from the **employee portal entry** (login/dashboard), not from arbitrary Sparrow URLs.
- Installed app should cover **portal + linked module views** (scheduling, todos, assignments, etc.) within **same-origin + safe `scope`** constraints.
- **Web Push** for work events: todos, shifts, assignments, messages — **payloads must stay generic** (no sensitive detail in cleartext notification body).

---

## 2. Flask-side architecture (canonical)

```mermaid
flowchart TB
  subgraph browser [Browser PWA scope]
    SW[service-worker.js]
    Pages[Jinja pages under scope]
  end
  subgraph flask [Flask app same origin]
    M["GET /employee-portal/manifest.webmanifest"]
    SWroute["GET /employee-portal/sw.js"]
    Sub["POST /employee-portal/api/push/subscribe"]
    Unsub["POST /employee-portal/api/push/unsubscribe"]
    Prefs["POST /employee-portal/api/push/preferences"]
  end
  subgraph data [Persistence]
    DB[(push_subscriptions + user prefs)]
  end
  subgraph triggers [Server triggers on domain events]
    Todos[Todo created/assigned]
    Shifts[Shift published/changed]
    Assign[Assignment to contractor]
    Msg[Portal message]
    Notify[push_notify_user(contractor_id, payload)]
  end
  Pages --> M
  Pages --> SWroute
  Pages -->|register SW| SW
  Pages --> Sub
  Sub --> DB
  Notify --> DB
  Notify -->|webpush| SW
```

**Principle:** Manifest and SW are **served by Flask** (route or `send_from_directory` on blueprint `static_folder`) so `Content-Type` and caching headers are correct and paths stay under `/employee-portal/`.

---

## 3. Files to add or touch (checklist)

| Area | Likely location |
|------|------------------|
| Public routes | `app/plugins/employee_portal_module/routes.py` |
| Portal services / hooks | `app/plugins/employee_portal_module/services.py` |
| Push helper + VAPID | New: `app/plugins/employee_portal_module/push_service.py` (or `notifications_push.py`) |
| Install SQL | `app/plugins/employee_portal_module/install.py` (follow existing plugin pattern) |
| Base templates | `employee_portal_module/templates/public/` — extend shared layout used by login + dashboard |
| Static PWA assets | `app/plugins/employee_portal_module/static/pwa/` — icons, optional precache list |
| SW + manifest | Either **templates rendered as `application/javascript` / `application/manifest+json`** or static files with explicit routes (prefer routes for dynamic `start_url` query if needed) |

---

## 4. HTTP surface (spec)

Implement as **authenticated contractor** where noted; use existing session decorator/helpers from this blueprint.

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/employee-portal/manifest.webmanifest` | Web App Manifest (`application/manifest+json`) |
| GET | `/employee-portal/sw.js` | Service worker (`Content-Type: application/javascript`; `Service-Worker-Allowed` header if scope is parent path — only if you understand the security implication) |
| POST | `/employee-portal/api/push/subscribe` | Body: PushSubscription JSON from browser; store for `contractor_id` |
| POST | `/employee-portal/api/push/unsubscribe` | Body: endpoint URL or full subscription; delete row |
| GET/POST | `/employee-portal/api/push/preferences` | Optional: master enable/disable push |

**Manifest (illustrative JSON):**

```json
{
  "name": "Employee Portal",
  "short_name": "Portal",
  "start_url": "/employee-portal/",
  "scope": "/employee-portal/",
  "display": "standalone",
  "theme_color": "#0d6efd",
  "background_color": "#ffffff",
  "icons": [
    { "src": "/employee-portal/static/pwa/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any maskable" },
    { "src": "/employee-portal/static/pwa/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any maskable" }
  ]
}
```

**Adjust `scope` / `start_url` only after auditing** all portal navigation targets. If scheduling lives at e.g. `/time-billing/` and must feel “inside” the PWA, that requires a **product/security decision** (wider scope vs opening in browser tab).

---

## 5. Jinja integration (Flask website)

In the **shared public portal base** template (used after login and on login if you want install prompt after auth):

```html
<link rel="manifest" href="{{ url_for('public_employee_portal.pwa_manifest') }}">
<meta name="theme-color" content="#0d6efd">
```

End of body (minimal inline bootstrap — keeps logic out of hundreds of pages):

```html
<script>
  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register(
      "{{ url_for('public_employee_portal.pwa_service_worker') }}",
      { scope: "{{ pwa_scope or '/employee-portal/' }}" }
    ).catch(function () {});
  }
</script>
```

Optional: capture `beforeinstallprompt`, show “Install app” on dashboard only.

---

## 6. Service worker behaviour (contract)

- **Install:** cache static assets only (icons, portal CSS/JS if versioned URLs exist). **Do not** cache HTML documents per-user.
- **Fetch:** default **network-first** for navigations under `/employee-portal/`; avoid stale authenticated pages.
- **Push:** `self.addEventListener('push', …)` → `showNotification(title, { body, icon, data: { url } })`.
- **Notification click:** `notificationclick` → `clients.openWindow(data.url)` or focus existing client.

SW file can be a **static string** served by Flask, or generated once from template — keep **no Jinja secrets** inside SW (VAPID public key only if you ever need client-side encryption; subscription uses browser API only).

---

## 7. Database (illustrative)

Table e.g. `employee_portal_push_subscriptions`:

- `id`, `contractor_id` (FK/index), `endpoint` (unique), `p256dh`, `auth`, `created_at`, `last_seen_at`, `user_agent`

Optional: `employee_portal_notification_prefs` with `contractor_id`, `push_enabled`, updated_at.

Use plugin `install.py` migrations consistent with the rest of `employee_portal_module`.

---

## 8. Server-side push sending (Flask process)

- Dependency: e.g. `pywebpush` + VAPID keys in **environment or site settings** (not committed).
- `push_notify_user(contractor_id, *, title, body, relative_url, tag=None)`:
  - Load all subscriptions for user; drop 410 Gone endpoints.
  - Payload: minimal JSON; **title/body generic** (“New task assigned”, “Shift updated”).
  - `relative_url` must stay inside portal policy (e.g. `/employee-portal/...` or approved deep link).

**Trigger wiring (Flask-side):** From code paths that already create todos, shifts, assignments, or portal messages — call the helper (directly or via small internal function). If those paths live in **other plugins**, add **narrow** calls into a registered callback or import a thin function from `employee_portal_module` to avoid circular imports (prefer optional import / lazy call).

---

## 9. Security & compliance

- HTTPS only in production; push requires secure context.
- Notifications are **not** a channel for clinical or highly sensitive content.
- Subscriptions are **per contractor**; validate session on subscribe/unsubscribe.
- Rate-limit subscribe endpoint if needed.

---

## 10. Ops / configuration

Document for deployers:

- Generate VAPID key pair; set e.g. `EMPLOYEE_PORTAL_VAPID_PUBLIC_KEY`, `EMPLOYEE_PORTAL_VAPID_PRIVATE_KEY`, `EMPLOYEE_PORTAL_VAPID_SUBJECT` (mailto: or URL).
- Reverse proxy must **not** strip** `Service-Worker-Allowed` if used; must serve SW with correct MIME type.

---

## 11. QA (Flask-local)

1. Open `/employee-portal/` over HTTPS; DevTools → Application → Manifest valid.
2. SW registers; no errors on hard refresh.
3. Subscribe from dashboard; row in DB.
4. Trigger test notification via Flask shell or temporary admin-only route calling `push_notify_user`.
5. Click notification → correct path loads; session behaves as expected.

---

## 12. Agent execution order

1. Map all `public_employee_portal` routes and the **base template** inheritance tree.
2. Add manifest + SW routes and static icons.
3. Add migration + subscribe/unsubscribe API + `push_service`.
4. Add Jinja hooks to base only.
5. Identify **three** real server events (todo, shift, assignment) and wire **one** end-to-end first; then expand.
6. Document iOS Safari limitations in module README or deploy notes.

---

**End of canvas.** Implement inside `employee_portal_module` first; touch other plugins only with minimal hook lines.
