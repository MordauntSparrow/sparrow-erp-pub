# Employee Portal Module

Central hub for staff (contractors): sign in once, then access Time & Billing, HR, Compliance, Training, and Scheduling. Mobile-first UI with profile, module links, messages, and a unified to-do list.

## Setup

1. **Time Billing** must be installed and migrated. Run the time_billing migration to add `profile_picture_path` to `tb_contractors`:
   ```bash
   python app/plugins/time_billing_module/install.py upgrade
   ```
2. **Employee Portal** schema (messages and todos):
   ```bash
   python app/plugins/employee_portal_module/install.py install
   ```

## Flow

- Staff go to **/employee-portal/login** (or are redirected from /time-billing/login when the portal is enabled).
- After login, they see the dashboard: profile picture/initials, name, module links, messages, and todos. The to-do list defaults to **pending only** (completed are under **Completed** / **All**) with capped row counts so mobile loads stay fast.
- Session uses `tb_user` (same as time_billing), so they are logged in to Time & Billing when they open it from the portal.

## Adding messages and todos from other modules

Other modules (HR, Compliance, Training, etc.) can push items to the portal by inserting into:

- **ep_messages** – `contractor_id`, `source_module`, `subject`, `body` (optional), `read_at` (NULL until read).
- **ep_todos** – `contractor_id`, `source_module`, `title`, `link_url` (optional), `due_date` (optional), `completed_at` (NULL until done).

Example (from HR when a new hire needs to upload documents):

```sql
INSERT INTO ep_todos (contractor_id, source_module, title, link_url, due_date)
VALUES (123, 'hr_module', 'Upload right to work document', '/hr/documents', '2025-04-01');
```

Example (policy to view in Compliance):

```sql
INSERT INTO ep_todos (contractor_id, source_module, title, link_url)
VALUES (123, 'compliance_module', 'View and sign Health & Safety policy', '/compliance/policies/1');
```

## Production checklist

- **HTTPS** – Serve the employee portal over HTTPS only. The website app (port 80) should sit behind a reverse proxy (e.g. nginx) that terminates TLS and sets headers (e.g. `X-Forwarded-Proto`).
- **Secret key** – The website app uses `app.secret_key` in `website_module`. Set a strong, unique secret via app config or environment so session cookies are signed. Do not use the default placeholder in production.
- **Session cookies** – Prefer `SESSION_COOKIE_HTTPONLY = True`, `SESSION_COOKIE_SAMESITE = 'Lax'` (or `Strict`), and `SESSION_COOKIE_SECURE = True` when served over HTTPS. Configure these on the website Flask app where possible.
- **Rate limiting** – Protect the login endpoint (e.g. `/employee-portal/login` POST) with rate limiting (nginx `limit_req`, or Flask-Limiter) to reduce brute-force risk. Recommend at least 5–10 requests per minute per IP for login.
- **Logging** – The module uses Python `logging` for login failures (email only, no password), inactive-account rejections, and errors in dashboard/API. Ensure the app’s logging is configured (level, handlers) so you can monitor and alert on failures.
- **Redirect safety** – The `next` parameter after login is validated to allow only relative paths (no open redirect). Profile picture paths are validated before use in static URLs (no path traversal).
- **API behaviour** – Mark-read and todo-complete APIs return 400 for invalid ids, 401 when not authenticated, 404 when the resource is not found or not owned by the current user, and 500 on server errors. Front-end handles non-OK responses with user feedback.

## Tenant industry profile (Core settings)

Core → **General** → **Industry & categories** sets `organization_profile.industries`. The **website** app loads the same list into `app.config["organization_industries"]` and exposes Jinja helpers `organization_industries` and `industry_visible(...)` on contractor-facing templates.

- **Dashboard module links** (`get_module_links`): optional per-row `industry_slugs` in `services.py` (OR semantics). Example: **Fleet** is shown only when the tenant includes `medical`, `security`, or `cleaning` (hidden for hospitality-only).
- **Default meta description** in `base_public.html` uses neutral copy unless `medical` is selected.
- **Portal assistant** (`portal_ai.py`) appends the tenant’s industry slugs to the system prompt so replies are not assumed to be clinical.

## PWA and Web Push

- The portal registers a **scoped** service worker under `/employee-portal/` only (`/employee-portal/sw.js`). Manifest: `/employee-portal/manifest.webmanifest`. Replace default icons in `static/pwa/` if you want branded tiles (or run `python app/plugins/employee_portal_module/scripts/gen_pwa_icons.py` to regenerate placeholders).
- **Web Push** requires VAPID environment variables (`EMPLOYEE_PORTAL_VAPID_PUBLIC_KEY`, `EMPLOYEE_PORTAL_VAPID_PRIVATE_KEY`, `EMPLOYEE_PORTAL_VAPID_SUBJECT`); see `app/config/.env.example`. After `install.py`, contractors can opt in from the dashboard (“Browser notifications”). Payloads are intentionally generic (e.g. “New portal message”, “New task assigned”); click targets stay under `/employee-portal/…`.
- **iOS Safari** – Add to Home Screen works for a basic PWA experience; Web Push for web apps has historically been limited on iOS compared to Chromium-based browsers. Test on your target iOS version; do not rely on push as the only channel for time-critical clinical or highly sensitive content.
