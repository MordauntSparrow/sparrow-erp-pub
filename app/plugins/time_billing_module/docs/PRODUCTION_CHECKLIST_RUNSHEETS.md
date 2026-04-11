# Production checklist — contractor run sheets & time billing

Work top → bottom before go-live; re-run **scripted** steps after upgrades.

## 1. Scripted verification (run from project root)

| Step | Command | Pass criteria |
|------|---------|----------------|
| **Schema** | `python scripts/verify_time_billing_runsheet_schema.py` | Exit `0`, prints `OK:` for required columns |
| **Migrations** | `python -m app.plugins.time_billing_module.install upgrade` | No SQL errors; ledger records new `.sql` files |
| **Unit tests** | `pip install -r requirements-dev.txt` then `pytest tests/test_runsheet_production.py -q` | All green |

> **Backup:** take a DB snapshot before first production `upgrade` that adds run sheet columns.

## 2. Environment & cookies (see also `app/config/.env.example`)

| Check | What to set |
|-------|-------------|
| **DEBUG off** | `FLASK_DEBUG=0` or do not enable debug in production so `_tb_safe_api_error` hides internals |
| **HTTPS** | Terminate TLS at nginx/Caddy/ALB; do not expose `/time-billing/*` on plain HTTP in prod |
| **Secure cookie** | `SESSION_COOKIE_SECURE=true` when site is HTTPS-only (see `create_app.py`) |
| **Rate limits** | Optional: `TB_CONTRACTOR_SEARCH_RATE_LIMIT` (default `60 per minute`), `TB_ADMIN_CONTRACTOR_SEARCH_RATE_LIMIT` (default `120 per minute`). Requires Flask-Limiter + storage (`RATELIMIT_STORAGE_URI` or `REDIS_URL`) for multi-worker accuracy |
| **HSTS** | Optional: `ENABLE_HSTS=1` if you serve HTTPS directly from Flask (often set at proxy instead) |

**Implemented in repo:** `SESSION_COOKIE_HTTPONLY`, `SameSite=Lax`, optional Redis sessions with secure cookie when prod.

## 3. Security (code review — satisfied in current branch)

- [x] Contractor run sheet APIs: `@staff_required_tb` on mutating routes  
- [x] PUT / publish: lead only (`contractor_may_edit_runsheet_header`) unless `lead_user_id` is NULL (legacy)  
- [x] CSRF: `sparrow_csrf_head` + `tbMergeCsrfHeaders` on contractor saves  
- [x] Contractor search: auth required, min query length 2, LIMIT 30, **rate limit** when Limiter active  
- [ ] **You:** confirm admin time-billing URLs stay on internal app; contractors use `/time-billing/…` only  

## 4. Functional smoke (manual — record pass/fail in change ticket)

- [ ] **Lead:** Run sheets → New → client, job type, date → Save → Publish → timesheet rows exist  
- [ ] **Crew:** Second user sees sheet → saves own actuals → if already published, timesheet/pay updates  
- [ ] **Withdraw / reactivate:** One person withdraws; others unchanged; reactivate + publish restores pay line  
- [ ] **Template:** Publish blocked until required payload fields pass validation (toast / JSON `message`)  

## 5. Monitoring

- [ ] Ship logs to your stack; search: `time_billing API`, `api_my_runsheets_`, `runsheets template schema`, `assignment_withdraw`  
- [ ] Alert on elevated 5xx for `/time-billing/api/my/runsheets` and `/time-billing/api/refs/contractors`  

## 6. Roadmap (not blocking first ship)

| Item | Notes |
|------|--------|
| Ventus / admin **Resync** UX | **Shipped:** list + edit copy, confirm text; `ventus_integration` doc → call `publish_runsheet` after sign-off |
| Scheduler vs run sheet **dedupe** | **Shipped:** `TimesheetService.delete_scheduler_prefill_for_shift` after `create_and_publish_runsheet_for_shift` (only if `edited_by IS NULL`) — see model doc §6.E |
| Work → **visit / client** | **Shipped:** `work_visits` + `work_photos.visit_id`; admin `/plugin/work_module/visits`; run `python -m app.plugins.work_module.install upgrade` |
| **Integration tests** | DB + session fixture for full create→publish flow |

## Quick reference

- Model doc: [`RUNSHEET_SCHEDULING_WORK_MODEL.md`](RUNSHEET_SCHEDULING_WORK_MODEL.md)  
- Time billing install: `python -m app.plugins.time_billing_module.install` (`install` | `upgrade` | `uninstall`)  
- **Dev push / deploy:** [`../../../../docs/dev/TIME_BILLING_RUNSHEETS_PUSH.md`](../../../../docs/dev/TIME_BILLING_RUNSHEETS_PUSH.md)  
- Main deploy guide: `DEPLOYMENT.md` (Time Billing migration subsection)  
