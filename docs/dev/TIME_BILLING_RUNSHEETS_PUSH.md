# Dev deployment — time billing run sheets (what to push & run)

Use this when opening a PR or deploying to a **development** environment for QA.

## 1. Include in Git (runsheets feature)

Ensure these are **committed** (not left untracked):

- `app/plugins/time_billing_module/db/010_runsheet_assignment_payroll.sql`
- `app/plugins/time_billing_module/docs/` (`PRODUCTION_CHECKLIST_RUNSHEETS.md`, `RUNSHEET_SCHEDULING_WORK_MODEL.md`)
- `app/plugins/time_billing_module/templates/public/runsheets/`
- `app/plugins/time_billing_module/routes.py`, `services.py`, `install.py` (and other TB changes you intend to ship)
- `app/create_app.py` (rate limits)
- `scripts/verify_time_billing_runsheet_schema.py`
- `requirements-dev.txt`, `tests/test_runsheet_production.py`
- `app/config/.env.example` (optional but recommended)
- `app/plugins/time_billing_module/manifest.json` (version bump)

Do **not** commit `__pycache__/` or `*.pyc` (see `.gitignore`).

## 2. On the dev server / container (after pull)

```bash
python -m app.plugins.time_billing_module.install upgrade
python scripts/verify_time_billing_runsheet_schema.py   # expect exit 0
```

Restart the app process if needed.

## 3. Smoke URLs (logged-in contractor)

- `/time-billing/` → **Run sheets** (nav)
- `/time-billing/runsheets` — list
- `/time-billing/runsheets/new` — create

## 4. CI (optional)

```bash
pip install -r requirements-dev.txt
pytest tests/test_runsheet_production.py -q
```
