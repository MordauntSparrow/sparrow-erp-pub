# Tester wave: Cura Minor Injury + operational event report

Use this checklist when deploying **Sparrow (medical records / Cura API)** and the **Cura PWA** together for field testing.

## Automated preflight (Sparrow)

From the **sparrow-erp** repo root, with the same environment the app uses for MySQL:

```bash
python scripts/verify_cura_mi_schema.py
```

Or:

```bash
bash scripts/cura_preflight.sh
```

Windows PowerShell:

```powershell
.\scripts\cura_preflight.ps1
```

Exit code **0** means required Cura/MI tables and key columns exist. On failure, run `python app/plugins/medical_records_module/install.py upgrade` then re-check.

## Database and upgrades

- Run the **medical records module** database install / migrations on the test server so tables exist: `cura_operational_events`, `cura_operational_event_assignments`, `cura_mi_events`, `cura_mi_assignments`, `cura_mi_reports`, etc.
- If MI or incident-report calls return **503 / empty hub tables**, upgrade first.

## Roles and API access

- **Clinical ops hub** (browser, Flask login): `/clinical/cura-ops` — roles such as **clinical_lead**, **admin**, **superuser** (see `_cura_ops_roles_ok` in `medical_records_module/routes.py`).
- **Bearer JSON APIs** (Cura app): MI assigned events, incident report, and MI admin PATCH require a valid Cura JWT and appropriate server-side checks. Test with both a **privileged** account (admin PATCH) and a **field** account (assigned to MI + operational period).

## Linking Minor Injury to an operational period

1. **Pinned on MI event (preferred for a fixed deployment)**  
   Set on the MI row’s `config_json`: `operational_event_id` (integer).  
   **Admin API:** `PATCH /api/cura/minor-injury/admin/events/{id}` with body `{"operational_event_id": 12}` (Bearer, privileged).  
   **Ops hub:** Minor injury table shows **Linked op period** with a link to operational event detail.

2. **Per-user default (fallback)**  
   Assign the user in **`cura_operational_event_assignments`** to an operational period.  
   Cura calls `GET /api/cura/me/operational-context` → `recommended_operational_event_id`.

**Resolution order on the Cura MI screen:** MI config pin first, then server recommendation.  
This is **not** the same as the optional **Settings → fallback operational period ID** on the device (that field is for **new EPCR cases**, not the MI incident-report card).

## Suggested test matrix

| Scenario | MI `operational_event_id` in config | User assignment | Expected incident-report period |
|----------|--------------------------------------|-----------------|--------------------------------|
| A | Set (e.g. 5) | Any / other | Period **5** |
| B | Not set | Active assignment to period 7 | Period **7** |
| C | Not set | None | MI card explains missing link; no report |

## Incident-report API (sanity)

- `GET /api/cura/operational-events/{eventId}/incident-report?mi_event_id={miId}&min_cell=1`  
- **EPCR rows** only appear when case JSON includes `operationalEventId` / `operational_event_id` matching `{eventId}`.  
- **Small counts:** server may return **—** for per-status cells below `min_cell` (Cura MI uses `min_cell=1`).

## Capabilities

- `GET /api/cura/capabilities` includes `features.operational_event_incident_report` (default **true**). If you set this to **false** in a fork, the Cura MI screen hides the operational report block.

## Audit

- Incident-report requests are audited server-side; grep logs during a dry run if validating permissions.
