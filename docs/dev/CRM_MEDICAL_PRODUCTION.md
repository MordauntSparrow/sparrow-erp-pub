# CRM medical event planner and Cura handoff — production notes

This document covers deploying and operating the CRM event planner handoff to Cura (`medical_records_module`) operational periods.

## Upgrade order

1. Run CRM plugin migrations so `crm_event_plans` includes handoff columns and `cura_operational_event_id`:

   ```bash
   python app/plugins/crm_module/install.py upgrade
   ```

2. Ensure `medical_records_module` is installed and its schema is current (including `cura_operational_events`, `cura_operational_event_assets`, and handoff sync code).

## Environment variables

| Variable | Purpose |
|----------|---------|
| `SPARROW_CRM_EVENT_HANDOFF_MODE` | **Default `live`** (unset or empty): user **Sync to Cura** calls `sync_crm_event_plan_to_cura`. Set `off` / `0` / `false` / `no` to disable; `dry_run` / `simulate` for QA. |
| `SPARROW_CRM_HANDOFF_REQUIRE_PDF` | When `1` / `true` / `yes` / `on`, `live` handoff **fails** if the plan has no row in `crm_event_plan_pdfs`. Use in production if every handoff must ship a PDF asset into Cura. |

## Static storage and PDFs

- Event plan PDFs are stored under `static/uploads/crm_event_plans/` (with repo `static/` fallback via `crm_static_paths`). The app root used for resolution must match where files are written (persistent volume on Railway or similar).
- WeasyPrint (or your PDF stack) needs OS dependencies on the host image; align with your existing Sparrow deployment docs.

## Operational linking

- After a successful `live` handoff, `crm_event_plans.cura_operational_event_id` is set. UI links to Cura prefer this column, then `handoff_external_ref` (`cura_operational_event:{id}`).
- Cura **event manager** lists operational periods with a **CRM** column when `config.crm_event_plan_id` is set or the slug looks like `crm-ep-{plan_id}`.

## Failure modes

- **Import error for medical module**: handoff logs `failed`; ensure both plugins are enabled.
- **Missing handoff tables**: error message points at `crm_module/install.py upgrade`.
- **PDF required but missing**: set `SPARROW_CRM_HANDOFF_REQUIRE_PDF` only when you enforce PDF generation before sync.

## Smoke checks

- CRM: Event plan list shows **Ops event** when linked; plan edit shows **Open Cura operational event**.
- Cura: Operational event detail shows banner back to CRM when `crm_event_plan_id` is in config.
- Hub: `/clinical/cura-ops` table shows CRM badge/link for CRM-sourced periods.
