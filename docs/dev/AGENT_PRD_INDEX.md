# Agent PRD index — discover docs before coding

**Purpose:** Any specialised Cursor agent must **read listed PRDs in order** before planning or implementing. Humans paste a short **entry prompt** (see `CURSOR_AGENT_ENTRY_PROMPT.md`) with an `agent_id`; the agent **opens this file**, finds its row, and loads every document in **`read_order`**.

**Repo root:** Sparrow ERP (`sparrow-erp`). Paths below are relative to `docs/dev/` unless noted.

---

## Workflow (for the AI agent)

1. Read **this entire file** once.
2. Take **`agent_id`** from the user message (e.g. `sparrow_crm`, `cura_clinical`).
3. Find the matching row in **§ Index table**.
4. **`read_order`:** open each path **in sequence** (use your editor `@` mention or read tool). If a path is marked **(TBD)**, the row’s **routing_doc** is the authoritative PRD until the child file is added.
5. **Optional** rows add context only when the task touches that domain.
6. Only after all required reads: restate scope in your own words, then implement.

**Rule:** Do not skip `routing_doc` when listed — it holds **IDs**, **acceptance themes**, and **dependencies**.

---

## Index table

| `agent_id` | Primary codebase | `routing_doc` (required if present) | `read_order` (in order) |
|------------|------------------|-------------------------------------|-------------------------|
| `sparrow_crm` | `app/plugins/crm_module` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` → `PRD_QA_CRM_MEDICAL_EVENT_PLANNER.md` |
| `sparrow_inventory` | `app/plugins/inventory_control` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` → `QA_CLIENT_REPORT_ROUTING_2026-03.md` (INV-KIT-001 / MR split) |
| `sparrow_hr` | `app/plugins/hr_module` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` |
| `sparrow_timesheet_scheduling` **or** `sparrow_time_billing` | `app/plugins/time_billing_module` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` → `PRD_CURSOR_AGENT_TIMESHEET_SCHEDULING.md` → `TIME_BILLING_RUNSHEETS_PUSH.md` |
| `sparrow_medical_records` | `app/plugins/medical_records_module` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` → `QA_CLIENT_REPORT_ROUTING_2026-03.md` → `PRD_COMPLIANCE_AUDIT_SUITE_CQC.md` |
| `sparrow_medical_records_qa` | same + QA audit features | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` (§3.1 QA epics) → `PRD_COMPLIANCE_AUDIT_SUITE_CQC.md` |
| `sparrow_compliance` | future `compliance_audit_module` / governance | — | `PRD_COMPLIANCE_AUDIT_SUITE_CQC.md` → `QA_CLIENT_AUDIT_ROUTING_2026-04.md` (§3.1) |
| `sparrow_ventus` | `app/plugins/ventus_response_module` | `QA_CLIENT_REPORT_ROUTING_2026-03.md` | `QA_CLIENT_REPORT_ROUTING_2026-03.md` → `PRD_VENTUS_CAD_AGENT.md` |
| `sparrow_ventus_cad` | same (CAD/dispatch) | `QA_CLIENT_REPORT_ROUTING_2026-03.md` | `QA_CLIENT_REPORT_ROUTING_2026-03.md` → `PRD_VENTUS_CAD_CLIENT_ROUND_2026-03.md` |
| `ventus_mdt` | Ventus MDT / `vue-connector` (other repo) | `QA_CLIENT_REPORT_ROUTING_2026-03.md` | `QA_CLIENT_REPORT_ROUTING_2026-03.md` → `PRD_MDT_CLIENT_ROUND_2026-03.md` |
| `cura_clinical` | Cura app (other repo) | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` (§3.2) → `PRD_CURA_MEDICAL_RECORDS_AGENT_2026-03.md` → `QA_CLIENT_REPORT_ROUTING_2026-03.md` (CURA-* IDs) |

### Optional cross-reads (only if task says so)

| Topic | Add to read_order |
|--------|-------------------|
| CRM medical event planner | `PRD_QA_CRM_MEDICAL_EVENT_PLANNER.md` |
| Employee portal / identity | `PRD_UNIFIED_ADMIN_EMPLOYEE_PORTAL_IDENTITY.md` |
| DBS monitoring | `PRD_DBS_UPDATE_SERVICE_MONITORING.md` |
| SNOMED dataset | `PRD_CURA_SNOMED_CONDITIONS_DATASET.md` |

---

## Child PRDs (April 2026 audit) — status

These filenames were **planned** in `QA_CLIENT_AUDIT_ROUTING_2026-04.md` §7. Until they exist, **`QA_CLIENT_AUDIT_ROUTING_2026-04.md` is the full PRD** for that stream.

| Planned file | When missing, use |
|--------------|-------------------|
| `PRD_QA_AUDIT_EPCR_GOVERNANCE.md` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` §3.1 |
| `PRD_CURA_TRAUMA_ATMIST_2026-04.md` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` §3.2 |
| `PRD_ROTA_SHIFT_RULES_2026-04.md` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` §3.3 |
| `PRD_CURSOR_AGENT_TIMESHEET_SCHEDULING.md` | **Agent charter** (always read for `sparrow_timesheet_scheduling`); feature spec remains §3.3 until `PRD_ROTA_SHIFT_RULES_2026-04.md` exists |
| `PRD_CRM_HOSPITAL_GEO_FIX.md` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` §3.4 |
| `PRD_INVENTORY_KIT_MEDS_REGISTER.md` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` §3.6 |
| `PRD_HR_APPRAISALS.md` | `QA_CLIENT_AUDIT_ROUTING_2026-04.md` §3.5 |

**Maintenance:** When you add a child PRD, insert it into `read_order` **after** `routing_doc` and update this section.

---

## agent_id quick pick (for humans)

| You need… | `agent_id` |
|-----------|------------|
| CRM, hospitals, event plans | `sparrow_crm` |
| Kit bags, meds register | `sparrow_inventory` |
| Appraisals | `sparrow_hr` |
| Timesheets, scheduling, rota (April audit **ROTA-***) | `sparrow_timesheet_scheduling` or `sparrow_time_billing` |
| ePCR server, Cura handoff, QA audit backend | `sparrow_medical_records` or `sparrow_medical_records_qa` |
| Compliance console / CQC audit suite | `sparrow_compliance` |
| Ventus CAD / dispatch | `sparrow_ventus` / `sparrow_ventus_cad` |
| MDT PWA | `ventus_mdt` |
| Cura UI (trauma, ATMIST, drugs) | `cura_clinical` |

---

*Single source for “which PRDs to read.” Entry copy: `docs/dev/CURSOR_AGENT_ENTRY_PROMPT.md`.*
