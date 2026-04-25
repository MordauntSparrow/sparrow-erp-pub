# Cura operational events × Ventus MDT — integration plan

This document records the agreed product model and maps it to implementation phases. It supersedes ad-hoc notes from design discussions.

## Goals

1. **Single event umbrella** — `cura_operational_events.id` is the stable key grouping EPCR, minor injury, safeguarding, patient-contact, and (later) CAD/dispatch artefacts for audit, rollups, and debrief packs (CQC / Purple Guide–oriented reporting).

2. **Time-bound temporary Ventus division** — For each live event, provision an **`mdt_dispatch_divisions`** row (slug stored on the event, typically under `config.ventus_division_slug`). While the event is in its active window, the division is **`is_active=1`** so MDT sign-on and Ventus boards can scope to it; outside the window it is **`is_active=0`** so it disappears from normal division pickers.

3. **Assignments** — **Usernames** (Cura-only staff, e.g. medical tent) and **Ventus crews** (callsign + MDT crew list) are linked via:
   - `cura_operational_event_assignments.principal_username` (required)
   - Optional `expected_callsign` — hint for “this user normally signs that unit on MDT” (validation aid; not required for Cura-only roles).

4. **Cura session context** — On sign-in, Cura resolves **which operational event(s)** the user is assigned to and which are **time/status-active**, to pre-fill `operational_event_id` / show event details without manual ID entry when rushing.

5. **Callsign ↔ username ↔ MDT truth** — If the user enters a **callsign** on Cura **and** MDT has that unit signed on with a **crew JSON** listing usernames, Sparrow validates that the **logged-in username** appears in that crew list. If not, **do not treat dispatch timing sync as trusted**; surface **warnings** in Cura and list failures in the **Cura ops hub** (event manager portal).

6. **Controller/admin control** — Event creation, division provisioning, assignment lists, and enforcement (`enforce_assignments` on the event) remain **privileged** operations; field crews get defaults and clear warnings, not silent wrong data.

## Config keys (`cura_operational_events.config` JSON)

| Key | Purpose |
|-----|---------|
| `ventus_division_slug` | Slug for `mdt_dispatch_divisions.slug` (e.g. `cura_evt_42` or organiser-provided slug). |
| `ventus_division_name` | Display name for the division picker (default: event `name`). |
| `ventus_division_color` | Hex colour for Ventus UI (default `#6366f1`). |
| `epcr_signon_incident_fields` | Optional JSON array of `{ key, label, type?, placeholder?, response_type? }` — live per-event Incident Log extras. `response_type` (optional) limits the field to that incident response option value (same as Cura Settings incident list); omitted = all types. Delivered on `GET /api/cura/me/operational-context` (roster `items` and each `dispatch_divisions` row); not part of static EPCR datasets. Event manager UI edits this without exposing raw JSON. |

## API surface (Cura JSON, Bearer JWT)

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/cura/me/operational-context` | Active assignments + event summary + recommended `operational_event_id`. |
| `POST` | `/api/cura/validate-mdts-callsign` | Body: `callsign`, optional `operational_event_id`. Validates username vs `mdts_signed_on.crew`; optional division match vs event config. |
| `POST` | `/api/cura/operational-events/<id>/ventus-division/sync` | Privileged: upsert Ventus division + set `is_active` from event window/status. |
| `GET` | `/api/cura/operational-events/<id>/callsign-validation-log` | Privileged: recent validation rows (failures highlighted). |
| `GET` | `/api/cura/operational-events/<id>/cad-correlation` | Privileged: CAD jobs + comms stats scoped by `config.ventus_division_slug` vs `mdt_jobs.division`. |

Assignments API extended: optional `expected_callsign` on create (existing `POST .../assignments`).

## Database

- **`cura_operational_event_assignments.expected_callsign`** — optional `VARCHAR(64)`.
- **`cura_callsign_mdt_validation_log`** — append-only log for portal + analytics (`ok`, `reason_code`, `username`, `callsign`, `operational_event_id`, `detail_json`, `created_at`).

## Phased delivery

| Phase | Scope | Status |
|-------|--------|--------|
| **A** | Plan doc + migration + bridge helpers + Cura APIs + ops hub log table + portal table snippet | **Done** (migration `010_event_ventus_integration`, `cura_event_ventus_bridge.py`, ops hub table) |
| **B** | Cura SPA: consume `operational-context` on login; banner when `validate-mdts-callsign` fails; gate dispatch timing import on `allow_dispatch_timing_sync` | **Done** (incl. assignment-based default op period + dashboard hint) |
| **C** | Medical records “event manager” v2: KPIs, export pack, CAD message correlation | **Largely done** — ops hub KPIs + per-event **detail** page; JSON debrief pack; optional `mi_event_id` on debrief; `GET …/cad-correlation`; Ventus `mdt_jobs` / `mdt_job_comms` correlation by division slug |
| **D** | Ventus: optional auto-filter jobs by event division (policy) | **Done** — **Ventus Response Admin** UI + `GET`/`POST` `/plugin/ventus_response_module/dispatch/cura-event-job-policy`; stores `mdt_dispatch_settings.cura_event_job_filter_strict`; optional env `VENTUS_CURA_EVENT_JOB_FILTER_STRICT` forces strict and locks UI until unset |

## Operational notes

- **Phase D policy** — Prefer **UI**: Ventus Response Admin → Division Control → “Cura / EPCR — event division job filter” (`GET`/`POST` `/plugin/ventus_response_module/dispatch/cura-event-job-policy`). The CAD topbar shows **Cura filter ON** when strict mode is effective; `GET /dispatch/divisions` includes `cura_event_job_filter_strict`, `cura_event_job_filter_strict_database`, and `environment_override_active`. Optional host override: env `VENTUS_CURA_EVENT_JOB_FILTER_STRICT=true` (forces strict on and disables the checkbox until unset).
- **Ventus must be deployed on the same DB** (or shared DB) so `mdts_signed_on` and `mdt_dispatch_divisions` are visible to medical_records queries.
- If MDT sign-on is not used for a user, **omit callsign** on Cura or accept that MDT validation will fail until they sign the correct unit on MDT.
