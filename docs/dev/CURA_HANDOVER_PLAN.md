# Cura Clinical Systems — implementation plan (from handover)

**Source of truth (UI + contract):** [`CURA_CLINICAL_SYSTEMS_HANDOVER.md`](./CURA_CLINICAL_SYSTEMS_HANDOVER.md)  
**Sparrow Cura architecture:** [`CURA_OVERHAUL.md`](./CURA_OVERHAUL.md)  
**Execution checklist:** extend [`CURA_TRACKING.md`](./CURA_TRACKING.md) section **K**.

**Principles:** offline-first client queue + **idempotent** server writes; no silent loss of drafts/submissions; preserve legacy plugin URLs where deployed (`/plugin/medical_records_module/api/...`).

---

## Phase 1 — Real auth (handover priority 1)

| Step | Deliverable | Status |
|------|-------------|--------|
| 1.1 | `POST .../api/cura/auth/token` (exists) | [x] |
| 1.2 | `GET .../api/cura/auth/me` — profile for SPA (`id`, `username`, `name`, `email`, `role`, handover role hints) | [x] |
| 1.3 | `POST .../api/cura/auth/refresh` — new JWT from valid Bearer (sliding session) | [x] |
| 1.4 | `POST .../api/cura/auth/logout` — acknowledge logout (client discards token; document no server revoke without denylist) | [x] |
| 1.5 | Map Sparrow roles → handover labels (`clinician`, `event_lead`, `safeguarding_reviewer`, `admin`) — v1 heuristic + permissions later | [x] `cura_role_map.py` |
| 1.6 | Document env: `JWT_SECRET_KEY`, `SESSION_TOKEN_EXPIRY_HOURS` | [x] see `CURA_OVERHAUL.md` + `CODEX_HANDOVER_TEMPLATE.md` |

---

## Phase 2 — EPCR cases + offline-friendly writes (priority 2)

| Step | Deliverable | Status |
|------|-------------|--------|
| 2.1 | Optional `idempotency_key` / `Idempotency-Key` on `POST /cases`, `PUT /cases/<id>`, `PUT /cases/<id>/close` + DB column / lookup | [x] migration `005` |
| 2.2 | Echo `serverAck` metadata in responses (e.g. `recordVersion`, timestamps) for client sync state | [x] |
| 2.3 | Formalise `Review` + `Drugs Administered` sections (stored in `sections[]`; document + optional validation) | [x] normalisation in `routes.py` |
| 2.4 | Align `GET /search` query params with handover (`date_of_birth`, `postcode`) | [x] |

---

## Phase 3 — Safeguarding API surface (priority 3)

| Step | Deliverable | Status |
|------|-------------|--------|
| 3.1 | **Compatibility blueprint** under `/plugin/medical_records_module/api/safeguarding/...` (`safeguarding_facade_routes.py`) | [x] |
| 3.2 | Handover paths: list/create/get/put/submit/close/delete + attachments | [~] attachments remain `/api/cura/attachments` |
| 3.3 | Idempotent submit/close (reuse `idempotency_key` pattern from Cura) | [x] facade + Cura PATCH |

---

## Phase 4 — Minor injury (single-module) + clinician API (priority 4)

| Step | Deliverable | Status |
|------|-------------|--------|
| 4.1 | Tables + routes in `medical_records_module` (`cura_mi_*`, `cura_mi_routes.py`) — not a separate plugin | [x] |
| 4.2 | Tables: events, notices, documents, reports, assignments | [x] |
| 4.3 | `GET .../minor-injury/events/assigned`, reports POST with idempotency, event closed → structured rejection | [x] |

---

## Phase 5 — Event manager admin (priority 5)

| Step | Deliverable | Status |
|------|-------------|--------|
| 5.1 | Admin CRUD under `/api/cura/minor-injury/admin/events` | [x] |
| 5.2 | Notices read, analytics + export shapes (assignments/documents/void can extend) | [~] |
| 5.3 | Late / rejected report queue: `GET .../admin/reports/pending` | [x] |

---

## Phase 6 — Files / media (priority 6)

| Step | Deliverable | Status |
|------|-------------|--------|
| 6.1 | `epcr_case` attachment entity type + `GET .../attachments/<id>/file` (auth, path hardening) | [x] |
| 6.2 | Section linkage via `entity_id` = case id + client convention for Needs / handover / ECG | [~] |

---

## Phase 7 — Datasets & config (priority 7)

| Step | Deliverable | Status |
|------|-------------|--------|
| 7.1 | `GET/PUT /api/cura/datasets/versions` + `/api/cura/datasets/<name>` | [x] |
| 7.2 | `GET/PUT /api/cura/config/app-settings` (tenant-scoped) | [x] |

---

## Phase 8 — Analytics & exports (priority 8)

| Step | Deliverable | Status |
|------|-------------|--------|
| 8.1 | Minor injury per-event analytics + export (aggregated keys only) | [x] |
| 8.2 | Org-wide anonymised aggregates if required (IG) | [ ] *if product requires* |

---

*Update this file as steps complete. Tick **K** rows in `CURA_TRACKING.md` in the same commit when possible.*
