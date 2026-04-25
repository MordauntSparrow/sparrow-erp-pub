# Cura overhaul — live tracking checklist

Use this file to monitor parity with **`CURA_OVERHAUL.md`**, **[`CURA_CLINICAL_SYSTEMS_HANDOVER.md`](./CURA_CLINICAL_SYSTEMS_HANDOVER.md)** (full React/Vite contract), **[`CURA_HANDOVER_PLAN.md`](./CURA_HANDOVER_PLAN.md)** (phased execution), **`CODEX_HANDOVER_TEMPLATE.md`** (short SPA checklist), and deployment readiness.

**Legend:** `[x]` done · `[~]` partial · `[ ]` not started

---

## A. Architecture & docs

| # | Item | Status |
|---|------|--------|
| A1 | Single module (`medical_records_module`) — no split plugins | [x] |
| A2 | `docs/dev/CURA_OVERHAUL.md` kept in sync with behaviour | [x] schema v5 + new surfaces documented |
| A3 | **`docs/dev/CODEX_HANDOVER_TEMPLATE.md`** filled with SPA paths/payloads | [x] |
| A4 | Cross-check handover vs `cura_routes.py` + EPCR `/api/*` + facade/MI/datasets | [x] |

---

## B. Database & upgrades

| # | Item | Status |
|---|------|--------|
| B1 | `001_cura_foundation` — Cura tables + `cases` link columns | [x] |
| B2 | `002_safeguarding_operational_event` | [x] |
| B3 | `003_cura_file_attachments` | [x] |
| B4 | `004_assignments_and_enforcement` — assignments + `enforce_assignments` | [x] |
| B5 | `005`–`008` — cases idempotency, safeguarding audit, datasets/settings, minor injury | [x] + `init_medical_records.sql` cases columns |
| B6 | Run `python app/plugins/medical_records_module/install.py upgrade` on each env | [ ] *ops* |

---

## C. Authentication (SPA / MDT)

| # | Item | Status |
|---|------|--------|
| C1 | Session cookie auth for browser EPCR (unchanged) | [x] |
| C2 | **`Authorization: Bearer <JWT>`** for JSON APIs (`app/auth_jwt.py`) | [x] |
| C3 | **`POST /api/cura/auth/token`** (username/password → Bearer token) | [x] |
| C4 | Clinical roles only: `crew`, `admin`, `superuser`, `clinical_lead` | [x] |
| C5 | Document `JWT_SECRET_KEY` / `SESSION_TOKEN_EXPIRY_HOURS` for production | [x] `CURA_OVERHAUL.md` + `CODEX_HANDOVER_TEMPLATE.md` |

---

## D. EPCR JSON API

| # | Item | Status |
|---|------|--------|
| D1 | `/api/cases`, `/api/cases/<id>`, `/close`, `/search`, `/ping` | [x] |
| D2 | Dispatch columns on `cases` + merge/strip + `recordVersion` optimistic lock | [x] |
| D3 | JWT principal used for assignment checks & audit on JSON routes | [x] |
| D4 | Case idempotency + `serverAck` + close idempotency | [x] |

---

## E. Cura extension API (`/api/cura/*`)

| # | Item | Status |
|---|------|--------|
| E1 | Capabilities + schema version | [x] v6 (`CURA_SCHEMA_VERSION` in `cura_routes.py`) |
| E2 | Operational events CRUD + by-slug + record-counts | [x] |
| E3 | Safeguarding + patient-contact + `public_id` lookups + PATCH RBAC | [x] |
| E4 | Attachment **metadata** + **multipart upload** + **`epcr_case`** + **download** | [x] |
| E5 | **Assignments** list/add/remove + **`enforce_assignments`** on event | [x] |
| E6 | **Analytics** summary with **minimum cell suppression** | [x] |
| E7 | **Dispatch preview** (best-effort `mdt_jobs` when Ventus DB present) | [x] |
| E8 | Auth **me / refresh / logout**; safeguarding **prefill** + **audit** | [x] |

---

## F. CAD / Ventus integration

| # | Item | Status |
|---|------|--------|
| F1 | Nullable case link fields + API surface | [x] |
| F2 | Read-only job preview for prefill workflows | [x] |
| F3 | Auto-write timings / merge rules / webhooks | [ ] *product slice* |
| F4 | Ventus module calls into Cura (shared DB or HTTP) | [ ] *product slice* |

---

## G. Files & uploads

| # | Item | Status |
|---|------|--------|
| G1 | Metadata table + list/create/delete | [x] |
| G2 | Local disk upload under plugin `data/cura_uploads/` | [x] |
| G3 | S3 / CDN / virus scan | [ ] *ops product* |

---

## H. Analytics & IG

| # | Item | Status |
|---|------|--------|
| H1 | Per-event aggregates (counts by status) | [x] |
| H2 | Suppressed small cells (`min_cell`, default 5) | [x] |
| H3 | Org-wide anonymised dashboards | [ ] *if handover requires* |
| H4 | MI per-event analytics + export (aggregated keys) | [x] |

---

## I. Care company portal

| # | Item | Status |
|---|------|--------|
| I1 | **Default off**; enable **`CURA_ENABLE_CARE_COMPANY_PORTAL=true`**; **`CURA_DISABLE_CARE_COMPANY_PORTAL=true`** forces off | [x] |
| I2 | Manifest flag `care_company_portal_enabled` (hint; default **false** in `manifest.json`) | [x] |
| I3 | Code + routes retained; admin UI hidden when disabled | [x] |
| I4 | Legacy **`GET /care_company/case-access`** → **301** to **`/case-access`** | [x] |

---

## J. Verification (after DB upgrade)

| # | Item | Status |
|---|------|--------|
| J1 | Obtain token → call `/api/cura/capabilities` with Bearer | [ ] *QA* |
| J2 | CRUD one operational event + assignments + enforced POST | [ ] *QA* |
| J3 | EPCR PUT with `recordVersion` conflict path | [ ] *QA* |
| J4 | Upload file → metadata row → GET list → **GET .../file** | [ ] *QA* |

---

## K. Clinical Systems handover plan (`CURA_HANDOVER_PLAN.md`)

| # | Phase | Status |
|---|--------|--------|
| K1 | **1** Real auth (`/me`, refresh, logout, role mapping) | [x] |
| K2 | **2** EPCR idempotency keys + sync metadata + Review/Drugs sections + search params | [x] |
| K3 | **3** Safeguarding REST facade + prefill + audit | [x] |
| K4 | **4** Minor injury clinician API (`cura_mi_*`) | [x] |
| K5 | **5** Event manager admin API | [~] core CRUD + pending queue + export |
| K6 | **6** Unified EPCR/media files | [x] `epcr_case` + download |
| K7 | **7** Datasets + app config API | [x] |
| K8 | **8** Analytics / exports | [~] MI event-level; org-wide TBD |

---

*Last updated: CURA implementation continuation — migrations 005–008, facade, datasets, MI, tracking refresh.*
