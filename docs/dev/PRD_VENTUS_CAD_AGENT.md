# PRD — Ventus CAD Agent

**Scope:** Dispatcher/CAD system of record, resource state, major-incident workflows.  
**Source index:** [QA_CLIENT_REPORT_ROUTING_2026-03.md](./QA_CLIENT_REPORT_ROUTING_2026-03.md) (matrix IDs **CAD-SCENE-001**, **CAD-MI-001**; cross-ref **MDT-MI-001**).

---

## 1. CAD-SCENE-001 — Multi-unit “on scene” on MDT (bug)

### Problem

When **more than one unit** is assigned to the same CAD, **one** MDT marking **on scene** can cause **another** MDT (same job) to show **on scene** even though that crew has not pressed it. **CAD** shows per-unit state correctly; the bad behaviour is **MDT-specific**.

### Root cause (verified in codebase)

| Layer | Behaviour |
|--------|-----------|
| **POST** `/api/mdt/<cad>/status` | Updates **`mdt_jobs.status`** for the whole CAD (`UPDATE mdt_jobs SET status = %s WHERE cad = %s`) while also updating **`mdts_signed_on.status`** only for the **posting callsign** (`routes.py` ~11578–11610). |
| **GET** `/api/mdt/<cad>` (`mdt_details`) | Returns **`mdt_jobs.status`** only — a **single** job-level field, not per callsign. |
| **MDT client** (`vue-connector` `pollDetails`) | Previously advanced the local job-flow ladder using **`GET /mdt/<cad>` → `status`**, while **`pollStatus`** intentionally **ignored** job-flow updates when already on a job (“handled by pollDetails”). So every tablet on that CAD followed **whoever last moved the job row**, not its own unit row. |

### Fix direction

1. **MDT (implemented in `Ventus MDT/vue-connector`):** While on an active CAD, derive **ladder advancement** from **`GET /api/mdt/<callsign>/status`** when `assigned_incident` matches the open CAD — i.e. **`mdts_signed_on`** truth for **this** device. Keep **`GET /mdt/<cad>`** for **job-level** outcomes (e.g. stood down / queued) and **triage / incident display**.
2. **Resume-after-refresh:** Same rule when reconciling `active_status` from the server — prefer unit row when assigned CAD matches saved CAD.
3. **API (Sparrow — implemented):** `GET /api/mdt/<cad>?callsign=X` adds **`my_status`** (and **`my_callsign`**) when the resolved callsign is allowed on that CAD (same rules as `POST /mdt/<cad>/status`: live `assignedIncident` or `mdt_job_units` link). MDT prefers this field and falls back to `GET /mdt/<callsign>/status` on older servers or when `my_status` is absent.

### Acceptance

- **Repro closed:** Two callsigns on one CAD; unit A → on scene; unit B remains **mobile** (or prior step) until B explicitly advances; logs show `pollDetails` using unit ladder from `/mdt/<callsign>/status`.
- **Regression:** Single-unit jobs still advance as before; stand-down / queue detection from job row unchanged.
- **Traceability:** Aligns with QA doc **§3.3 Epic CAD-1** and matrix **CAD-SCENE-001**; MDT epic **MDT-1** verification.

### Dependencies

- Deploy **Sparrow** with `mdt_details` query support and **MDT** with `?callsign=` on detail/resume polls (backward compatible).

---

## 2. CAD-MI-001 — Major incident: TST, MITT, METHANE (feature)

### Goal

Support **major incident** workflow in **CAD / dispatcher**: structured incident model, **alerts**, **resource linkage**, and **inbound** payloads from MDT (**MDT-MI-001**).

### In scope (CAD / Sparrow `ventus_response_module`)

- **Incident model:** Fields or JSON blobs for declared major-incident type, phase (e.g. TST / MITT stages), and **METHANE** (or agreed mnemonic) checklist / narrative blocks — exact shape from joint API session.
- **Dispatcher UX:** Create/update MI from CAD; visible banner or rail state; tie-in to job/CAD list and unit assignment where product agrees.
- **Alerts:** Urgent dispatcher notification path when MDT sends MI (sound/toast/socket parity with existing `mdt_event` patterns where applicable).
- **Audit:** Who/when/what for MI create/update and MDT-originated messages.
- **Ingest API:** Accept **MDT-initiated** POST (versioned) — **no silent drop**: validate, persist, 4xx/5xx with clear body; log server-side on failure.

### Out of scope (other agents)

- **MDT** hot buttons, offline queue, and clinical wording (**MDT-MI-001**).
- **Cura** ePCR content unless explicitly linked later.

### Acceptance

- Client **repro scenario** from field (or internal sim) **closed with logs** (request/response + CAD UI state).
- **Templates** (TST / MITT / METHANE as agreed) exercised end-to-end with **MDT handoff** in staging.
- **No silent drop** of MDT major-incident messages: failures surfaced to MDT client and server audit.

### Dependencies

- **Joint API/schema session** with MDT: payload JSON, IDs, idempotency, error codes, and whether MI attaches to **CAD**, **division**, or **standalone incident** record.
- Coordinate sequencing with QA **§4** (major incident full stack **P3** unless escalated to P0/P1 by ops).

---

## 3. Implementation status (vs this PRD)

| Item | Status | Notes |
|------|--------|--------|
| **CAD-SCENE-001** | **Done** | MDT: per-unit ladder via `my_status` or `/mdt/<callsign>/status`; job row still drives stand-down/queued. Sparrow: `GET /api/mdt/<cad>?callsign=` → `my_status`. |
| **CAD-MI-001** | **v1 shipped (iterative)** | Table `ventus_major_incidents`; `POST /api/mdt/<callsign>/major_incident` (api_version **1**, required `client_request_id`, `template` ∈ methane/tst/mitt/custom, `payload` object); idempotent replay; audit `mdt_major_incident` / `cad_major_incident`; `mdt_event` **major_incident_alert** / **major_incident_closed**; job comm `major_incident` when `cad` set; CAD banner + `GET/POST /dispatch/major-incidents`, `POST .../close`; **CAD UI** — Messages panel + job detail **Declare MI** modal (METHANE/TST/MITT/custom parity with MDT); non-empty **payload** enforced server-side; banner close confirm. MDT modal + **Major incident** entry (sidebar + mobile More). Extend **payload** fields after joint clinical schema session. |

---

## 4. API quick reference (CAD-MI-001 v1)

**MDT —** `POST /plugin/ventus_response_module/api/mdt/<CALLSIGN>/major_incident`

```json
{
  "api_version": 1,
  "client_request_id": "uuid-v4",
  "template": "methane",
  "phase": "optional",
  "cad": 12345,
  "division": "optional",
  "payload": { "summary": "…", "major_incident_declared": "…" }
}
```

**CAD —** `POST /plugin/ventus_response_module/dispatch/major-incidents` (session auth; same body shape; `client_request_id` optional). `GET` list open: `?status=open`. `POST .../major-incidents/<id>/close` to clear banner.

---

## 5. Document control

| Version | Date | Notes |
|---------|------|--------|
| 1.0 | 2026-03-29 | Initial PRD; CAD-SCENE root cause + MDT client fix; CAD-MI-001 scope from QA index. |
| 1.1 | 2026-03-29 | Sparrow `my_status` on job detail GET; MDT single-request preference; §3 implementation status. |
| 1.2 | 2026-03-29 | CAD-MI-001 v1: DB, MDT/CAD APIs, sockets, banner, MDT UI; §4 API reference. |
| 1.3 | 2026-03-29 | MI follow-up: `job_comm` socket for linked CAD (inbox + job detail refresh); CAD styling/notifications for `major_incident`; MDT modal METHANE/TST/MITT structured fields. |
| 1.4 | 2026-03-29 | CAD **Declare MI** modal (production UX); server validation non-empty `payload`; inbox/MI row polish; banner close confirmation. |
