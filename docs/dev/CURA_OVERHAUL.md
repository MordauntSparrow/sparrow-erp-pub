# Cura overhaul — Sparrow architecture

**Live checklist:** `docs/dev/CURA_TRACKING.md` — keep in sync with this doc and `docs/dev/CODEX_HANDOVER_TEMPLATE.md` (fill the handover to close SPA/API parity items).

## Single module: Cura = `medical_records_module`

**Cura is not a separate plugin.** The **`medical_records_module`** is Cura’s **brain and data store** in Sparrow ERP.

All Cura capabilities are implemented **inside this one module**:

| Capability | Status in Sparrow | Notes |
|------------|-------------------|--------|
| **EPCR** (cases, sections, hospital case access, structured search) | **Existing** — evolve APIs, sync, schemas | Core `cases` model + `/api/cases`, `/api/search`, public case-access flow |
| **Safeguarding** | **Growing** — tables + routes under this module | Offline-friendly, idempotent sync where the product requires it |
| **Patient contact / welfare encounters** | **Growing** — tables + routes under this module | Generic clinical or welfare documentation tied optionally to an operational period |
| **Operational grouping (“events”)** | **Growing** — admin/control surface under this module | **Metrics should follow completed records** (submitted forms), not only static metadata |

The **separate SPA (e.g. Vite)** may expose safeguarding, encounter workflows, and operational grouping **in one client**; Sparrow extends **`medical_records_module`** so the backend matches that product **without** new top-level plugins for each workflow.

## Why keep one module

- **One clinical / safeguarding datastore** under shared governance, audit, and deployment.
- **One deploy / one database** per tenant already maps to “one ERP + one medical module.”
- Simpler **RBAC**, **logging**, and **DSPT-style evidence** (“where does sensitive health data live?” → `medical_records_module` + DB).

## API layout

Keep a **consistent prefix** for Cura JSON APIs:

- `/plugin/medical_records_module/api/...`

**EPCR (existing / evolving):** `.../api/cases`, `.../api/cases/<id>`, `.../api/cases/<id>/close`, `.../api/search`, `.../api/ping`

**EPCR case JSON + DB columns (optional dispatch / versioning):** responses include **`dispatchReference`**, **`primaryCallsign`**, **`dispatchSyncedAt`**, **`recordVersion`** (from `cases` columns added by migration `001`). Clients may send the same keys on **POST/PUT**; they are stored in columns and **stripped** from the `data` JSON blob to avoid duplication. **PUT/POST upsert** and **`/close`** support optimistic concurrency: send **`recordVersion`** matching the server; on mismatch the API returns **409** with the current `recordVersion`. Successful **PUT** and **close** bump **`recordVersion`**.

**SPA / MDT JSON auth:** EPCR and Cura JSON routes accept **`Authorization: Bearer <JWT>`** (see `app/auth_jwt.py`) in addition to the browser session cookie. **`POST /api/cura/auth/token`** exchanges username/password for a token when the user’s role is allowed for the JSON APIs (`crew`, `admin`, `superuser`, `clinical_lead`). Configure **`JWT_SECRET_KEY`** and token lifetime for production.

**Cura extensions (under `/api/cura/`):**

- `GET /api/cura/capabilities` — schema version (**4**) and feature flags (Bearer auth, assignments, analytics, dispatch preview, multipart upload, etc.)
- `GET|POST /api/cura/operational-events` — operational periods / deployments for grouping work
- `GET /api/cura/operational-events/by-slug/<slug>` — resolve one operational period by slug
- `GET|PATCH /api/cura/operational-events/<id>` — read or update (PATCH: **creator** or **admin / clinical_lead / superuser**); body may include **`enforce_assignments`** / **`enforceAssignments`** (migration **`004`**)
- `GET /api/cura/operational-events/<id>/record-counts` — counts of patient-contact reports and safeguarding referrals **linked to** that period, grouped by `status` (for dashboards)
- `GET /api/cura/operational-events/<id>/analytics-summary?min_cell=` — same idea as record-counts with **per-status suppression** when a cell is below **`min_cell`** (default **5**); totals are not suppressed
- `GET|POST /api/cura/operational-events/<id>/assignments` — list principals / add assignment (**privileged** or event **creator**); **`DELETE .../assignments/<assignment_id>`** to remove
- `GET|POST /api/cura/safeguarding/referrals` — list (optional `?operational_event_id=`) or create (`idempotency_key`, optional `operational_event_id` after migration `002`). When the linked event has **`enforce_assignments`**, non-privileged users must be **assigned** to that event to create.
- `GET /api/cura/safeguarding/referrals/by-public-id/<uuid>` — fetch by stable `public_id`
- `GET|PATCH /api/cura/safeguarding/referrals/<id>` — read or update (PATCH: **`record_version`** / **`expected_version`**; **creator** may edit non-terminal statuses; **submitted / closed / archived** only privileged roles; **admin / clinical_lead / superuser** may always PATCH)
- `GET|POST /api/cura/patient-contact-reports` — list (`?operational_event_id=` optional) or create (same **assignment enforcement** when `operational_event_id` is set and the event enforces assignments)
- `GET /api/cura/patient-contact-reports/by-public-id/<uuid>` — fetch by `public_id`
- `GET|PATCH /api/cura/patient-contact-reports/<id>` — read or update (same versioning; **submitter** may edit until **submitted / closed / archived**; privileged roles may always PATCH)
- `GET|POST /api/cura/attachments` — list or register **metadata** (`entity_type` = `safeguarding_referral` | `patient_contact_report`, `entity_id`, `storage_key`, optional filename / mime / size / checksum)
- `POST /api/cura/attachments/upload` — **multipart** upload (`form`: `entity_type`, `entity_id`; `file`: binary). Files are stored under the plugin’s **`data/cura_uploads/`**. Max size: env **`CURA_UPLOAD_MAX_MB`** (default **25**).
- `DELETE /api/cura/attachments/<id>` — remove metadata (**privileged** roles only)
- `GET /api/cura/dispatch/preview?reference=` — **best-effort** read from **`mdt_jobs`** when present (Ventus / shared DB); shape may vary by deployment

**Care company portal (optional, code retained):** **Off by default.** Set **`CURA_ENABLE_CARE_COMPANY_PORTAL=true`** to serve **`/care_company/*`** and show care-company admin UI in the medical module. **`CURA_DISABLE_CARE_COMPANY_PORTAL=true`** always forces **503** on the public portal and blocks admin screens (overrides enable). Manifest **`care_company_portal_enabled`** is a UI hint only.

**Public EPCR case access (hospitals / SAR):** **`GET|POST /case-access`** at site root (not under `/care_company`). Subdomain reverse-proxy (e.g. `epcr.customer.tld` → this path) is deployment-specific.

Run **`python app/plugins/medical_records_module/install.py upgrade`** so migrations apply: **`001_cura_foundation`**, **`002_safeguarding_operational_event`**, **`003_cura_file_attachments`**, **`004_assignments_and_enforcement`** (assignments table + **`enforce_assignments`** on operational events).

Exact paths should stay aligned with any **handover contract** for the SPA so client changes stay small. Legacy names in docs or UI should be treated as **logical** labels only if the implementation lives here.

## Product context (field use)

- **Mobile and intermittent connectivity** — design for **offline-first**, **bursty sync**, **idempotent writes**, without brittle always-on VPN assumptions.
- **Per-tenant isolation:** separate domain, service, database, and ERP instance per customer (existing model).

## Standalone Cura (no dispatch / CAD)

Cura **must remain fully usable without CAD or dispatch integration** — a **hard product requirement**.

Typical situations:

- Services where **full dispatch is not used** but **clinical and welfare documentation** still is.
- **Walk-up** encounters with **no linked dispatch job**.
- Deployments where **CAD integration is off**, not licensed, or out of scope.

**Engineering implications:**

- **No mandatory dispatch link** on a case or record: link fields are **nullable**; core flows do not depend on “CAD must exist.”
- **EPCR, safeguarding, patient-contact reports, and operational grouping** must **create, save, sync, and close** with **manual** identity, location, and timings when no external system is present.
- **CAD integration** (when present) is an **optional accelerator** (prefill, timings); treat external data as **“enhance when available”**, not a gate.
- **Operational identifiers** (e.g. callsign) remain useful **standalone** for habit, display, and audit; they do **not** require a live dispatch session.
- **Feature detection:** actions that pull from dispatch only when the deployment exposes those APIs and the user is permitted — otherwise hidden or disabled without noisy errors.

## Operational grouping — records as the source of truth

- The **operational picture** should reflect **actual activity**: primarily **counts / rates of submitted records** (and other form types you add), not only “open/closed” flags on a grouping row.
- **Dashboards / analytics** should aggregate from **submitted records** (with anonymisation / suppression rules where governance requires).
- **Assignments** (who may submit under which operational period) stay in Cura; **dispatch** (when present) adds **optional** context for linked episodes.

## Optional CAD / MDT integration

When a **dispatch or MDT module** is active for the same deployment, Cura can align with it **without** changing standalone behaviour.

### Identity

Staff may use an **operational identifier** (e.g. callsign) in Cura. With MDT active, that can align with **unit identity**; without MDT, it remains a **local org** identifier for audit and assignment.

### Prefill and timings (future slices)

When linking is enabled, design for:

- **From the dispatch record:** suggest structured fields already captured externally (identity, location, reference numbers, assigned units), without overwriting deliberate clinician edits without clear rules.
- **From unit / response timelines:** map **timestamps** into EPCR timing fields where product rules allow, to reduce duplicate keying.

### Linking model (design targets)

- On the **EPCR case** (or a small link table), keep **nullable** fields for: external job/reference, primary operational identifier, optional division, **sync state** / last pull time (null when never linked).
- Implementation choices (shared DB reads, hooks on status change, push from dispatch) must respect **auth**, **audit**, and **conflict handling** (merge vs forced replace — product decision).

### Offline

- Prefill is **best-effort when online**; offline creation and **later merge** must remain safe and predictable.

*Use existing job/unit/timing structures in your dispatch codebase as anchors when designing link APIs.*

## Implementation phases (scoped to one module)

1. **Auth** — session (or token) for SPA; roles; no mock production login.
2. **EPCR** — harden and extend APIs (versioning, idempotency, sections, file refs, list/closed semantics).
3. **Safeguarding + patient-contact + operational grouping** — persistence + routes **in this module**; DB via `install.py` / migrations (`mr_cura_migrations`).
4. **Files + datasets** — shared helpers **in this module** (or `app/` shared lib used only from here for Cura).
5. **Aggregated analytics** — compliant minimums / suppression; no re-identification.
6. **Care company portal** — deprecate or feature-flag when product no longer needs it; separate from the new field client story.

## Additional JSON surfaces (schema version **5**)

- **Auth:** `GET /api/cura/auth/me`, `POST /api/cura/auth/refresh`, `POST /api/cura/auth/logout` (see `cura_role_map.py` for handover role hints).
- **Safeguarding handover facade:** `/api/safeguarding/referrals` (+ `submit` / `close` / `DELETE` on draft) — same blueprint prefix as EPCR; canonical Cura paths unchanged.
- **EPCR ↔ safeguarding:** `POST /api/cura/safeguarding/referrals/prefill-from-epcr`; audit trail `GET /api/cura/safeguarding/referrals/<id>/audit`.
- **Attachments:** entity type `epcr_case` (case id); **download** `GET /api/cura/attachments/<id>/file` (authenticated, path hardening).
- **Datasets / settings:** `/api/cura/datasets/versions`, `/api/cura/datasets/<name>`, `/api/cura/config/app-settings` (`cura_datasets_routes.py`).
- **Minor injury:** `/api/cura/minor-injury/...` clinician + `/api/cura/minor-injury/admin/...` (`cura_mi_routes.py`; tables `cura_mi_*`, migrations `005`–`008`).

Run **`python app/plugins/medical_records_module/install.py upgrade`** for migrations **`005_cases_idempotency`** through **`008_minor_injury`** (and **`006_safeguarding_audit`**, **`007_cura_datasets_settings`**).

## Database upgrade

From repo root:

```bash
python app/plugins/medical_records_module/install.py upgrade
```

Or use the application’s **plugin upgrades** runner if your deployment invokes each plugin’s `install.py`.

## Related compliance / security docs

- `docs/compliance/` — DPIA scaffold, ROPA, retention, incident response, etc.
- EPCR JSON routes require **authenticated clinical roles** and **case assignment checks** (see `medical_records_module/routes.py`).

## Handover detail

The **full UI contract, section names, payloads, and offline queue model** for the SPA should live in a single **handover** used as the **API and schema checklist** while implementing **only** inside `medical_records_module`.

---

*Single module (`medical_records_module`); standalone-first; optional dispatch linkage; extension APIs under `/api/cura/`.*
