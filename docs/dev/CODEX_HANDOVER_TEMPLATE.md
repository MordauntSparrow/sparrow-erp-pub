# Codex → Sparrow Cura handover template

**Status:** Filled from [CURA_CLINICAL_SYSTEMS_HANDOVER.md](./CURA_CLINICAL_SYSTEMS_HANDOVER.md) and implemented Sparrow routes (`cura_routes.py`, `routes.py`, `safeguarding_facade_routes.py`, `cura_mi_routes.py`, `cura_datasets_routes.py`). Update this matrix when the SPA contract changes.

## 1. Base URLs

| Client env | Sparrow prefix |
|------------|----------------|
| Production | `https://<host>/plugin/medical_records_module` |
| EPCR JSON | `.../api/...` |
| Cura JSON | `.../api/cura/...` |
| Safeguarding facade (handover paths) | `.../api/safeguarding/...` |
| Minor injury (canonical) | `.../api/cura/minor-injury/...` |
| Datasets / settings | `.../api/cura/datasets/...`, `.../api/cura/config/...` |
| Public case access (browser form; not SPA) | `https://<host>/case-access` |
| Care company portal (optional; default off) | `https://<host>/care_company/...` only if `CURA_ENABLE_CARE_COMPANY_PORTAL=true` |

## 2. Auth

- [x] SPA uses **Bearer JWT** from `POST .../api/cura/auth/token`
- [x] Or **session cookie** from existing Flask login
- [x] `GET .../api/cura/auth/me` — profile + `handover_roles` (clinician, event_lead, safeguarding_reviewer, admin)
- [x] `POST .../api/cura/auth/refresh` — new JWT from valid Bearer (sliding session)
- [x] `POST .../api/cura/auth/logout` — acknowledgement only; client discards token (no server-side revoke unless a denylist is added)
- **Token refresh strategy:** Call `/api/cura/auth/refresh` with current `Authorization: Bearer` before expiry; `SESSION_TOKEN_EXPIRY_HOURS` / `JWT_SECRET_KEY` — see [CURA_OVERHAUL.md](./CURA_OVERHAUL.md).

## 3. Endpoint matrix (primary)

| SPA action | Method | Path | Request body (keys) | Response shape |
|------------|--------|------|---------------------|----------------|
| Health / ping | HEAD/GET | `/api/ping` | — | `200` / `{ok}` |
| Patient lookup | GET | `/api/search` | `date_of_birth`, `postcode` | `patients[]` |
| List cases | GET | `/api/cases` | `?username=` (privileged) | `Case[]` + sync fields |
| Create / upsert case | POST | `/api/cases` | `id`, `sections`, `assignedUsers`, optional `idempotencyKey` / header `Idempotency-Key` | `case`, `serverAck` |
| Get case | GET | `/api/cases/<id>` | — | Case JSON + `recordVersion` |
| Update case | PUT | `/api/cases/<id>` | Full/partial case + `recordVersion` | `case`, `serverAck` |
| Close case | PUT | `/api/cases/<id>/close` | `closedAt`, `sections`, `recordVersion`, optional idempotency | `message`, `serverAck` |
| Cura capabilities | GET | `/api/cura/capabilities` | — | `schema_version` (currently **6**), `features` |
| Operational events | GET/POST | `/api/cura/operational-events` | event fields | `items` / `item` |
| Safeguarding (Cura) | GET/POST | `/api/cura/safeguarding/referrals` | `payload`, `idempotency_key`, filters | `items` / `item` |
| Safeguarding (facade) | GET/POST | `/api/safeguarding/referrals` | Handover-shaped JSON | Referral envelope |
| Safeguarding one | GET/PUT/DELETE | `/api/safeguarding/referrals/<id>` | PUT: `data`, `status`, `record_version` | Referral |
| Submit / close (facade) | POST | `/api/safeguarding/referrals/<id>/submit` / `.../close` | optional `record_version` | Referral |
| Prefill from EPCR | POST | `/api/cura/safeguarding/referrals/prefill-from-epcr` | `case_id` | Minimal PHI bundle |
| Safeguarding audit | GET | `/api/cura/safeguarding/referrals/<id>/audit` | `?limit=` | `items[]` |
| Patient-contact | GET/POST | `/api/cura/patient-contact-reports` | same pattern as safeguarding | `items` / `item` |
| Attachments | GET/POST/DELETE | `/api/cura/attachments` | `entity_type`, `entity_id` | metadata |
| Attachment upload | POST | `/api/cura/attachments/upload` | multipart | `item` |
| Attachment file | GET | `/api/cura/attachments/<id>/file` | — | binary stream (auth) |
| MI assigned events | GET | `/api/cura/minor-injury/events/assigned` | `?userId=` | `items[]` |
| MI event | GET | `/api/cura/minor-injury/events/<id>` | — | `item` |
| MI notices / status | GET | `/api/cura/minor-injury/events/<id>/notices`, `/status` | — | list / `item` |
| MI submit report | POST | `/api/cura/minor-injury/events/<id>/reports` | `payload`, `idempotency_key` | `item` |
| MI admin events | GET/POST | `/api/cura/minor-injury/admin/events` | event body | CRUD |
| MI admin reports queue | GET | `/api/cura/minor-injury/admin/reports/pending` | — | rejected / late |
| Dataset versions | GET | `/api/cura/datasets/versions` | — | `{ name: version }` |
| Dataset CRUD | GET/PUT | `/api/cura/datasets/<name>` | PUT: `{ payload }` | dataset JSON |
| App settings | GET/PUT | `/api/cura/config/app-settings` | PUT: `{ settings: {} }` | merged settings |

## 4. Payload schemas

### Safeguarding referral `payload` JSON

Opaque JSON stored in `cura_safeguarding_referrals.payload_json`. The UI captures fields listed in **CURA_CLINICAL_SYSTEMS_HANDOVER** (subject, concern, consent, agencies, etc.). Server does not require a fixed schema; optional validation can be added later.

### Patient contact report `payload` JSON

Opaque JSON in `cura_patient_contact_reports.payload_json`; optional link `operational_event_id`.

### EPCR case `sections` (if custom)

Array of `{ "name": string, "content": object }`. Required section **names** include `PatientInfo`, `OBS`, airway/breathing/circulation/disability, pathways, **`Review`**, **`Drugs Administered`**, etc. — see handover § “Required section names”.

## 5. Offline / sync

- **Idempotency header/key field name:** `Idempotency-Key` (header) and/or `idempotency_key` / `idempotencyKey` (JSON body). **EPCR cases:** stored on `cases.idempotency_key` (create replay). **Close:** `close_idempotency_key` on successful close.
- **Queue retry rules:** Client exponential backoff; replay same idempotency key until `200/201` with `serverAck`.
- **Conflict resolution:** `recordVersion` optimistic lock — mismatch returns **409** with current `recordVersion`; client merges or refetches.

## 6. Status enums

- **Operational event statuses:** `draft`, `active`, `completed`, `closed`, etc. (see `cura_operational_events.status`).
- **Safeguarding statuses:** `draft`, `submitted`, `closed`, `archived` (+ sync fields).
- **Patient-contact statuses:** `draft`, `submitted`, `closed`, `archived`.
- **Minor injury event statuses:** `upcoming`, `active`, `completed`, `closed`.
- **Minor injury report statuses:** `draft`, `submitted`, `rejected`, `void`.

## 7. Sign-off

- [x] Engineering reviewed against `cura_routes.py` + `routes.py` EPCR handlers + facade/MI/datasets modules  
- [x] Care company portal **off by default** (`CURA_ENABLE_CARE_COMPANY_PORTAL`); public case access at **`/case-access`**; capabilities **`schema_version`** = **6**  
- [ ] QA signed on staging  

---

*Tick **A3** and **A4** in `CURA_TRACKING.md` when this stays aligned with production SPA behaviour.*
