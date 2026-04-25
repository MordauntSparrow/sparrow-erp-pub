# PRD: Cura EPCR — coded conditions & presentations (SNOMED subset, server-synced dataset)

**Status:** Initial implementation landed (Sparrow + Cura v1)  
**Owner:** Product + Clinical governance + Engineering  
**Scope:** Sparrow (terminology ingest + tenant dataset) + Cura ecpr-fixit-buddy (offline search, case fields, sync).

**Implemented (v1):** Dataset slug `snomed_uk_ambulance_conditions` in `cura_tenant_datasets`; server module `cura_snomed_conditions.py` (Snowstorm `.../MAIN/concepts` + ECL chunks, not `.../browser/...`); Cura settings “Refresh from Snowstorm”; Cura dataset sync + pickers on Presenting Complaint / PMH / Family History. Configure via `SNOWSTORM_BASE_URL`, `SNOWSTORM_BRANCH`, `SNOWSTORM_MAX_CONCEPTS`, optional `SNOWSTORM_ECL_CHUNKS` (newline-separated ECL).

---

## 1. Research summary — how much to download for ambulance response EPCR?

### 1.1 What official artefacts imply (UK-centric; adaptable per jurisdiction)

| Source | Relevance |
|--------|-----------|
| **NHS Ambulance Data Set (ADS)** | National ambulance collection includes **chief complaint**, **acuity**, **diagnosis**, **injury** and related clinical fields — i.e. coded **presentations and conditions** are expected at dataset level, not only free text. See [NHS England — Ambulance Data Set](https://www.england.nhs.uk/urgent-emergency-care/improving-ambulance-services/ambulance-data-set/). |
| **Emergency Care Data Set (ECDS)** | ED-facing national set; **SNOMED CT** is used for many clinical elements. NHS Digital publishes **“Emergency care concepts”** and related products to support consistent coding (procedures, arrival mode, acuity, injury context, etc.). Aligning **prehospital “presentation”** with ECDS-friendly concepts helps handover and secondary use. |
| **UK refsets / subsets (examples)** | Integrated Urgent Care material references **Ambulance Request Type** SNOMED subsets for request typing. Broader **emergency care refsets** (e.g. treatments, arrival mode) illustrate the pattern: **bounded SNOMED subsets per data item**, not the full ontology. |

**Interpretation for product:** Ambulance EPCR should not ship “all of SNOMED.” It should ship **one or more bounded lists** (national refset, ECDS-aligned product, or an **Expression Constraint Language (ECL)** slice) sized for **field use** and **reporting**.

### 1.2 Recommended download / store size (engineering target)

| Tier | Purpose | Approx. concept count | Approx. minified JSON* | When to use |
|------|---------|------------------------|-------------------------|-------------|
| **A — Core ambulance list** | Chief complaint / suspected condition / common presentations for pickers | **1,500 – 5,000** | **~0.3 – 1.5 MB** | **Default** for most deployments; best UX and smallest sync. |
| **B — Extended clinical** | + broader disorders/findings for richer coding (still bounded) | **8,000 – 20,000** | **~2 – 6 MB** | Larger services or jurisdictions with a published medium refset. |
| **C — Not recommended for device bundle** | Wide “all clinical finding” style hierarchies | **50,000+** | **10 MB+** | Avoid on Cura; use server-side search or further split into multiple datasets. |

\*Per-concept payload: `conceptId` + preferred term (+ optional FSN, `active` flag). Gzip reduces wire size roughly **5–10×** for bulk lists.

**Recommendation:** **Target Tier A (1.5k–5k)** as the **primary** downloadable dataset for ambulance response, sourced from a **named refset or tight ECL** chosen per tenant (UK: prefer **NHS-published emergency/urgent care refsets** where license permits; other regions: national release + refset or clinically agreed ECL).

**Presenting complaint vs past history:** Same underlying **condition/finding** code system; UI may use **two pickers** (e.g. “this episode presentation” vs “relevant past conditions”) backed by the **same or overlapping** lists — no need to double download if the payload includes flags or separate sections in one versioned blob.

### 1.3 Terminology server / API (production posture)

- **Public Snowstorm training/demo endpoints** are for **testing**, not guaranteed for production load (see SNOMED International implementation docs). **Infrequent** batch extract (e.g. quarterly) per deployment reduces abuse risk but does **not** replace **license** and **terms-of-use** review.
- **Production:** Prefer **self-hosted Snowstorm** (or licensed hosted TS) + **licensed RF2 / national edition**, **or** a **static refset file** supplied by the customer’s NRC. Railway-friendly path: **scheduled job on Sparrow** calls allowed endpoint → writes dataset → Cura syncs.

---

## 2. Problem

- Cura EPCR uses **free text** for conditions, histories, and presenting complaint → **spelling variance**, weak **trending**, poor **interoperability** with ECDS/ADS-style reporting.
- Field devices have **limited connectivity**; live terminology search per keystroke is undesirable.

---

## 3. Goals

1. **Structured coding:** Clinicians **search/select** from a **versioned, offline** list on device; each selection stores **SNOMED concept identifier** + **display term** (and optional metadata).
2. **Server-side refresh:** Sparrow **periodically** (or on-demand admin action) **pulls** a bounded extract from a configured terminology source and **persists** it as a **tenant dataset** (same sync pattern as existing Cura datasets).
3. **Reporting:** Dashboards and exports can **group/filter** by **conceptId** (and release/snapshot id).
4. **Configurable scope:** Per deployment: base URL (or “file upload”), **ECL query and/or refset identifier**, language/preferred term preferences, max concepts **hard cap** (safety).
5. **Clinical safety:** Free text **remains available** where needed (e.g. narrative presenting complaint) with clear UX that **coded** fields are preferred for analytics.

---

## 4. Non-goals (initial release)

- Replacing **all** narrative fields with SNOMED (only **additional coded sections** + optional linkage).
- Hosting **Snowstorm/Elasticsearch** inside Sparrow’s default Railway image (optional future: separate service).
- Real-time **device → Snowstorm** queries.
- **Full** international SNOMED edition on device.

---

## 5. Solution outline

```
[ Terminology source — Snowstorm FHIR/REST or RF2/refset file ]
        │ infrequent batch (cron / admin “Refresh”)
        ▼
[ Sparrow job ]  validate, dedupe, cap count, JSON schema v1
        │
        ▼
[ cura_tenant_datasets ]  name e.g. `snomed_conditions_epcr` (or section in agreed dataset)
        │ version bump
        ▼
[ Cura dataset sync ]  download + local store
        │
        ▼
[ Cura UI ]  typeahead / picker → patient.case fields (conceptId + term)
```

---

## 6. Data artefacts

### 6.1 Dataset payload (conceptual schema v1)

```json
{
  "schemaVersion": 1,
  "snapshotId": "uuid-or-hash",
  "generatedAt": "ISO-8601",
  "source": {
    "type": "snowstorm_fhir | snowstorm_native | static_upload",
    "baseUrl": "optional",
    "refsetId": "optional",
    "ecl": "optional",
    "snomedEditionUri": "optional",
    "snomedVersion": "optional"
  },
  "concepts": [
    {
      "conceptId": "string",
      "pt": "preferred term",
      "fsn": "optional fully specified name",
      "active": true
    }
  ]
}
```

### 6.2 EPCR record fields (Cura + API contract)

| Field | Type | Notes |
|-------|------|--------|
| `presentingComplaintSnomed` | `{ conceptId, pt, snapshotId? }` or array if multi-select | New; parallel to existing free-text complaint. |
| `suspectedConditionsSnomed` | array of same | “Suspected conditions / illnesses” panel. |
| `pastMedicalHistorySnomed` | array of same | Optional same release; UX may differ. |

Exact naming should match existing Cura camelCase / Sparrow JSON conventions; align with any existing patient payload schema.

---

## 7. Functional requirements

### 7.1 Sparrow

1. **Configuration** (per tenant or global plugin setting): terminology URL, auth if any, ECL/refset, locale, **maxConcepts** (default **5000**), timeout, retry.
2. **Job:** “Refresh SNOMED conditions dataset” — idempotent; logs row count, duration, errors; respects **maxConcepts** with deterministic truncation + warning in admin UI.
3. **Persistence:** Write to `cura_tenant_datasets` with incremented **version**; optional admin preview diff (count only for v1).
4. **Security:** Secrets in env; no PII in terminology requests; rate-limit admin trigger.
5. **API:** Existing Cura dataset download path serves new dataset name (document in ops runbook).

### 7.2 Cura

1. Register dataset in `datasetService` (or equivalent); sync on existing pipeline.
2. **Picker component:** offline search (prefix + simple fuzzy if already used elsewhere); shows **pt**; stores **conceptId + pt**.
3. **EPCR panels:** add **“Coded presentation / conditions”** section; keep free text where product agrees.
4. **Offline:** no network required after sync; show **snapshot date** in settings or debug if useful.

### 7.3 Reporting / Sparrow medical plugin

1. Extract/report **coded fields** for trends (counts by `conceptId`).
2. Backward compatibility: legacy cases **null** coded fields.

---

## 8. Configuration defaults (proposal)

| Key | Default | Notes |
|-----|---------|--------|
| `max_concepts` | `5000` | Aligns with Tier A–B. |
| `refresh_schedule` | `manual + optional quarterly cron` | Tenant-configurable. |
| `dataset_name` | `snomed_conditions_epcr` | Single blob v1. |

---

## 9. Compliance & licensing

- Confirm **SNOMED CT affiliate / NRC** rights for the **edition** and **refset** used in production.
- Document **snapshotId** / **edition** on each export for audit.
- Public demo Snowstorm: **not** recommended for production clinical load; acceptable only if **legal/compliance** approves infrequent technical extract (organisation-specific).

---

## 10. Milestones

1. **Clinical + legal:** Choose refset/ECL scope (UK vs international); confirm license path.  
2. **Sparrow:** Settings model + ingest job + dataset write + admin “Refresh” + logging.  
3. **Cura:** Dataset type + picker UI + patient payload fields + migration/default.  
4. **Sparrow reporting:** Aggregations by `conceptId` (minimal v1).  
5. **Runbook:** Railway cron, failure alerts, restore from static upload fallback.

---

## 11. Acceptance criteria

1. After **Refresh**, `cura_tenant_datasets` version increases and payload size is **≤ agreed cap** (e.g. ≤ 6 MB JSON or tenant override).
2. Cura **airplane mode:** user can search and save **coded** presentation + suspected conditions without network.
3. New case JSON contains **conceptId** stable across app restarts; free text fields unchanged unless product removes them later.
4. Admin can **re-run** refresh without duplicating concepts (idempotent merge/dedupe by `conceptId`).
5. Reporting shows **top N concepts** for a date range on pilot data.

---

## 12. Open questions

- Single **global** list vs **separate** refsets for “complaint” vs “PMH”?  
- **Multi-select** limits per field?  
- **Map** local legacy free-text to SNOMED post-hoc (ML/NLP) — out of scope v1?  
- **ICD-10/11** mapping layer for billing — future?

---

## 13. References (starting points)

- NHS England — [Ambulance Data Set](https://www.england.nhs.uk/urgent-emergency-care/improving-ambulance-services/ambulance-data-set/)  
- NHS Digital — Emergency Care Data Set (ECDS) and emergency-care SNOMED products  
- SNOMED International — [Is there a SNOMED API?](https://docs.snomed.org/implementation-guides/implementation-fact-sheets/technology-adoption/is-there-a-snomed-api) (verify current training URL and ToS)  
- Snowstorm — [GitHub IHTSDO/snowstorm](https://github.com/IHTSDO/snowstorm)  
- Integrated Urgent Care — Ambulance Request Type SNOMED material (NHS Connect archive / successors)

---

*PRD started: aligns Tier A–B sizing with NHS-style ambulance + emergency care coding expectations; implementation details to be refined after jurisdiction and refset choice.*
