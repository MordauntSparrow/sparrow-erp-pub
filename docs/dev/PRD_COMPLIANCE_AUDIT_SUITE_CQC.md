# PRD: Unified compliance & audit suite (CQC-oriented)

**Role:** System overseer plan — module vs extension decision, CQC-aligned scope, and implementation phases.  
**Status:** Implemented as plugin `compliance_audit_module` v1.2 — all v1.1 features plus **governance eDiscovery** (saved evidence matters, actor/entity/action filters, optional `matter_reference` / `matter_id` on exports) and **SIEM bridge** (in-app dashboard, JSON APIs `GET .../api/siem-snapshot` and `GET .../api/audit-events` with session or `COMPLIANCE_AUDIT_SIEM_TOKEN`). Run `python app/plugins/compliance_audit_module/install.py upgrade`; grant `compliance_audit_module.access`, optional `inspection` / `export`. Optional: `COMPLIANCE_AUDIT_SCHEDULER=0` to disable scheduled ZIP jobs.  
**Related systems:** Sparrow ERP (`medical_records_module`, `ventus_response_module`, `admin_staff_audit`, core users), Cura/ePCR payloads, MDT/CAD comms.

---

## 1. Overseer decision: module vs extension

| Option | Verdict |
|--------|---------|
| **Extend** existing `admin_staff_audit` only | Insufficient alone: that trail is admin-focused; CQC needs **patient-centred** and **operational** evidence across CAD, ePCR, MI, messages, timings. |
| **New plugin: `compliance_audit_module` (recommended)** | **Single “Compliance & assurance” console** that **aggregates** existing stores (read-mostly), adds **export jobs**, **export audit**, and **step-up PIN** — without rewriting clinical/CAD write paths. |

**Principle:** Do not duplicate source-of-truth writes. The suite is a **governance layer**: index, search, correlate, export, and prove who accessed what.

---

## 2. What CQC expects (research summary)

The Care Quality Commission regulates health and adult social care in England. While **CQC does not prescribe a single software layout**, inspection and regulations drive consistent themes your UI and exports should satisfy.

### 2.1 Legal / regulatory anchors (official)

- **Access to records (inspection):** Inspectors may require access to records; obstruction without reasonable excuse can be an offence — providers must enable **timely** access to digital records on site.  
  Source: [Accessing digital records on site visits (CQC)](https://www.cqc.org.uk/guidance-providers/adult-social-care/digital-record-systems-adult-social-care-services/accessing-digital-records-site-visits) (powers under Health and Social Care Act 2008 s.63, etc.).

- **Regulation 17 — Good governance:** Providers need effective governance, including **assurance and auditing** processes; they must **securely maintain accurate, complete and detailed records** about people using the service, staff employment, and management of the regulated activity — and be able to **detect risks** and drive improvement.  
  Source: [Regulation 17: Good governance (CQC)](https://www.cqc.org.uk/guidance-providers/regulations-enforcement/regulation-17-good-governance).

- **Digital / information governance:** CQC’s digital record guidance (e.g. adult social care digital records programme) stresses **read-only** inspector access where possible, **appropriate access controls**, and alignment with **data security** expectations (e.g. DSPT for NHS-traded services — apply proportionally to your context).  
  Sources: [Points to consider](https://www.cqc.org.uk/guidance-providers/adult-social-care/digital-record-systems-adult-social-care-services/points-to-consider), [Assessing and inspecting](https://www.cqc.org.uk/guidance-providers/adult-social-care/digital-record-systems-adult-social-care-services/assessing-and-inspecting).

- **UK GDPR / Data Protection Act 2018:** Exports and viewing sensitive data require **lawful basis**, **purpose limitation**, **minimisation**, and **accountability** (logging access and exports supports Article 5(2) accountability).

### 2.2 What inspectors typically need to *see* (product implications)

Not an exhaustive legal list — a **practical** set derived from CQC themes and field inspection practice:

| Theme | What to surface in one place |
|--------|------------------------------|
| **Person-centred trail** | Per patient/service user: episodes, ePCR/MI lifecycle, key decisions, who did what and when. |
| **Contemporaneity & completeness** | Timestamps, version or amendment indicators, “reason for change” where policy requires it. |
| **Medicines & interventions** | Drug administrations, refusals, consent posture (aligned to your clinical model). |
| **Safeguarding & escalation** | Flags, referrals, CAD/MDT panic or priority events linked to incident IDs. |
| **Staff & competency** | Who was on shift, sign-offs, role, training flags if stored in Sparrow/HR. |
| **Communications** | Dispatcher ↔ crew / CAD comms, MDT messages (as policy allows — redaction rules). |
| **Governance actions** | Admin overrides (e.g. force-close EPCR), configuration changes affecting care delivery. |

**Layout expectations (CQC-friendly, not mandated):**

- **Inspection mode:** Read-only; fast **filters** (date range, patient ID, CAD, ePCR ID, division, staff).  
- **Chronological “storyline”** per case with **typed events** (clinical, dispatch, message, config).  
- **Evidence pack:** One-click **export bundle** (PDF index + CSV/JSON attachments) for a date range or single episode — with **watermark**, **export manifest**, and **hash** optional.

---

## 3. Scope: data domains to ingest (everything relevant)

| Domain | Likely source in stack | Notes |
|--------|-------------------------|--------|
| **ePCR** | `medical_records_module` + Cura sync tables | Sections, timings, signatures, amendments; link `external_ref` / case IDs. |
| **Minor injury / MI** | Medical records routes/templates | Same trail as ePCR where separate tables exist. |
| **CAD / jobs** | `ventus_response_module` — `mdt_jobs`, comms, status transitions | Intake/triage, panic, assignments, closures. |
| **Intake forms** | `response_triage`, job `data` JSON | Reason for call, priority source, division. |
| **Crew / dispatcher messages** | `messages`, `mdt_job_comms`, incident update fan-out | Policy: redact third parties if required. |
| **Timings** | Job timestamps, ePCR timers, CAD stack events | Normalise to UTC + display local. |
| **Admin / staff audit** | `admin_staff_audit` (and siblings) | Who changed settings, user admin, force-close (when built). |
| **Identity / access** | Login events if logged | Optional phase 2. |
| **Inventory / kit** (if regulated context) | `inventory_control` | If CQC scope includes equipment traceability for your registration. |

**Proportionate eDiscovery & SIEM (v1.2, governance — not enterprise legal/SIEM replacement):**

| Capability | What Sparrow provides | What it is *not* |
|------------|------------------------|------------------|
| **eDiscovery-style workflow** | Saved **matters** (named filter scopes), merged **timeline**, **ZIP evidence packs**, **export log** with SHA-256 and optional matter reference / matter id | Relativity-style review, legal hold on third-party mailboxes, predictive coding, counsel production sets |
| **SIEM-style monitoring** | **Dashboard**: login audit, PIN step-up, export counts, sampled timeline volume by domain; **JSON pull APIs** for external SIEM/SOAR on a schedule | Fleet-wide agent telemetry, real-time UEBA, Splunk/Sentinel/Elastic replacement |

Non-Sparrow systems remain **out of scope** unless ingested elsewhere (e.g. CSV) into Sparrow.

---

## 4. Functional requirements

### 4.1 Unified audit console

- Role-gated: e.g. `compliance_viewer`, `compliance_exporter`, `clinical_governance` (exact keys TBD with your permissions registry).  
- **Global search:** patient identifier (as configured), CAD, ePCR id, date range, event type, user.  
- **Case timeline:** merged stream from registered **adapters** (pluggable per domain).  
- **Reasoning / justification:** surface structured fields (e.g. override reason, DNAR workflow notes) where stored; do not invent free-text if not captured at source — **gap analysis** report instead.

### 4.2 Export

- Formats: **PDF** (human-readable summary + index), **CSV** (tabular audit lines), optional **JSON** (machine-readable bundle).  
- **Export manifest:** scope (filters used), row counts, generator version, time generated.  
- **Export audit log:** append-only table: `who` (user id), `when`, `what scope`, `format`, `ip` (if available), `pin_step_up_ok`, `file_hash` or storage key if written to disk/blob.  
- Rate limit / size cap to protect production DB.

### 4.3 Step-up authentication (personal PIN)

- Before **export** or **viewing highly sensitive** bundles: re-verify with **user’s compliance PIN** (stored as **hash**, never plaintext) or org SSO step-up if available.  
- Lockout / backoff after N failures; audit each attempt.  
- PIN is **per user**, not shared; rotation policy configurable.

### 4.4 Security & privacy

- Field-level **redaction** rules for exports (e.g. national identifiers if not needed for the export purpose).  
- Separate **“inspection read-only”** role mirroring CQC guidance (use provider workstation, read-only).  
- Retention: exports stored **temporarily** (e.g. 24–72h) unless org policy extends.

---

## 5. Technical architecture (high level)

```
┌─────────────────────────────────────────────────────────┐
│           compliance_audit_module (new)                 │
│  UI: Assurance console + Export wizard + PIN gate       │
│  Services: Query orchestrator, Export job runner         │
│  Tables: compliance_export_log, user_compliance_pin_hash │
│  Adapters: ePCR, CAD, messages, admin_staff_audit, …     │
└───────────────────────────┬─────────────────────────────┘
                            │ read / COPY (no double-write
                            ▼ of clinical facts)
┌─────────────────────────────────────────────────────────┐
│  Existing modules & tables (source of truth)             │
└─────────────────────────────────────────────────────────┘
```

- **Adapters** implement a small interface: `list_events(filters) -> iterable[AuditEvent]`.  
- **AuditEvent** normalises: `occurred_at`, `actor`, `action`, `entity_type`, `entity_id`, `summary`, `detail_ref`, `integrity_hint` (e.g. source row id).

---

## 6. Phased delivery

| Phase | Deliverable |
|-------|-------------|
| **P0** | Plugin scaffold, roles, PIN enrolment + verify endpoint, export audit log table. |
| **P1** | CAD + intake + `mdt_job_comms` + `admin_staff_audit` adapters; basic console + CSV export + export logged. |
| **P2** | ePCR / medical_records adapter (incl. MI), PDF bundle, redaction profiles. |
| **P3** | “Inspection mode” UX, scheduled reports, NHS DSPT **evidence orientation** page (links to official dsptoolkit.nhs.uk; Sparrow does not host DSPT submission). |
| **P4** | Governance **eDiscovery** (saved matters, matter-linked timeline/export, export log annotations) and **SIEM bridge** (dashboard + token- or session-authenticated JSON APIs). |

---

## 7. Acceptance criteria (sample)

1. A governance user can filter **last 7 days** of CAD + comms + admin audit in one timeline.  
2. Export requires **successful PIN step-up**; failed attempts are audited.  
3. Every successful export writes an **immutable** row to `compliance_export_log` with scope metadata.  
4. No clinical write path is bypassed or duplicated by the compliance module.  
5. Documentation links CQC themes (Reg 17, inspection access) for **trust board** packs — not legal advice.

---

## 8. Open questions (clarification for your organisation)

1. **Registration scope:** CQC-registered for **transport/treatment** only, or also **personal care**? (Determines which record types are in scope for templates and training.)  
2. **Data residency:** Must exports and scheduled ZIPs stay on **UK/EU-only** storage paths?  
3. **Patient pseudonymisation:** When is pseudonymised data acceptable vs full operational exports?  
4. **Cura / ePCR read path:** Confirm batch read model for any fields still synced from Cura.  
5. **PIN vs MFA:** Confirm with IG/counsel for DPIA (compliance PIN remains the in-product step-up until SSO step-up is available).

---

## 9. References (official)

- [CQC — Regulation 17: Good governance](https://www.cqc.org.uk/guidance-providers/regulations-enforcement/regulation-17-good-governance)  
- [CQC — Accessing digital records on site visits](https://www.cqc.org.uk/guidance-providers/adult-social-care/digital-record-systems-adult-social-care-services/accessing-digital-records-site-visits)  
- [CQC — Digital record systems: points to consider](https://www.cqc.org.uk/guidance-providers/adult-social-care/digital-record-systems-adult-social-care-services/points-to-consider)  
- [CQC — Assessing and inspecting (digital records)](https://www.cqc.org.uk/guidance-providers/adult-social-care/digital-record-systems-adult-social-care-services/assessing-and-inspecting)  
- ICO / UK GDPR: [ICO for organisations](https://ico.org.uk/for-organisations/) (accountability & logging)

---

*This document is operational planning, not legal advice. Final compliance posture should be confirmed with your governance lead and, where appropriate, specialist counsel.*
