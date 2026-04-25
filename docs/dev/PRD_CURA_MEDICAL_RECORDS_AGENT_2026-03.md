# PRD — Cura / Medical Records Agent (ePCR)

**Scope:** ePCR / clinical chart UX, data binding, and consent flows from client field report (Phill → Jordan).  
**Source index:** [QA_CLIENT_REPORT_ROUTING_2026-03.md](./QA_CLIENT_REPORT_ROUTING_2026-03.md)  
**Repo:** Cura (`ecpr-fixit-buddy`) + Sparrow `medical_records_module` where payloads / PDF / admin apply.

**Acceptance (global):** Each ID below has **test notes or screenshots** before sign-off. **OBS** and **cardiac arrest** items require the **section matrices** in this doc to be filled during implementation. Where billing UI is removed from drugs, **no billing/cost language** remains on patient-facing drug screens.

**Dependencies:** DANISH / STAB5 wording from clinical lead; DNAR photo gate from legal/clinical governance.

**PRD MVP closure (2026-03-31):** All in-scope client IDs below are **Implemented** or explicitly **Out of MVP** / **Blocked**. **CURA-ZOLL-001** remains documentation-only (§3) — no Bluetooth client in this tranche. **CURA-VOICE-001** remains blocked until reproducible client video is available.

---

## 1. Deliverables by ID

| ID | Summary | Status | Test notes |
|----|---------|--------|------------|
| **CURA-LABEL-001** | Rename “Quick-trach2” → surgical airway / FONA (front of neck access). Final string subject to clinical sign-off. | Implemented (React + Vue UI strings) | Screenshot Airway tab: section title, button, EtCO₂ label, treatment chip (Vue). |
| **CURA-GI-001** | System Reviews — Gastro: abdominal diagram + quick-add buttons, parity with chest pattern. | Implemented (React) | Systems Review when system = Gastrointestinal. |
| **CURA-BREATH-001** | Chest decompression quick actions: L/R; 2nd ICS mid-clavicular and/or 5th ICS mid-axillary; finger thoracostomy L/R. | Implemented (React) | B Breathing — thoracic decompression log. |
| **CURA-CHEST-001** | Chest quick adds: **No Air Entry**. | Implemented (`BreathingTab` React + Vue) | Screenshot diagram modal sign list + marker on chest. |
| **CURA-ZOLL-001** | Circulation — Bluetooth upload ECG/OBS from ZOLL Lifepak (CorPlus). Feasibility, privacy, phased delivery. | **Out of MVP** — §3 only | No BLE implementation in this PRD; spike / vendor work tracked separately. |
| **CURA-EXP-001** | Exposure wound care quick adds: glue, staples; wound type **skin tear**. | Implemented | `datasetService` defaults + Exposure tab. |
| **CURA-EXP-002** | Fracture assessment: Open / Closed / Other + free text for “other”. | Implemented | Exposure — fracture type presets + Other. |
| **CURA-BURN-001** | Burns: shade area → auto % TBSA. | Implemented (MVP) | Adult **anterior** diagram hint + rule-of-nines chips on confirm; paediatric / Lund-Browder not in MVP. |
| **CURA-BURN-002** | Burns depth: **Superficial** alongside partial / full thickness. | Implemented | Exposure — burn thickness toggle. |
| **CURA-OBS-001** | OBS from Primary Survey must flow through — no duplicate entry; OBS populates everywhere **except Breathing** per client. Encode in rules + tests. | Implemented (React) | `submitObs` no longer writes B Breathing; `obsPropagation.ts`; Circulation shows latest OBS hint. |
| **CURA-VOICE-001** | Voice-to-text: repeating / dropped words — reproduce with client video; fix or change engine/settings. | **Blocked** on video | Attach evidence to ticket; no engine change in this PRD. |
| **CURA-TIMER-001** | Case timer populates linked text fields and drug section as designed. | Implemented (React) | Laps → `OBS.caseTimerLaps`; drug laps → `OOHCA.arrestDrugs`. |
| **CURA-CA-001** | Cardiac arrest: text boxes + drug tab population / sign-off. | Implemented | OOHCA + timer → drugs; **Case Review** — OOHCA count prompt + optional `cardiacArrestReviewSignoff` field. |
| **CURA-CA-002** | CA timers: arrest, rhythm check, meds. | Implemented | Case timer + OBS laps; OOHCA shock/drug/rhythm tables; drug-tagged laps sync to OOHCA. |
| **CURA-CA-003** | Reversible causes: copy “tick yes when considered or reversed” + time log into narrative. | Implemented (React) | Considered / Reversed + optional time + narrative. |
| **CURA-CA-004** | HOTT for trauma arrest (wording clinical sign-off). | Implemented (React) | HOTT checklist (wording TBC). |
| **CURA-CA-005** | Re-arrest: restart timers, clear ROSC outcomes, **keep** prior time logs. | Implemented (React) | Re-arrest button clears ROSC / post-ROSC only. |
| **CURA-CA-006** | Termination of resus: time; senior name/role; justification; witness name/role; family + police notified + time logs. | Implemented (React) | ROLE card: senior role, justification, notify times, witness. |
| **CURA-TRAUMA-001** | Trauma: RTC, falls from height, stabbings — **STAB5** (chest) per protocol. | Implemented | Incident Log — STAB-5 aide-mémoire quick adds + chest examination phrases + free text. |
| **CURA-HYPO-001** | Hypoglycaemia: quick **IV glucose**. | Implemented | Crew interventions. |
| **CURA-STROKE-001** | BE-FAST: add Balance + Eyes; **UNABLE** on all elements. | Implemented | Stroke tab. |
| **CURA-STROKE-002** | Add **DANISH** (definition TBC with client). | Implemented | DANISH / severity notes + trust-specific guidance + quick-adds (CPSS, PASS, SSS, LKW). |
| **CURA-DRUG-001** | Drugs tab: remove billing/cost/consent billing UI; **drug administered with consent**. | Implemented | Consent copy; cost summary only if billing on. |
| **CURA-DRUG-002** | Drug **route** = controlled dropdown, not free text. | Implemented | Route select + Other. |
| **CURA-FS-001** | Family/social — **Pests** env quick add. | Implemented | Environmental factors. |
| **CURA-FS-002** | Advanced directives: **TEP / Care plan**. | Implemented | Directive type dropdown. |
| **CURA-FS-003** | Not for resus: DNAR camera + **block close** without DNAR photo (governance). | Implemented | Cura Settings + **Sparrow Cura settings** (`clinical/cura-ops/settings`) tenant toggle; `/api/cura/config/app-settings` sync; blocks close if a **DNAR** directive row has no image. |
| **CURA-CONSENT-001** | Written consent: **signature** capture. | Implemented | Patient Info — canvas signature when Written + consent Yes; stored as PNG data URL on record. |
| **CURA-PINFO-001** | Patient Info: home address quick actions + GP lookup incorrectly greyed. | Implemented | Geolocation + snackbars for pending integrations. |

---

## 2. OBS propagation matrix (CURA-OBS-001)

**Rule (client):** Primary Survey OBS is the source of truth; downstream sections **read** it. **Breathing** is explicitly **excluded** from auto-fill (crew re-assess respirations there).

| Consumer section / screen | Should receive Primary Survey OBS? | Notes |
|---------------------------|-------------------------------------|-------|
| Primary Survey (source) | — | Authoritative entry (OBS save + `globalObservations`). |
| B Breathing and Chest | **No** (per client) | `ObservationsTab.submitObs` does **not** patch Breathing; `obsPropagation.ts`. |
| Observations / OBS tab | Yes | `globalObservations` + `caseTimerLaps` (timer). |
| Circulation | Yes (read-only hint) | Circulation tab shows latest OBS HR/BP; rows still entered on C Circulation. |
| Handover / PDF | **Sparrow case-access** | `medical_records_module` `public/case_access_pdf.html` renders shared case JSON: **OOHCA** (arrest, presenting, reversibles + times, HOTT, arrest drugs, rhythm log, pronounce, notify, ROSC time), **Stroke** (BE-FAST, AVVV, DANISH notes, exemptions), **E Exposure** (`markers` as % on body/spine diagrams + legacy `exposureRecords`), **Patient Information** (capacity / consent with signature / refusal), **OBS** (`globalObservations`, `caseTimerLaps`). |
| Drugs (time-linked vitals) | Yes (via timer) | Case timer lap with **drug tag** appends to `OOHCA.arrestDrugs` with timestamp; full Drugs tab ↔ OBS row linkage not required for MVP. |

---

## 3. ZOLL Lifepak / CorPlus (CURA-ZOLL-001) — feasibility note

**Goal:** Bluetooth (or vendor SDK) ingest of ECG strip metadata and/or OBS into Circulation / OBS.

**Status:** **Out of MVP** for this PRD — no client implementation; retain for future spike.

**Phasing**

1. **Phase 0 — Spike (P3):** Confirm device model (CorPlus vs other), OS (Android/iOS/Web PWA), whether ZOLL exposes **BLE GATT services** or requires **proprietary app/SDK**. Document data categories (ECG waveform vs summary, PHI).
2. **Phase 1 — Design:** Pairing UX, explicit user consent (“Import monitor data”), retention, audit log, offline queue.
3. **Phase 2 — MVP:** Single vital set or PDF/image attach if BLE not viable.
4. **Phase 3 — Full:** Live trace or structured vitals into chart fields.

**Privacy / safety:** Treat as **special category health data**; encryption in transit/at rest; no cloud relay without DPA; consider **air-gapped** import (share sheet / file) if BLE is blocked on managed devices.

**Outcome of spike:** Short addendum linked here (feasible / not / workaround).

---

## 4. Cardiac arrest epic matrix (CURA-CA-001 … 006)

| Area | Requirement | Done | Test |
|------|-------------|------|------|
| Data binding | CA page text + drug tab + sign-off pipeline | Yes — OOHCA + timer → drugs; Case Review sign-off field + prompt | OOHCA + Case Review + Drugs |
| Timers | Arrest clock, rhythm check, meds | Yes — Case timer → OBS laps; OOHCA shock/rhythm/drug tables | OBS `caseTimerLaps`, OOHCA |
| Reversibles | Wording + timestamp → narrative | Yes | Narrative field on OOHCA |
| Trauma | HOTT block | Yes (wording TBC) | HOTT checklist |
| Re-arrest | Reset behaviour vs preserved logs | Yes | Re-arrest button |
| Termination | All fields + notifications + times | Yes | ROLE termination fields |

---

## 5. Changelog (doc)

| Date | Change |
|------|--------|
| 2026-03-31 | Initial PRD from client routing index; LABEL-001 + CHEST-001 implemented in Cura. |
| 2026-03-31 | OBS-001, TIMER-001, CA-003–006 (partial CA-001/002), trauma STAB5 notes, FS-002, Circulation OBS hint, PRD status refresh. |
| 2026-03-31 | BURN-001 (TBSA hint + presets), CONSENT-001 (signature pad), FS-003 (DNAR photo gate + Settings); Sparrow default `app-settings` includes `dnarPhotoGateEnabled`. |
| 2026-03-31 | **PRD MVP closure:** CA-001/002 → Implemented (Review tab sign-off + OOHCA prompt); TRAUMA-001 STAB-5 aide-mémoire; STROKE-002 DANISH UX; matrices updated; ZOLL out of MVP; VOICE blocked. |
| 2026-03-31 | Sparrow **Cura settings** (ops UI): tenant toggle for **DNAR photo gate** + `medical_handover_aliases` empty app-settings default includes `dnarPhotoGateEnabled`. |
| 2026-03-31 | **Case-access PDF / HTML** updated for **Cura React payload parity** (OOHCA, Stroke, Exposure markers, Patient Info root arrays, OBS timer laps); OBS matrix row **Handover / PDF** updated accordingly. |

---

## 6. Sparrow case-access PDF (Cura payload parity)

**Template:** `app/plugins/medical_records_module/templates/public/case_access_pdf.html`  
**Helpers:** `case_access_render.py` (`epcr_get_section`, `epcr_meaningful`, `epcr_reversible` filter for OOHCA reversible cause enums).

**Test:** Open case access / PDF for a chart authored in Cura with OOHCA, Stroke, Exposure markers, and consent signature; confirm sections render without Jinja errors and fields match the app.

---

*Child of QA routing index — update Status and Test notes as items ship.*
