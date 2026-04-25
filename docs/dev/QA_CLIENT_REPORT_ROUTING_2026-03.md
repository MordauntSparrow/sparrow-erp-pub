# QA overseer: client report routing & PRD allocation

**Source:** Field feedback (Phill → Jordan), clinical + ops + platform.  
**Purpose:** Decompose into **owned work packages** for **Cura**, **Medical Records (Sparrow ePCR server/admin)**, **MDT**, **Ventus CAD**, and **Inventory**, with traceable IDs for PRDs and sprints.

**Repos / surfaces (typical):**

| Stream | Primary codebase / surface |
|--------|------------------------------|
| **CURA** | `Cura` app (chart UX, clinical UI) + shared ePCR payloads as applicable |
| **Medical Records** | Sparrow `medical_records_module` — EPCR lifecycle, APIs, **admin EPCR tools** |
| **MDT** | `Ventus MDT` / `vue-connector` + dispatch integration |
| **Ventus CAD** | CAD / dispatcher UI and APIs (Sparrow `ventus_response_module` where wired) |
| **Inventory** | Sparrow `inventory_control` — kit lists, stock, expiry, alerts |

---

## 1. Executive summary

| Theme | Count (approx.) | Primary owner |
|-------|-----------------|---------------|
| ePCR clinical UX (diagrams, quick adds, arrest, stroke, drugs, consent) | ~35 bullets | **CURA** |
| MDT ↔ CAD consistency + MOTD | 2 + major incident overlap | **MDT** + **CAD** |
| Major incident (TST / MITT / METHANE) | 1 cross-cutting | **CAD** + **MDT** + optional **Sparrow** alerting |
| EPCR admin (force close incomplete) | 1 | **Medical Records** (`medical_records_module`) |
| Kit bag / expiry / notifications | 1 | **Inventory** (`inventory_control`) |

**Dependencies:** CAD “at scene” vs MDT display requires **contract** between MDT state, CAD incident resource state, and server truth. Clinical chart UX is **Cura**; **Medical Records** owns Sparrow-side EPCR rules, admin overrides, and module APIs.

**Evidence:** Client offered **video** for voice-to-text — attach to **CURA-VOICE-001** when received.

---

## 2. Master routing matrix

| ID | Summary | Owner | Type | PRD / note |
|----|---------|-------|------|------------|
| **CAD-SCENE-001** | Other callsigns show “at scene” on MDT but not on CAD; incorrect when not at scene | **Ventus CAD** (+ **MDT** verification) | Bug / integration | PRD: CAD–MDT resource location sync |
| **MDT-MOTD-001** | Message of the day: pop-up on login + when MOTD updates | **MDT** | Feature | PRD: MDT MOTD client UX |
| **CURA-LABEL-001** | Rename “Quick-trach2” → “Surgical Airway” or “FONA (Front of Neck Access)” | **CURA** | Copy / clinical label | Small PRD or ticket |
| **CURA-GI-001** | System Reviews Gastro: abdo diagram + quick-add buttons (parity with chest) | **CURA** | Feature | PRD: GI system review diagram |
| **CURA-BREATH-001** | Chest decompression quick buttons: L/R; 2nd ICS MCL and/or 5th ICS mid-axilla; finger thoracostomy L/R | **CURA** | Feature | PRD: Breathing interventions quick-add |
| **CURA-CHEST-001** | Chest quick adds: add **No Air Entry** | **CURA** | Feature | Ticket |
| **CURA-ZOLL-001** | Circulation: Bluetooth upload ECG/OBS from ZOLL Lifepak (CorPlus) — *no rush* | **CURA** | Spike / future | PRD: Device integration (phase 2+) |
| **CURA-EXP-001** | Exposure wound care: glue, staples; wound type **skin tear** | **CURA** | Feature | Ticket |
| **CURA-EXP-002** | Fracture assessment: quick **Open / Closed / Other** (+ free text) | **CURA** | Feature | Ticket |
| **CURA-BURN-001** | Burns: shade region → auto **% TBSA** | **CURA** | Feature | PRD: Burns diagram calculator |
| **CURA-BURN-002** | Burns depth: **Superficial** beside Partial/Full | **CURA** | Feature | Ticket |
| **CURA-OBS-001** | OBS from **Primary Survey** not carried into flow; duplicate entry; OBS should populate everywhere **except** Breathing as stated | **CURA** | Bug | PRD: OBS propagation rules |
| **CURA-VOICE-001** | Review voice-to-text: repeating words, missing words | **CURA** | Bug | PRD: Voice capture QA + vendor/model review; **video evidence** |
| **CURA-TIMER-001** | Case timer not populating text boxes or drug section | **CURA** | Bug | PRD: Case timer binding |
| **CURA-CA-001** | Cardiac arrest: text boxes / drug tab not populating for sign-off | **CURA** | Bug | PRD: CA page data binding |
| **CURA-CA-002** | Cardiac arrest: timers — arrest time, rhythm check, meds | **CURA** | Feature | PRD: CA timers |
| **CURA-CA-003** | Reversible causes: statement “tick yes when considered or reversed” + **time log** into text | **CURA** | Feature | PRD: CA reversible causes |
| **CURA-CA-004** | HOTT principle for **trauma arrest** | **CURA** | Content / UX | Clinical sign-off on wording |
| **CURA-CA-005** | **Re-arrest** button: restart timers, clear ROSC outcomes, **retain** time logs | **CURA** | Feature | PRD: CA re-arrest |
| **CURA-CA-006** | Termination of resus: time; senior clinician name/role; justification (free text); witness name/role; family + police notified **with time log** | **CURA** | Feature | PRD: CA termination block |
| **CURA-TRAUMA-001** | Trauma assessment modules: RTC, falls from height, stabbings — chest **STAB5** | **CURA** | Feature | PRD: Trauma assessment templates |
| **CURA-HYPO-001** | Hypoglycaemia: quick **IV glucose** | **CURA** | Feature | Ticket |
| **CURA-STROKE-001** | Stroke: **BE-FAST** (add Balance, Eyes); **UNABLE** on all BE-FAST items | **CURA** | Feature | PRD: BE-FAST v2 |
| **CURA-STROKE-002** | Add **DANISH** stroke assessment | **CURA** | Feature | PRD: DANISH tool (clinical definition) |
| **CURA-DRUG-001** | Drugs tab: remove billing/cost/consent; replace with **drug administered with consent** | **CURA** | UX / policy | PRD: Drugs tab non-billing |
| **CURA-DRUG-002** | Drug **route** as dropdown not free text | **CURA** | Feature | Ticket |
| **CURA-FS-001** | Family/social environment: **Pests** quick add | **CURA** | Feature | Ticket |
| **CURA-FS-002** | Advanced directives: **TEP / Care plan** option | **CURA** | Feature | Ticket |
| **CURA-FS-003** | Not for resus: **require DNAR photo** (camera); **block EPCR close** without photo | **CURA** | Feature + rule | PRD: DNAR photo gate |
| **CURA-CONSENT-001** | Written consent: **signature** capture | **CURA** | Feature | PRD: Consent signature |
| **CURA-PINFO-001** | Patient Info: home address quick actions + GP lookup **incorrectly greyed** | **CURA** | Bug | PRD: Patient info actions |
| **SPR-EPCR-001** | Admin: **force close** EPCR when incomplete; **scoped admin privilege** | **Sparrow** | Feature + security | PRD: EPCR admin override |
| **SPR-INV-001** | Inventory: **kit bag kit list** with **expiry** + **notifications** | **Sparrow** | Feature | PRD: Kit bag expiry alerts |
| **CAD-MI-001** | Major incident: **TST**, **MITT**, **METHANE** (CAD + dispatcher workflow) | **Ventus CAD** | Feature | PRD: Major incident CAD |
| **MDT-MI-001** | METHANE (etc.) as **MDT hot buttons** → send to CAD + **urgent dispatcher alert** + start TST | **MDT** (+ **CAD** API) | Feature | PRD: MDT major incident handoff |

---

## 3. PRD bundles by agent (copy into agent-specific PRDs)

Use these as **epic headers** when splitting into repo-specific `PRD_*.md` files.

### 3.1 Cura / Medical Records Agent

**Epic A — Airway & breathing (CURA-LABEL-001, CURA-BREATH-001, CURA-CHEST-001)**  
Acceptance: labels approved by clinical lead; decompression options match UK pre-hospital practice; “No Air Entry” appears in chest quick adds and persists to narrative/export.

**Epic B — System reviews & exposure (CURA-GI-001, CURA-EXP-001, CURA-EXP-002, CURA-BURN-001, CURA-BURN-002)**  
Acceptance: abdo diagram UX parity with chest pattern Phill referenced; TBSA calculation validated against clinical calculator rules (document formula); fracture and wound enums in handover/PDF.

**Epic C — OBS & timers & voice (CURA-OBS-001, CURA-TIMER-001, CURA-VOICE-001)**  
Acceptance: matrix of which sections consume Primary Survey OBS; Breathing exclusion explicit in tests; case timer drives at least the fields client listed; voice issue reproduced with video then fixed or mitigated.

**Epic D — Cardiac arrest (CURA-CA-001 … CURA-CA-006)**  
Acceptance: single PRD for **CA page** with timer spec, reversible causes + timestamps, HOTT (trauma), re-arrest behaviour, termination block fields; drug tab sign-off flow end-to-end.

**Epic E — Stroke & hypoglycaemia (CURA-STROKE-001, CURA-STROKE-002, CURA-HYPO-001)**  
Acceptance: BE-FAST + UNABLE documented; DANISH fields/scoring per agreed clinical protocol.

**Epic F — Drugs & consent & family (CURA-DRUG-001, CURA-DRUG-002, CURA-FS-001–003, CURA-CONSENT-001)**  
Acceptance: no billing strings in patient-facing drugs UI; route controlled vocabulary; DNAR photo gate enforced on **complete/close** (define legal/clinical sign-off).

**Epic G — Patient info & devices (CURA-PINFO-001, CURA-ZOLL-001)**  
Acceptance: PINFO bug fixed with regression test; ZOLL as phased spike with privacy/HIPAA-style data handling note.

---

### 3.2 MDT Agent

**Epic MDT-1 — Scene truth (ties to CAD-SCENE-001)**  
Acceptance: given same incident + resource, MDT “at scene” state matches CAD within defined latency; no “at scene” when GPS/state says otherwise (define rule).

**Epic MDT-2 — MOTD (MDT-MOTD-001)**  
Acceptance: show on login; poll or push on MOTD version change; dismissible; does not block critical dispatch actions (or configurable).

**Epic MDT-3 — Major incident hot actions (MDT-MI-001)**  
Acceptance: METHANE (and agreed TST/MITT triggers) structured payload to CAD; dispatcher alert channel; audit trail.

---

### 3.3 Ventus CAD Agent

**Epic CAD-1 — Resource / scene sync (CAD-SCENE-001)**  
Acceptance: CAD UI reflects MDT-marked scene status per API contract; reconciliation logs for mismatch.

**Epic CAD-2 — Major incident (CAD-MI-001)**  
Acceptance: TST/MITT/METHANE models, dispatcher workflows, and linkage to incident record; integration tests with MDT handoff.

---

### 3.4 Medical Records Agent (Sparrow — EPCR admin / server)

**Epic MR-1 — EPCR admin override (MR-EPCR-001)**  
Implement in **`medical_records_module`** (not generic core admin only): permission e.g. `epcr_force_close` or equivalent in existing permission registry; **reason** mandatory; full **audit** (who/when/why); optional notify clinical governance; deny by default for staff without the privilege. Align routes/templates with existing medical records admin patterns.

---

### 3.5 Inventory Agent (Sparrow — `inventory_control`)

**Epic INV-1 — Kit bag expiry (INV-KIT-001)**  
**Kit bag** entity has **kit list** (line items); each line supports **expiry date** (and batch/lot if product agrees); **notifications** for upcoming/expired items (in-app and/or email per existing notification patterns); dashboard or report surface for compliance.

---

## 4. Sequencing & risk (QA view)

| Priority | Rationale |
|----------|-----------|
| **P0** | CAD-SCENE-001 (safety/command clarity), CURA-CA-001/002 (arrest documentation), CURA-OBS-001 (duplicate work / error risk) |
| **P1** | CURA-CA-003–006, CURA-PINFO-001, MDT-MOTD-001, CURA-DRUG-001, MR-EPCR-001 |
| **P2** | Diagrams (GI, burns), BE-FAST/DANISH, trauma/STAB5, DNAR photo gate, INV-KIT-001 |
| **P3** | ZOLL Bluetooth, major incident full stack (coordinate CAD+MDT) |

**Cross-team critical path:** **CAD-MI-001** + **MDT-MI-001** should be one **joint design session** (payload, IDs, alert semantics).

---

## 5. Gaps & questions back to client

1. **DANISH** — confirm definition/source (local protocol vs national tool).  
2. **STAB5** — confirm exact pathway content and whether it is chest-only or multi-region.  
3. **DNAR photo** — legal/clinical approval for **mandatory** photo before close.  
4. **CAD-SCENE-001** — one example incident ID + timestamp + callsigns for reproduction.  
5. **Voice-to-text** — platform (iOS/Android/Web) and await **video**.

---

## 6. Suggested child PRD filenames (optional splits)

| File | Owner |
|------|--------|
| `docs/dev/PRD_CURA_EPCR_CLIENT_ROUND_2026-03.md` | Cura agent (Epics A–G) |
| `docs/dev/PRD_MEDICAL_RECORDS_EPCR_ADMIN_2026-03.md` | Medical Records agent — **MR-EPCR-001** (Sparrow module) |
| `docs/dev/PRD_INVENTORY_KIT_BAG_2026-03.md` | Inventory agent — **INV-KIT-001** |
| `docs/dev/PRD_MDT_CLIENT_ROUND_2026-03.md` | MDT agent |
| `docs/dev/PRD_VENTUS_CAD_CLIENT_ROUND_2026-03.md` | CAD agent |

This document is the **index**; child PRDs can lift IDs and acceptance text verbatim.

---

*QA routing doc — maintain as items move from backlog → in progress → done.*
