# DCB0129 and DCB0160 — how they apply to Sparrow / Mordaunt Group

This is **operational guidance**, not legal or clinical advice. Use the official NHS England specifications and your Caldicott / medical director where patient safety is involved.

## Official sources (NHS England Digital)

- **Overview:** [Clinical risk management standards](https://digital.nhs.uk/services/clinical-safety/clinical-risk-management-standards)
- **DCB0129** (manufacturer / supplier): [Clinical risk management in the **manufacture** of health IT systems](https://digital.nhs.uk/data-and-information/information-standards/information-standards-and-data-collections-including-extractions/publications-and-notifications/standards-and-collections/dcb0129-clinical-risk-management-its-application-in-the-manufacture-of-health-it-systems) — specification, implementation guidance, compliance assessment materials.
- **DCB0160** (deploying organisation): [Clinical risk management in the **deployment and use** of health IT systems](https://digital.nhs.uk/data-and-information/information-standards/information-standards-and-data-collections-including-extractions/publications-and-notifications/standards-and-collections/dcb0160-clinical-risk-management-its-application-in-the-deployment-and-use-of-health-it-systems) — same pattern of documents for the **customer** side.

Standards are published under **section 250** of the Health and Social Care Act 2012. **Applicability** (whether a given product or deployment is in scope) is determined using the published **applicability / compliance assessment** guidance and templates on those pages — start there if a customer asks “do we need DCB0129?”

---

## Split of responsibilities (simple model)

| Standard | Who | What it covers |
|----------|-----|----------------|
| **DCB0129** | **You** (Mordaunt Group as developer / supplier of Sparrow, Cura-linked modules, etc.) | Clinical risk management over the **product**: design, build, release notes, known hazards, mitigations in software, updates, supplier hazard log / safety case **for the product**. |
| **DCB0160** | **Each customer** (NHS trust, ambulance service, independent provider using your software in care) | Safe **deployment and use**: local configuration, workflows, training, interfaces to other systems, go-live, monitoring incidents, **their** hazard log and safety case for **that** deployment. |

You **cannot** “do DCB0160” on behalf of an NHS organisation end-to-end — they must own deployment risk. You **support** them with: DCB0129 evidence, hazard transfer, release notes, known limitations, and incident escalation paths.

---

## What you typically do for **DCB0129** (supplier)

1. **Appoint a Clinical Safety Officer (CSO)** for the manufacturer side  
   NHS England expects a named, competent CSO (see implementation guidance for training and role expectations). For a very small supplier this is often a **lead + external clinical safety advisor**; competency matters more than headcount.

2. **Risk Management Plan**  
   How you will identify, assess, mitigate, and monitor **clinical** hazards across the product lifecycle.

3. **Hazard Log**  
   Living register of hazards (e.g. wrong patient context, medication display, dispatch decision support, data loss affecting care). Link hazards to controls in software, process, or documentation.

4. **Clinical Safety Case Report**  
   Argument that the system is acceptably safe **for the intended use** you claim, given residual risks and mitigations.

5. **Ongoing**  
   Update the log and safety case for **major** releases; review serious clinical incidents or near misses that relate to the product; feed into design.

Use the **DCB0129 compliance assessment template** from the NHS England page to gap-check yourself.

---

## What the **customer** does for **DCB0160** (deployer)

1. Their **CSO** (often a registered clinician with clinical safety training) owns deployment clinical safety.  
2. **Hazard workshops** for *their* use of Sparrow (rosters, ePCR, dispatch rules, data flows).  
3. **Hazard Log + Clinical Safety Case Report** for the **deployment** (not the same document as yours).  
4. They should **request your DCB0129 pack** and map transferred risks into their log.  
5. **Live** monitoring: clinical incidents, changes to configuration, decommissioning.

---

## Sparrow-specific pointers

- **Non-clinical modules only** (e.g. pure finance with no patient data): applicability may differ — use NHS applicability guidance; do not blanket-claim DCB0129 where no health IT / no influence on care.  
- **Medical records, dispatch, triage, handover, medication-related UI:** high likelihood of being **in scope** when used for patient care — treat as in scope unless applicability assessment says otherwise.  
- **Evidence in git** can include: hazard log exports (redacted), versioned safety case summaries, release notes calling out safety-relevant changes — **sensitive** raw workshop notes often stay **out** of git.

---

## ICO registration (data protection) vs clinical safety

Your **ICO registration** (e.g. ZC128822) covers **data protection** notification. **DCB0129 / DCB0160** are **clinical risk management** information standards — related themes (patient safety, confidentiality) but **different** frameworks. You still need both where applicable.

---

## Repository access (evidence snippet)

Document for DSPT / audits: **single account** with access to source code, **MFA enabled** on the git host — attach screenshot or provider attestation when the toolkit asks for access controls.

## In-product entry point

**Compliance & audit → Reports & assurance → Governance & assurance packs** (`/plugin/compliance_audit_module/governance-packs`). Hazard logs and safety cases remain outside this screen under clinical governance.
