# Security policy (Sparrow ERP)

This repository is **private**. This file exists for **internal governance**, NHS Data Security and Protection Toolkit (DSPT) evidence, and the UK [Software Security Code of Practice](https://www.gov.uk/government/publications/software-security-code-of-practice/software-security-code-of-practice) (voluntary). Publish a **customer-facing** equivalent (or excerpt) via your own website or client pack if required for procurement.

## Supported versions

Security fixes are applied to the **active development branch** used for customer deployments (typically `main` or your agreed release branch). Older branches may not receive backports unless contractually agreed.

## Reporting a vulnerability

**Do not** open a public issue for security-sensitive reports.

1. **Contact:** **jordan@mordauntgroup.co.uk** (Mordaunt Group — security / vulnerability reports and coordination).  
   **UK ICO registration:** **ZC128822** (data controller notification; use for DSPT / privacy cross-reference where relevant).
2. **Repository access:** Source code is in a **private** repository; **one** nominated account with access, **MFA enforced** on the git host (evidence: export or screenshot for NHS DSPT / audits).
3. **Severity:** Include affected area (e.g. Sparrow web app, employee portal, medical records API), reproduction steps, and whether data exposure is suspected.
4. **Response:** Triage within **5 business days** where feasible; critical issues prioritised for patch and customer notification per your incident runbook (`docs/compliance/INCIDENT_RESPONSE.md`).

## Secure configuration reminders

- Set a strong **`SECRET_KEY`** in production (`FLASK_ENV=production` or `RAILWAY_ENVIRONMENT=production` enforces this at app startup).
- Use **HTTPS** at the edge; set **`SESSION_COOKIE_SECURE`** appropriately.
- Restrict database and admin access; follow `docs/compliance/EVIDENCE_CHECKLIST.md` for DSPT-style evidence.

## Out of scope

- Third-party penetration tests and signed DPAs are **organisational** artefacts (see “Outside repository control” in `docs/compliance/SOFTWARE_SECURITY_CODE_OF_PRACTICE_ALIGNMENT.md`).
- **Clinical safety (DCB0129 / DCB0160)** is separate from **cyber** security; see [`docs/compliance/DCB0129_DCB0160_GUIDE.md`](./docs/compliance/DCB0129_DCB0160_GUIDE.md) and [`docs/compliance/NHS_DTAC_NOTES.md`](./docs/compliance/NHS_DTAC_NOTES.md).
