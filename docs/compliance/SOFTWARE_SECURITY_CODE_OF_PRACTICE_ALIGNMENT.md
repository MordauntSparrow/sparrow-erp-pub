# Alignment: UK Software Security Code of Practice (DSIT) & NHS DSPT evidence

**Purpose:** Map the voluntary [Software Security Code of Practice](https://www.gov.uk/government/publications/software-security-code-of-practice/software-security-code-of-practice) (14 principles) to **this repository** and list work that must happen **outside** the codebase (policies, contracts, operations).

**Audience:** Senior Responsible Owner (SRO), IG/DPO, engineering. This is not legal advice.

---

## Repository artefacts added or referenced (engineering)

| Item | Location |
|------|----------|
| Coordinated disclosure / security contact template | `SECURITY.md` (root) |
| Automated dependency update proposals | `.github/dependabot.yml` (GitHub only; use GitLab dependency scanning / Renovate if hosted elsewhere) |
| Product technical controls (GDPR/DSPT themes) | `CONTROL_MAP_UK_GDPR_DSPT.md` |
| Personal data breach steps | `INCIDENT_RESPONSE.md` |
| Sling credential encryption | **Hardened:** `scheduling_module` no longer falls back to a weak default `SECRET_KEY` for Fernet derivation in production paths (see code change). |

---

## Principle-by-principle snapshot

| Theme | Principle | In-repo / product | Outside repo (you) |
|-------|-----------|---------------------|---------------------|
| **1 Secure design** | 1.1 Secure development framework | Control maps, QA docs under `docs/` | Formal SDL policy, SRO sign-off, training records |
| | 1.2 Third-party risk | `requirements.txt`, Dependabot PRs | SBOM for releases, licence review, vendor DPAs |
| | 1.3 Test before distribution | `tests/` | CI mandatory gates, signed release checklist |
| | 1.4 Secure by design / default | `create_app.py` session/CSRF/secret checks | Threat modelling workshops, pen test remediation |
| **2 Build** | 2.1 Protect build environment | N/A in git | MFA on Git host, branch protection, least-privilege CI |
| | 2.2 Log build changes | Git history | Immutable CI logs, segregated build agents |
| **3 Deploy & maintain** | 3.1 Distribute securely | Docker/Railway patterns in repo | TLS certs, WAF, customer hardening guides |
| | 3.2 Vulnerability disclosure | `SECURITY.md` | Publish **customer-facing** contact if required by NHS toolkit |
| | 3.3 Detect & manage vulns | Dependabot | `pip-audit` / container scan in CI, severity SLA |
| | 3.4 Report to relevant parties | `INCIDENT_RESPONSE.md` (breach) | Product CVE comms to customers/regulators when needed |
| | 3.5 Timely patches | Changelog / plugin versions | Advisory email or status page, contract SLAs |
| **4 Communication** | 4.1 Support / maintenance info | Plugin READMEs, compliance pack | Published support matrix, major version policy |
| | 4.2 ≥1 year EOL notice | Not in code | Contract + public lifecycle statement |
| | 4.3 Notable incidents to customers | Not automated | Playbook for service-wide security incidents |

---

## Outside repository control (checklist for you — NHS DSPT / ICO)

Complete these as **organisation** artefacts; link or upload to DSPT where prompted.

1. **Named roles:** DPO/Data Protection Lead, Caldicott Guardian (if health), **SRO** for Software Security Code of Practice.
2. **ICO registration:** Keep registration details current; link privacy notice to processing.
3. **NHS DSPT:** Complete toolkit statements; attach evidence (policies, screenshots, last pen test summary, training logs).
4. **Contracts:** Data processing agreements with sub-processors (`SUBPROCESSORS_REGISTER.md` as starter list — execute real DPAs).
5. **Hosting:** Railway/other — MFA, backup restore **test**, incident access to logs.
6. **Clinical / safety:** DCB0129/0160 where applicable — separate from this Code.
7. **Customer-facing security.txt** (optional): If you operate a **public** marketing or status domain, host [security.txt](https://securitytxt.org/) there pointing to the same reporting channel as `SECURITY.md`.
8. **Certification / assurance:** NCSC self-assessment template for the Code of Practice when you pursue formal evidence.

---

## Review cadence

- **Quarterly:** Review Dependabot PRs, critical CVEs, and rotate secrets where policy requires.
- **Annually:** Refresh this alignment doc and DSPT evidence pack version numbers.

_Last updated: engineering pass for private-repo governance._
