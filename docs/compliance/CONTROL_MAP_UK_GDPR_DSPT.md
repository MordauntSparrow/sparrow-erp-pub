# Control map: product behaviour vs UK GDPR / DPA 2018 / DSPT themes

This maps **implemented technical behaviour** in this repository (as shipped) to common themes. Your **accredited** position depends on organisational measures, contracts, and completed templates in this folder.

| Theme | Product / code alignment (indicative) | Evidence you still need |
|-------|----------------------------------------|-------------------------|
| **Lawfulness & transparency** | Configurable site name/branding; privacy text is **your** responsibility | Published privacy notice, lawful basis per processing |
| **Data minimisation** | Auth audit logs avoid storing plaintext usernames on **failed** login (pseudonymous hint) | ROPA + DPIA on fields stored in plugins (e.g. medical/dispatch) |
| **Integrity & confidentiality (Art 32)** | bcrypt passwords; TLS expected at edge; `SESSION_COOKIE_SECURE` default in prod; CSRF on session routes; JWT for API; CORS tightened; ZIP safety; backup excludes `.env*` | HTTPS config proof, pen test, encryption at rest for DB/backups off-server |
| **Availability** | Your hosting SLA, backups, restore tests | Runbook, tested restore, RTO/RPO |
| **Access control** | Flask-Login + roles/permissions; admin-only routes; Ventus history compat uses dispatch scope | Access review records, joiners/leavers, RBAC matrix |
| **Audit & accountability** | `app/logs/audit.log` JSON; security events for login/logout/reset | Log review procedure, retention, tamper protection |
| **Breach notification** | Not automated | `INCIDENT_RESPONSE.md` + ICO 72h process |
| **Subprocessors** | Optional Sentry, SMTP, Redis, Railway, GitLab updates | `SUBPROCESSORS_REGISTER.md` + DPAs/SCCs |
| **DSPT** | Maps across multiple assertions | Complete DSPT toolkit with artefact uploads |

**DCB0129 / clinical safety:** If the system influences clinical or safety-critical decisions, clinical risk management is **out of scope** of this repo and must be satisfied separately.
