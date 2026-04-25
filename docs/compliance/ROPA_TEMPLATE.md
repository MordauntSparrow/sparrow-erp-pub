# Record of processing activities (ROPA) — template

**Controller name:**  
**Last updated:**  
**Version:**

Duplicate one table block per processing activity (Article 30 UK GDPR style — adapt with legal advice).

---

### Activity name

| Field | Detail |
|--------|--------|
| **Purpose** | e.g. staff access to ERP, patient care coordination, payroll |
| **Categories of individuals** | e.g. employees, patients, suppliers |
| **Categories of personal data** | e.g. contact, identifiers, health data, location |
| **Special category?** | Yes / No — if Yes, document **Article 9** condition |
| **Lawful basis (Article 6)** | e.g. contract, legal obligation, legitimate interests (+ LI assessment ref) |
| **Recipients** | Internal roles / external orgs |
| **Third countries** | None / list + transfer mechanism (SCCs, IDTA, etc.) |
| **Retention** | See `DATA_RETENTION_POLICY.md` — specific period per field if possible |
| **Security measures** | e.g. RBAC, encryption in transit, audit logging, backups |
| **Data source** | Data subject directly / another controller |

---

_Add rows for: authentication logs, audit logs, email (SMTP), error monitoring (Sentry), hosting backups, plugin-specific databases (medical/dispatch), etc._
