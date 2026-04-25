# Subprocessors / third-party services — template

List every external service that processes personal data on your behalf. Update when you change hosting or tools.

| Organisation | Service | Data categories | Location / transfer | Contract / DPA / SCCs | Notes |
|--------------|---------|-----------------|---------------------|-------------------------|-------|
| *e.g. Railway* | Hosting | All application data | *region* | *link to terms/DPA* | |
| *e.g. GitLab.com* | Source / update artefacts | Code, manifests (may reference env) | US / EU | *SCCs if applicable* | Updates pull from configured project |
| *e.g. SMTP provider* | Email | Email addresses, message content | | | Password reset |
| *e.g. Sentry* | Error monitoring | IPs, user context if sent | | | Disable PII scrubbing review |
| *e.g. Redis Cloud* | Session / rate limit | Session identifiers | | | |
| *Application server filesystem* | Cura attachment storage (`medical_records_module/data/cura_uploads/`) | Uploaded clinical/safeguarding files | Same region as app host | Covered by hosting DPA / org controls | Prefer encrypted volumes; optional move to object storage later |

**Action:** Obtain DPAs or standard contractual clauses where required and file with contracts register.
