# Data retention — policy template

Define periods **per data category** and **who enforces deletion**. Legal/statutory periods override this template.

| Data category | Example location | Suggested default | Deletion method | Owner |
|---------------|------------------|-----------------|-----------------|-------|
| Authentication audit | `app/logs/audit.log` | *e.g. 12–24 months* | Rotate / archive / secure delete | |
| Application DB — user directory | `users` table | Life of contract + *X* years | Anonymise or delete row | |
| Medical / dispatch records | Plugin tables | Per clinical / operational law | Plugin-specific purge jobs | |
| Backups | Object storage / ZIP | Match production + *X* | Encrypted; destroy after period | |
| Error traces | Sentry | Per vendor settings | Scrub PII; shorten retention | |
| Session data | Redis | Session TTL | Automatic expiry | |

**Review:** At least annually or when processing purposes change.

**Secure deletion:** For SSD/cloud objects, use provider tools; overwriting files on shared hosting may be insufficient — design around encryption and key rotation.
