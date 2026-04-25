# Security incident and personal data breach response

## 1. Detection and triage

- Identify: unauthorised access, ransomware, mis-sent email, lost device, misconfiguration exposing data.
- Preserve: logs (`app/logs/audit.log`, host logs, DB audit if enabled), do not destroy evidence.
- Classify: confidentiality / integrity / availability impact; approximate number of data subjects and categories.

## 2. Containment

- Revoke credentials, rotate secrets (`SECRET_KEY` / JWT invalidation via redeploy + short token life), block IPs, disable compromised accounts.
- If malware: isolate systems per runbook.

## 3. UK ICO notification (personal data breach)

- If the breach **poses a risk** to individuals: notify ICO within **72 hours** where feasible (UK GDPR Article 33).
- If **high risk** to individuals: also notify affected data subjects without undue delay (Article 34), unless exceptions apply.

**ICO:** [https://ico.org.uk/](https://ico.org.uk/) — use current reporting channels.

## 4. Documentation

- Record: timeline, cause, affected data, actions taken, lessons learned.
- Retain for accountability (DSPT / audit).

## 5. Post-incident

- Patch root cause; update risk register; staff communication if needed.

---

_This is operational guidance, not legal advice. Engage DPO/legal for borderline cases._
