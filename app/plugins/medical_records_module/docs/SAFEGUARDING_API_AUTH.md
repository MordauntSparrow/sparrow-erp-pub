# Safeguarding referral API — authorisation matrix

Server-enforced policy (March 2026). **clinical_lead**, **admin**, and **superuser** are **privileged** (`_epcr_privileged_role()`). **crew** is non-privileged.

## Read scope (list + detail + payload)

| Role | May read referral payload when |
|------|--------------------------------|
| clinical_lead / admin / superuser | Always (any referral). |
| crew | **Only** when `created_by` matches the JWT/session username. Operational event, callsign, and co-assignment do **not** grant read access to another user’s referrals. |

## List filters

| Query | crew | Privileged |
|-------|------|------------|
| No `operational_event_id` | Rows with `created_by = me` only. | All rows (up to limit). |
| `operational_event_id=X` | Rows with `operational_event_id = X` **and** `created_by = me` (intersection). | All rows for that event. |

## Write scope

| Action | crew | Privileged |
|--------|------|------------|
| Create referral | Yes; **operational_event_id** still requires assignment guard when the event enforces assignments (write path only). | Yes |
| PATCH/PUT draft | Only creator; not `submitted` / `closed` / `archived`. | Yes |
| Submit / close | Creator or privileged. | Yes |
| Delete draft | Creator or privileged. | Yes |

## Endpoints

Read checks use the same **creator-or-privileged** rule everywhere (facade list, plugin list, Cura canonical list, GET by id, idempotency replay, audit log, attachments).

## Query parameters

- **`assignedTo`** was removed from `GET .../safeguarding_module/api/referrals` (no assignee column). Use **`createdBy`** where needed (typically privileged filters).

- **`search`** on that GET remains **403** for non-privileged (payload search would bypass row filters).

## Audit (`cura_safeguarding_audit_events`)

- **create**, **patch**, **submit**, **close** / **status_change** use non-PHI `detail_json`.
- Failed required audit insert → **503** and rollback where implemented.
- Referral **delete** cascades audit rows; `_audit_epcr_api` still logs the delete.

## curl checks

- Crew **A** `GET .../referrals` → only A’s rows; never B’s, even if both work the same event.
- Crew **A** `GET .../referrals/<B_referral_id>` → **403**.
- `GET .../cura/safeguarding/referrals?operational_event_id=1` as crew → only own rows for that event.
- Privileged token → full list/detail unchanged.
