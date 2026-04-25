# PRD: DBS Update Service — status monitoring (no application submission)

**Status:** Draft for execution  
**Owner:** Product + DPO + Engineering  
**Scope:** HR / compliance — read-only status checks against existing Standard/Enhanced certificates on the Update Service.

## 1. Problem

Operations need assurance that employees subscribed to the **DBS Update Service** still have a **current** certificate (no new information). Today Sparrow stores manual DBS fields only; it does **not** call DBS.

## 2. Goals

- **Scheduled** status checks (e.g. daily or weekly) for opted-in employees with valid stored data and consent.
- **On-demand** (“Check now”) from HR admin.
- Record **history** and **alert** HR when status moves to material states (especially `NEW_INFO`).
- **Do not** submit new DBS applications via this workstream (separate process).

## 3. Non-goals

- Full disclosure PDF retrieval (API returns status + limited XML fields only).
- Automatic blocking of MDT sign-on / Ventus dispatch without explicit product approval.
- Replacing Umbrella Body / legal review.

## 4. Official integration reference

- DBS **Multiple Status Check** HTTP/XML API (organisation builds front-end):  
  [Multiple Status Checking Guide V2.0 (PDF)](https://assets.publishing.service.gov.uk/media/67449590ece939d55ce93006/Multiple_Status_Checking_Guide_V2.0_23112024.pdf)
- Endpoint shape (per guide):  
  `GET https://secure.crbonline.gov.uk/crsc/api/status/<disclosureRef>?dateOfBirth=DD/MM/YYYY&surname=...&hasAgreedTermsAndConditions=true&organisationName=...&employeeSurname=...&employeeForename=...`
- Outcomes: `BLANK_NO_NEW_INFO`, `NON_BLANK_NO_NEW_INFO`, `NEW_INFO` (XML).

Confirm production access, rate limits, and any extra auth with DBS before go-live.

## 5. Data model (proposed)

| Piece | Purpose |
|--------|---------|
| `hr_staff_details` or child table | Flags: `dbs_update_service_subscribed`, `dbs_certificate_ref`, consent timestamp, last check at, last status code, last raw payload or hash |
| `dbs_status_check_log` | `contractor_id`, `checked_at`, `status_code`, `result_type`, `checker_id` / channel (`scheduled` / `manual`), error message |

Store **minimum** criminal-data footprint; align retention with DPIA.

## 6. Configuration

- **Organisation name:** from core manifest `site_settings.company_name` (existing pattern).
- **Manual checks — checker identity:** logged-in HR user `first_name` / `last_name` (from `users` table when present).
- **Scheduled checks — checker identity:** dedicated HR setting *or* nominated system user (no fake `current_user` in cron).
- **Schedule:** cron / APScheduler — configurable interval (daily / weekly).

## 7. Functional requirements

1. Admin enables feature and documents consent policy.
2. Per employee: toggle “On Update Service”, capture certificate number + DOB/surname alignment with certificate (reuse HR fields where possible).
3. **Manual:** button runs one check; show result; write log; notify if changed to `NEW_INFO`.
4. **Scheduled:** job iterates opted-in rows; same call + diff + notify.
5. **Idempotency / errors:** retries, admin-visible failures, no silent drops.
6. **Audit:** who ran check, when; avoid logging full URLs with PII in web server logs.

## 8. Security / compliance

- DPIA; lawful basis; retention; RBAC on viewing status/history.
- TLS; secrets in config not repo.
- Frequency aligned with organisational policy and DBS guidance.

## 9. Milestones

1. Legal + DBS access confirmation  
2. Schema + HR UI (flags, history panel)  
3. HTTP client + XML parser + sandbox tests  
4. Scheduler + notifications  
5. Hardening + runbook  

## 10. Acceptance criteria

- For a test certificate in sandbox, manual check returns XML matching guide; status stored.
- Transition to `NEW_INFO` raises visible HR alert.
- No new DBS **application** traffic in this path.
