# Work Module – Product & Implementation Plan

**Visits (shipped):** `work_visits` table (1:1 with `schedule_shifts`), `work_photos.visit_id`, admin **Client visits** screen, visit ref on contractor stop UI, CSV/report columns. Upgrade: `python -m app.plugins.work_module.install upgrade`.

## Goal

Production-ready **daily work and time capture** comparable to Deputy, TSheets, and workforce apps: contractors see “My day” (from Scheduling), record clock-in/out and notes per stop, attach photos, with data syncing to Time & Billing and visible to admin for oversight and payroll.

---

## Current State

### Data model (existing)
- **work_photos** – shift_id, contractor_id, file_path, file_name, mime_type, caption, created_at
- **schedule_shifts** (scheduling) – actual_start, actual_end, notes updated by Work module when contractor records a stop
- Time Billing integration: Work creates or updates timesheet entries from recorded actual_start/actual_end

### Contractor (public) features
- **My day** – list today’s shifts (from Scheduling); link to “Record times & notes” per stop
- **Stop page** – per shift: form for actual_start, actual_end, notes; upload photo(s) with caption; submit updates shift and syncs to Time Billing
- Photos listed on stop page

### Admin
- Landing page only (link to Scheduling for shift management)

---

## Admin Features (to build)

### 1. Times and attendance overview
- **List recorded stops** – filter by contractor, client, site, date range; show shift (client, site, job type, date), scheduled vs actual times, notes, photo count
- **Drill down** – click contractor or date to see their day; click shift to see stop detail (times, notes, photos)
- **Gaps** – list shifts that are published but have no actual_start/actual_end (missing clock-in/out); optional “Remind” to send portal message
- **Late / early** – highlight shifts where actual is outside scheduled window (configurable tolerance)

### 2. Photos and evidence
- **Photo gallery** – list photos by date, contractor, or shift; lightbox view; optional download
- **Caption and audit** – view caption, upload time, contractor; optional “flag” for review (e.g. quality or compliance)
- **Delete photo** – admin can remove inappropriate or duplicate photo (with audit note if required)

### 3. Edit on behalf (override)
- **Edit recorded times** – admin can set actual_start, actual_end, notes for a shift (e.g. correction after contractor forgot to clock)
- **Sync to Time Billing** – ensure one source of truth; edit here triggers re-sync to timesheet entry where applicable
- **Audit** – log admin overrides (user_id, shift_id, before/after, timestamp)

### 4. Reporting
- **Hours worked** – by contractor, by client, by date range (from actual_start/actual_end)
- **Photo count** – how many photos per shift/contractor (for evidence requirements)
- **Export** – CSV of stops (contractor, date, client, site, scheduled, actual, notes) for payroll or audit

### 5. Settings and rules (optional)
- **Require photo** – per job type or client: contractor must upload at least one photo to complete stop
- **Cut-off** – no edits after N hours (e.g. 24h) unless admin overrides
- **Notifications** – remind contractor to record times if shift ended and no actual_end (via Employee Portal)

---

## Contractor (public) enhancements

- **Quick clock** – single-tap “Start” / “End” that sets actual_start or actual_end to now (optional)
- **Photo required** – if enabled, block submit until at least one photo uploaded; clear message
- **Offline / draft** – optional: save draft locally and submit when online (complex; lower priority)
- **History** – “My recent stops” (past 7 days) with read-only summary (optional)

---

## Data model changes (migrations)

| Change | Purpose |
|--------|--------|
| **work_photos**: add flagged TINYINT(0), flagged_at, flagged_by_user_id, flag_reason | Admin flag for review |
| **work_override_log** (optional) | shift_id, user_id, field (actual_start, actual_end, notes), old_value, new_value, at | Audit admin edits |
| **schedule_shifts**: already has actual_start, actual_end, notes | No change; ensure Work module is sole writer for contractor-initiated updates; admin writes via override log |

---

## Implementation order (admin first)

1. **Admin: Recorded stops list** – filter by contractor, date range; show shift + actual times + notes + photo count; link to detail
2. **Admin: Stop detail** – view shift, actual times, notes, list of photos (with lightbox); “Edit times” (admin override) with audit
3. **Admin: Gaps report** – shifts with no actual; optional remind
4. **Admin: Photo gallery** – list photos with filters; view, download; optional flag/delete
5. **Reporting** – hours worked by contractor/client; export CSV
6. **Contractor** – “Quick clock” if desired; photo-required validation when enabled
7. **Optional**: cut-off rule and reminder notifications

---

## Success criteria

- Admin can see all recorded times and notes, and identify gaps (no clock-in/out)
- Admin can view and manage photos and correct times when needed (with audit)
- Reporting supports payroll and client reporting
- Contractor experience stays simple: see day, record times and notes, add photos; optional quick clock
