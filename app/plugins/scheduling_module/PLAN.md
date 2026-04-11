# Scheduling & Shifts Module – Product & Implementation Plan

## Goal

Production-ready **workforce scheduling** comparable to Deputy, When I Work, and Planday: shifts by contractor/client/site/job type, time-off and sickness, approval workflows, templates, and a clear contractor “My day” and “My requests” view.

---

## Current State

### Data model (existing)
- **schedule_shifts** – contractor_id, client_id, site_id, job_type_id, work_date, scheduled_start/end, actual_start/end, break_mins, notes, status (draft, published, in_progress, completed, cancelled, no_show), source (manual, ventus, scheduler, work_module), external_id, runsheet_id, runsheet_assignment_id, labour_cost
- **schedule_availability** – contractor_id, day_of_week, start_time, end_time, effective_from, effective_to
- **schedule_time_off** – contractor_id, type (annual, sickness, other), start_date, end_date, reason, status (requested, approved, rejected)
- **shift_swap_requests** – shift_id, requester_contractor_id, status (open, claimed, approved, rejected, cancelled), claimer_contractor_id, etc.
- **schedule_templates**, **schedule_template_slots** – recurring patterns

### Contractor (public) features
- **My shifts** – list today’s shifts; link to Work module to record times
- **Request time off** – submit annual leave (start, end, reason)
- **Report sickness** – submit sickness (start, end, reason)
- **My requests** – list own time-off requests and status
- **My day** – today’s shifts with link to Work stop page

### Admin (existing)
- **Shifts** – list shifts (filters: contractor, client, date range); create/edit shifts; calendar or list view
- API for shifts CRUD and list

---

## Admin Features (to build / extend)

### 1. Shifts – full CRUD and calendar
- **Calendar view** – week/month view by contractor or by client/site; drag-and-drop to move shift (optional); colour by status
- **List view** – existing list; enhance filters (status, job type, site); bulk actions (publish, cancel)
- **Create shift** – contractor, client, site, job type, work_date, scheduled_start/end, break_mins, notes; optional recurrence (from template)
- **Edit shift** – change any field; status transitions (draft → published, etc.)
- **Copy / repeat** – copy shift to another day or week; repeat pattern (e.g. next 4 weeks)
- **Labour cost** – view/edit labour_cost where used for reporting

### 2. Time-off and sickness management
- **List requests** – all time-off (annual, sickness, other); filter by contractor, type, status, date range
- **Approve / reject** – with optional comment; status: requested → approved/rejected
- **Bulk approve** – e.g. approve all “annual” in a date range
- **Create on behalf** – admin creates time-off record for a contractor (e.g. recorded sickness call)
- **Calendar overlay** – show time-off on the same calendar as shifts (blocked days)
- **Sickness reporting** – highlight sickness vs annual; optional “return to work” date or note

### 3. Availability
- **Per-contractor availability** – list/edit availability windows (day of week, start/end, effective dates)
- **Conflict warnings** – when publishing a shift, warn if outside contractor’s availability (optional)
- **Availability calendar** – view who is available when (simplified)

### 4. Templates and recurrence
- **Templates** – list schedule_templates; create/edit template (name, client, site, job type, slots by day of week)
- **Apply template** – generate shifts from template for a date range and assign contractors (wizard or bulk)
- **Recurring shift** – “Repeat this shift” for N weeks (creates N draft shifts)

### 5. Shift swaps (optional)
- **List swap requests** – open, claimed, approved, rejected
- **Approve swap** – admin approves after two contractors agree (or admin assigns)
- **Cancel swap** – revert to original assignment

### 6. Reporting
- **Hours by contractor** – scheduled vs actual (actual from Work/Time Billing); date range
- **Hours by client/site** – for billing or capacity
- **Time-off summary** – days taken/approved by type per contractor
- **Export** – shifts CSV for payroll or external tools

---

## Contractor (public) enhancements

- **Calendar view** – optional week view of “My shifts” (read-only)
- **Time-off balance** – if we add allowance (e.g. days per year), show “X days remaining” (optional, may live in HR or config)
- **Cancel request** – contractor can cancel own pending time-off request
- **Shift swap** – contractor can offer a shift for swap and claim another’s (if enabled); see status (open, claimed, approved)
- **Notifications** – message/todo when request approved/rejected or when shift is published/changed (integrate with Employee Portal)

---

## Data model changes (migrations)

| Change | Purpose |
|--------|--------|
| **schedule_time_off**: add reviewed_at, reviewed_by_user_id, admin_notes | Audit and notes on approval |
| **schedule_shifts**: ensure labour_cost, notes, status transitions are used consistently | Reporting and UX |
| **schedule_allowances** (optional) | contractor_id, year, allowance_days (annual), used_days; for “days remaining” |
| **schedule_notifications** (optional) | event type, contractor_id, shift_id or time_off_id, read_at; or use ep_messages |

---

## Implementation order (admin first)

1. **Admin: Time-off list** – list all time-off requests; filters; approve/reject with notes
2. **Admin: Create time-off on behalf** – form to add annual/sickness for a contractor
3. **Admin: Shifts calendar** – week view (and month); click to view/edit shift
4. **Admin: Shifts list** – enhance filters and bulk publish/cancel
5. **Admin: Shift create/edit** – full form; copy; optional “repeat next N weeks”
6. **Admin: Templates** – list, create, edit; “Apply template” to generate shifts
7. **Admin: Availability** – list per contractor; edit windows; optional conflict check
8. **Admin: Reporting** – hours by contractor, time-off summary, export
9. **Contractor** – calendar view, cancel request, optional swap and notifications

---

## Success criteria

- Admin can manage full shift lifecycle (create, edit, publish, cancel) and see a calendar
- Admin can approve/reject time-off and record sickness; see time-off on calendar
- Templates and recurrence reduce data entry for regular patterns
- Contractors see their day clearly and can request time off and report sickness with clear status
