# Scheduling: Sling-Exceeding Roadmap (PRD)

Goal: Match and exceed Sling’s scheduling, time & attendance, labor cost, and communication features while keeping our UI brighter, more intuitive, and more capable.

---

## Epic 1: Smart Employee Scheduling

| Feature | Description | Status |
|--------|-------------|--------|
| **Drag-and-drop** | Move shifts across week view (day + staff). | Done |
| **Shift templates** | Save/reuse weekly patterns; apply to a week; clone; save week as template; assign by contractor/slot. | Done (clone, save week as template, apply with per-slot contractor). |
| **Conflict prevention** | Flag overlapping shifts, double-bookings, max hours/day or week. | In progress |
| **Shift swapping** | Staff request trades; manager approval or self-claim with rules. | Tables exist; full flow pending. |
| **Open shifts** | “Open shift” not assigned; eligible staff get notification; first to claim gets it. | Pending |

---

## Epic 2: Time & Attendance (Geofencing)

| Feature | Description | Status |
|--------|-------------|--------|
| **Mobile time clock** | Clock in/out from phone/laptop linked to scheduled shift. | Done (API + My day Clock in/out). |
| **GPS geofencing** | Restrict clock-in to site/location (e.g. within 50m). | Done (clock locations per site, validate on clock-in). |
| **Timesheet exports** | CSV/PDF for payroll; integrate with time_billing. | Done (Export timesheets CSV from scheduling). |

---

## Epic 3: Labor Cost Management

| Feature | Description | Status |
|--------|-------------|--------|
| **Overtime alerts** | Warn when a shift pushes someone into overtime. | Done (weekly threshold, flag in week view). |
| **Budget tracking** | Daily/weekly labor budget vs scheduled cost. | Done (weekly budget, % in week view). |
| **Per-shift cost** | Show cost per shift from wage cards (time_billing). | Done (labour_cost on shift, form + week tooltip). |

---

## Epic 4: Team Communication & Tasks

| Feature | Description | Status |
|--------|-------------|--------|
| **Newsfeed** | Company-wide or schedule-related announcements. | Done (Announcements: send to all / this week / selected via Employee Portal). |
| **Direct/group messaging** | Chat linked to shift/team/day. | Pending |
| **Task lists** | To-dos attached to shift or position; progress tracking. | Done (shift tasks: add on edit, staff mark complete via API). |

---

## Epic 5: UX & Performance

| Requirement | Target |
|-------------|--------|
| **Week view load** | &lt; 2s for 100+ employees. |
| **Geofencing accuracy** | Within ~50m where applicable. |
| **UI** | Bright, clear, color-coded by role/job type; tooltips; keyboard-friendly. |
| **Analytics** | Coverage heatmap, weekly cost/hours/overtime trend. Done (analytics page). |

---

## Epic 6: Permissions & Audit

| Feature | Description | Status |
|--------|-------------|--------|
| **Roles** | Scheduler, manager, staff self-service; per division. | Partial. |
| **Audit trail** | Who created/updated/cancelled shifts and when. | Done (schedule_shift_audit, audit log page, per-shift history). |

---

## User Story Example (Open Shift)

- **Story:** As a manager, I want to create an “Open Shift” so that qualified staff can claim it first-come, first-served.
- **Acceptance:** Manager can choose “Open Shift” (no assignee); eligible staff get notification; first to click “Claim” is assigned; optional approval flow.

---

*Last updated from Sling PRD summary and current codebase.*
