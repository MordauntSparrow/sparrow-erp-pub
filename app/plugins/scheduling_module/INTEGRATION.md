# Scheduling module – integration with Employee Portal cluster

The scheduling module is designed to work with the **employee portal cluster**: `employee_portal_module`, `time_billing_module`, and optionally `work_module`. Follow these practices when changing scheduling or portal code.

## Dependencies

- **time_billing_module** (required): provides `tb_contractors`, `clients`, `sites`, `job_types`. Install (or upgrade) time_billing before scheduling. Session uses `tb_user` (contractor id) for staff-facing routes.
- **employee_portal_module** (required): portal dashboard links to scheduling; scheduling sends messages via `admin_send_message(..., source_module="scheduling_module")` so they appear in the portal.
- **website_module** (required): for public base template and settings.

## Identity and session

- **Staff (contractors)** sign in via time_billing or employee portal; session stores `tb_user` (contractor id). Scheduling public routes use `_staff_required` / `_current_contractor_id()` and expect `session.get("tb_user")`.
- **Admins** use Flask-Login `current_user`; scheduling admin routes use `_admin_required_scheduling` (or core role check). Do not rely on contractor id for admin actions.

## Sending messages to staff

Use the employee portal message API so messages appear in the portal inbox:

```python
from app.plugins.employee_portal_module.services import admin_send_message
admin_send_message(
    [contractor_id],
    "Subject",
    body="Optional body",
    source_module="scheduling_module",
    sent_by_user_id=current_user.id,
)
```

Always set `source_module="scheduling_module"` so the portal can filter and attribute messages. Scheduling uses this for: open shift claim submitted/claimed, swap resolution, announcements.

## Links and navigation

- **Portal → Scheduling**: dashboard shows “Request time off” / “Report sickness” when `scheduling_module` is enabled; module tile links to `/plugin/scheduling_module/` (public index).
- **Scheduling → Portal**: public pages (e.g. My day, Open shifts) include “Back to Portal” linking to `url_for("public_employee_portal.dashboard")` or equivalent.
- **Public scheduling base**: `scheduling_module/base_public.html` extends a minimal layout; use the same nav pattern as other portal modules (e.g. link back to portal).

## Data shared with other modules

- **work_module**: reads schedule shifts for “my day” and records actual_start/actual_end (and notes) via `ScheduleService.record_actual_times`. Scheduling also exposes clock-in/clock-out API and My day Clock in/out buttons.
- **ventus_response_module**: may create shifts or reference contractors; scheduling does not depend on ventus.
- **mdt_crew_profiles** (ventus): scheduling reads skills/qualifications from `mdt_crew_profiles` for open-shift job-type eligibility; ensure that table exists when using job type requirements.

## Install order

1. `time_billing_module` install (creates tb_contractors, clients, sites, job_types).
2. `employee_portal_module` install (creates ep_messages, ep_todos).
3. `scheduling_module` install (creates schedule_* tables; schedule_clock_locations is created only if `sites` exists).

Run `python app/plugins/scheduling_module/install.py upgrade` after pulling changes to add new tables/columns (idempotent).

## Optional enhancements

- **Portal dashboard**: show a “Shifts this week” or “Open shift claims pending” count by calling scheduling APIs or a small shared helper.
- **Shift tasks as todos**: when a shift has tasks and is published, optionally create `ep_todos` for the contractor so tasks appear in the portal; mark complete in scheduling and sync to ep_todos.
- **CSRF for public API**: if the app enforces CSRF on POST, ensure clock-in/clock-out and other public POSTs send the token (e.g. from a meta tag or cookie) or exempt JSON API routes where appropriate.
