# Work Planner vs Scheduling: Combine or Keep Separate?

## Recommendation: **Keep them separate** (but tightly integrated)

### Why separate works better

| Aspect | Scheduling module | Work module |
|--------|-------------------|-------------|
| **Purpose** | Who works when and where (planning, capacity, time off) | Executing the plan: record times, notes, photos at each stop |
| **Primary users** | Schedulers, admins; contractors (view availability, request time off) | Contractors in the field; admins (review times, photos) |
| **Data** | Shifts, availability, time-off requests | Same shifts + actual times, notes, work_photos; syncs to Time Billing |
| **Lifecycle** | Plan can change up to the day of | Once recorded, work is historical evidence for payroll/compliance |

- **Single responsibility**: Scheduling owns “the plan”; Work owns “what actually happened” and evidence (notes, photos). Different change patterns and permissions.
- **Reuse**: Other systems (e.g. Ventus) can create or update `schedule_shifts` without going through the Work UI. Work just consumes the schedule.
- **Apps**: You can ship a “Scheduling” app for managers and a “Work” app for field staff, or one app with both sections.

### How they work together

- **Schedule** is the source of truth for “my day”. Work module reads `schedule_shifts` and enriches the contractor view with **client details** (client name, site name, postcode, job type).
- Contractors open **Work planner** (Work module) to see today’s schedule + client/site info, then record times, notes and photos per stop.
- Work writes `actual_start`, `actual_end`, `notes` back to `schedule_shifts` and syncs to Time Billing; photos stay in `work_photos`.

### If you merged them

- One plugin would own both “who’s working when” and “what they did + photos”. That’s a lot of surface area and mixed concerns.
- Any integration that only needs to create shifts (e.g. Ventus) would still depend on the same codebase as photo upload and timesheet sync.
- Harder to offer “Scheduling only” or “Work only” for different roles or white-label apps.

**Bottom line:** Keep Scheduling and Work as separate modules; keep the Work planner (schedule + client details + notes/photos) inside the Work module, fed by Scheduling’s data.
