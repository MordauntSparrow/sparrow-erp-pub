# Scheduling, Work module, and Runsheets — target model

This document captures the **intended division of responsibility** and **implementation phases** agreed for Sparrow ERP (Time Billing + Scheduling + Work + Ventus).

**Production go-live:** see [`PRODUCTION_CHECKLIST_RUNSHEETS.md`](PRODUCTION_CHECKLIST_RUNSHEETS.md).

## 1. Scheduling → timesheet (planned work)

- **Purpose**: Put **scheduled** shifts into the contractor’s week as timesheet rows (`source = 'scheduler'`), using shift start/end as **scheduled** times.
- **Contractor**: Enters **actual** start/end (and notes where allowed) for cases like school work, or when Work/Runsheet is not used — **backup path**.
- **Settings**: `scheduler_week_prefill_enabled` (existing) controls automatic prefill from `schedule_shifts` where `runsheet_id IS NULL`.
- **Pay rules**: Existing rate/policy computation on entries applies.

## 2. Work module (single-crew planned stops)

- **Purpose**: **Execution** of **planned** shifts (cleaning run, care visits): notes, **actual** times, photos → **client / visit record** (account under the visit).
- **Timesheet**: Actual times **sync into** the contractor’s timesheet row when the shift is linked to a runsheet assignment (`source = 'runsheet'`) or after auto materialisation (existing `sync_shift_to_time_billing` / `create_and_publish_runsheet_for_shift`).
- **Overrides**: Contractor or admin may still **edit the timesheet row** (e.g. no signal in the field) — existing edit flow; Work is not the only editor.

## 3. Runsheets (emergency / unplanned / Ventus crew jobs)

- **Purpose**: **Non-planned** or **response** jobs: Ventus sign-on can auto-create a runsheet + assignments; contractors can start a **draft** from a **template**, fill **payload**, add **crew** via **contractor search** (field UI).
- **Admin**:
  - **Templates only** in day-to-day use: define form fields for runsheet payloads.
  - **Exception**: Edit a runsheet to fix errors, **re-publish / resync** to timesheets, or create a **manual** runsheet **on behalf of** crew (numeric contractor IDs; no primary crew-search UX on admin).
- **Contractor**:
  - Primary **create / edit / crew add / save draft / publish** flows.
  - **Persistence**: drafts saved to DB; user can leave and return (no “keep tab open”).
  - **Withdraw from payroll** (per assignment): see §4.

## 4. Per-assignment payroll inclusion (crew conflict / duplicate jobs)

- Each **runsheet assignment** has `payroll_included` (default **included**).
- If a contractor **withdraws** from their line on this runsheet:
  - Their row is **not** updated on publish; any existing **`source='runsheet'`** line for that runsheet+user is **removed** (or left stale — we **delete** on withdraw for a clean pay picture).
  - **Other assignments unchanged**; other contractors still see the full runsheet.
  - UI: that assignment appears **greyed** with reason “withdrawn from this runsheet’s payroll” / “not counting toward timesheet”.
- **Reactivate**: Contractor sets `payroll_included` back on; **Publish** (or admin resync) recreates/updates their timesheet row from assignment times.
- **Scenario** (two units, duplicate naming): Maker removes self from runsheet A after being included on B; **left-out** crewmate on A keeps times and can still publish — **no whole-sheet delete** for everyone.

## 5. Data model (see migration `010_runsheet_assignment_payroll.sql`)

`runsheet_assignments`:

| Column | Meaning |
|--------|---------|
| `payroll_included` | `1` = include in publish; `0` = withdrawn |
| `withdrawn_at` | When withdrawn |
| `withdrawn_by_user_id` | Contractor who withdrew (must match `user_id` for self-service) |
| `reactivated_at` | Last reactivation (optional audit) |

Future optional: `runsheets.voided_at` for whole-job mistakes (separate from per-user withdraw).

## 6. Implementation phases

| Phase | Scope |
|-------|--------|
| **A** (done in repo) | Migration `010_runsheet_assignment_payroll.sql` + `publish_runsheet` skips `payroll_included=0` and purges stale timesheet rows; `withdraw` / `reactivate` APIs; public `GET/POST/PUT .../api/my/runsheets` + `publish` + assignment withdraw/reactivate; public `GET /api/refs/contractors`; **lead** contractor may PUT/publish (if `lead_user_id` NULL, any participant may — legacy/Ventus). Admin runsheet editor = numeric IDs only. |
| **B** | Contractor **pages** (MDB): **done** — `/time-billing/runsheets`, `/runsheets/new`, `/runsheets/<id>`; template picker + schema-driven payload; crew search; save (`create_runsheet` persists assignments); lead publish; non-lead `PATCH .../my-assignment` for actuals (when runsheet already **published**, timesheet row is updated + pay recomputed via `refresh_entries_actuals`); withdraw/reactivate; nav + dashboard link. |
| **C** | **Done (ops + docs):** Admin **Publish / resync** and list-row **Publish** call the same `publish_runsheet` path; `ventus_integration` module doc explains calling publish after sign-off. Optional: auto-publish hook from Ventus batch — not in repo. |
| **D** | **Done:** Work module **`work_visits`** (1:1 with `schedule_shifts`), **`work_photos.visit_id`**, admin visits list + filters, visit ref on public stop + hours CSV. Upgrade: `python -m app.plugins.work_module.install upgrade`. |
| **E** | **Done (default policy):** When Work/Scheduling materialises a shift via `create_and_publish_runsheet_for_shift`, the matching **`source='scheduler'`** prefill row (same `runsheet_id` = schedule shift id) is **removed** if the contractor has **not** manually edited it (`edited_by IS NULL`). Tenants that rely only on scheduler rows can keep prefill; linking to a runsheet avoids duplicate pay lines. |

## 7. API summary (contractor, `public_time_billing`)

- `GET /api/my/runsheets` — runsheets where user is lead or assignee.
- `GET /api/my/runsheets/<id>` — detail if participant.
- `POST /api/my/runsheets` — create draft (body: client_id, job_type_id, work_date, optional template_id).
- `PUT /api/my/runsheets/<id>` — update header/payload/assignments (permission: lead or assignee; refine rules in B).
- `POST /api/my/runsheets/<id>/publish` — same as publish (participant checks TBD).
- `POST /api/my/runsheets/<id>/assignments/<ra_id>/withdraw` — self only.
- `POST /api/my/runsheets/<id>/assignments/<ra_id>/reactivate` — self only.
- `GET /api/refs/contractors?q=` — staff contractor search (min 2 chars).

Admin internal APIs unchanged except behaviour of publish respecting `payroll_included`.

## 8. Calendar pay policies (timesheet line rates)

Night **PRORATA**, **TIME_BANDS**, legacy night max-of-line, weekday anchoring, and bonus limitations are documented for admins and implementers in [`POLICY_PAY_OPERATING_NOTES.md`](POLICY_PAY_OPERATING_NOTES.md) (see `RateResolver` in the time billing `services.py`).
