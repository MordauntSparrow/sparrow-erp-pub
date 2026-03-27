# Training module

Delivers **internal training** (lessons, quizzes, supervisor sign-off) and **external / evidence** workflows (certificate upload, admin verification) separately from **Compliance** policy acknowledgements.

## Install / upgrade

```bash
python app/plugins/training_module/install.py install
```

Creates `trn_*` tables and **one-time migrates** legacy `training_items` / `training_assignments` / `training_completions` into the new model (recorded in `training_migrations`).

## URLs

| Audience | URL |
|----------|-----|
| Staff (portal session) | `/training/` |
| Admin | `/plugin/training_module/` → Courses, Assignments, Completions, Audit |

## Delivery types

- **internal** — lessons + optional quiz; cannot use “mark complete” if `internal_signoff` or external types apply.
- **internal_signoff** — content + **competency sign-off** in admin.
- **external_required** — certificate / file upload; optional admin verification; internal lesson completion does **not** satisfy the assignment.
- **evidence_only** — upload proof for admin review.

## Compliance

Optional `comp_policy_id` on a course or lesson links to `/compliance/policy/<id>` for **reading only**. Legal acknowledgement remains in the Compliance module.

## Employee portal

`get_pending_training_count()` uses `TrainingService.count_pending_for_contractor()` (efficient `COUNT`).

## Ventus / HR

- **Role bulk assign** uses `tb_contractors.role_id` when present.
- Call `TrainingService.assign_contractor(...)` from HR on hire when you add integration hooks.

## Tests

```bash
pytest tests/test_training_module.py -q
```
