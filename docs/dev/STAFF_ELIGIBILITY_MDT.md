# Staff “compliance on shift” vs Sparrow enforcement

Marketing or internal docs sometimes imply that only **HR-compliant** staff can appear on shift in CAD/MDT. This note records **what the codebase actually does** so claims can be aligned with implementation.

## Summary

| Area | Enforced against HR compliance (DBS, RTW, contract end, document library)? |
|------|----------------------------------------------------------------------------|
| MDT sign-on | **No** — crew must map to valid contractor users; no HR expiry checks. |
| Job/unit eligibility (Ventus) | **No** for HR — ranking uses signed-on units, division, **skills/tags** from MDT crew data vs job payload, distance, break rules. |
| Cura / medical operational roster | **No** for HR — roster/MDT/callsign/division alignment for clinical workflows, not HR certificates. |
| Fleet (vehicles) | **Vehicle** MOT/tax/insurance style compliance only — not staff HR. |

## Ventus (`ventus_response_module`)

- **`POST /api/mdt/signOn`** (`mdt_sign_on` in `routes.py`): validates crew via `_validate_crew_usernames` / contractor mapping — **not** `hr_staff_details` or document library.
- **`GET /jobs/eligibility`** (`jobs_eligibility`): ranks units using live sign-on, division, **skills** from crew/MDT profile data vs job requirements — **not** HR compliance fields.

## HR module (`hr_module`)

Compliance fields and the document library are **informational** for people ops unless a product explicitly wires enforcement on a specific route. See `compliance_integration_contract.py`.

## Medical (`medical_records_module`)

Operational period / MDT alignment (e.g. callsign, division, co-assignment) supports clinical process control — **not** automatic gating on HR compliance expiries.

## Fleet (`fleet_management`)

Compliance sync and readiness apply to **assets** (vehicles), not “only compliant people on shift.”

## If product wants enforcement

Blocking sign-on or assignment based on HR data must be implemented **explicitly** on the relevant API (with tests). Do not assume it exists because HR stores expiries.
