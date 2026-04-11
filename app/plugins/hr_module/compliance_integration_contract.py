"""
Cross-module boundaries for HR compliance (hr_staff_details / document library).

**HR compliance is informational in the admin and employee portal UI.** It is stored for
people ops visibility (expiries, gaps, document requests). It is **not** automatically
enforced as eligibility for other products unless explicitly implemented there.

**Ventus (ventus_response_module / CAD)**  
- Uses `tb_contractors` / `contractor_ventus_mapping` and `mdt_crew_profiles` (skills,
  qualifications JSON) for dispatch display.  
- **Does not** read HR profile expiries (`driving_licence_expiry`, `right_to_work_expiry`,
  `dbs_expiry`, `contract_end`) or the HR document library for sign-on or job gating.  
- Concrete handlers: `mdt_sign_on` (crew validated as contractors, not HR expiries);
  `jobs_eligibility` (skills/operational matching, not HR compliance).  
- Longer matrix: `docs/dev/STAFF_ELIGIBILITY_MDT.md`.  
- Time Billing `ventus_integration.on_ventus_sign_on` creates runsheets from mapping +
  defaults only — **no HR compliance checks**.

**Cura / medical_records_module**  
- Clinical access control is separate (EPCR unlock, roles, audit). **No** dependency on
  HR compliance fields for case access in current code paths.

**If product requires enforcement** (e.g. block MDT sign-on or timesheet publish when DBS
is expired), that must be added explicitly on the relevant login/API route with tests;
do not assume it exists because HR data is present.
"""
