# QA B8 — Ventus high-risk audit behaviour

Blueprint prefix: `/plugin/ventus_response_module`. The same MDT paths are also registered at the site root via `api_compat` (e.g. `/api/mdt/signOn` → same view as `/plugin/ventus_response_module/api/mdt/signOn`).

On any `log_audit` failure, the app still emits structured fallbacks: `ventus_siem_audit` (fields `user`, `cad`, `action`, `ts`) and `ventus_audit_fallback` with extended context.

| HTTP route (method) | `log_audit` action | On audit DB failure |
| --- | --- | --- |
| `/plugin/ventus_response_module/job/<cad>/assign` (POST) | `assign_job` | **503** `audit_unavailable`; DB transaction rolled back |
| `/plugin/ventus_response_module/job/<cad>/unassign` (POST) | `unassign_job_units` | **503**; rollback |
| `/plugin/ventus_response_module/job/<cad>/standdown` (POST) | `standdown_job` | **503**; rollback |
| `/plugin/ventus_response_module/job/<cad>/close` (POST) | `close_job` | **503**; rollback |
| `/plugin/ventus_response_module/job/<cad>/force-status` (POST) | `force_job_status` | **503**; rollback |
| `/plugin/ventus_response_module/dispatch/assist-requests/<id>/resolve` (POST) | `assist_request_resolve` | **503**; rollback |
| `/plugin/ventus_response_module/unit/<callsign>/change-callsign` (POST) | `unit_change_callsign` | **503**; rollback |
| `/plugin/ventus_response_module/api/mdt/signOn` (POST) | `mdt_sign_on` | **503**; rollback |
| `/plugin/ventus_response_module/api/mdt/signOff` (POST) | `mdt_sign_off` | **503**; rollback |
| `/plugin/ventus_response_module/api/mdt/<cad>/claim` (POST) | `mdt_claim` | **503**; rollback |
| `/plugin/ventus_response_module/api/mdt/<cad>/status` (POST) | `mdt_status` | **503**; rollback |
| `/plugin/ventus_response_module/api/mdt/<cad>/comms` (POST) | `mdt_job_comm_message` / `mdt_job_comm_update` | **SIEM fallback only** — request already committed; comms remain stored |
| `/plugin/ventus_response_module/job/<cad>/comms` (POST), `type=update` | `cad_job_comm_update` | **SIEM fallback only** — same rationale as MDT comms |

All other `log_audit` call sites keep default `siem_fallback` unless changed above.
