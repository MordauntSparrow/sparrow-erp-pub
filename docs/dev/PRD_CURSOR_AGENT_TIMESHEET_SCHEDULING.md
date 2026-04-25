# PRD: Cursor agent — Timesheet & Scheduling (Time billing module)

**Status:** Active charter for a **specialised Cursor agent**.  
**`agent_id`:** `sparrow_timesheet_scheduling` (alias: `sparrow_time_billing` — same `read_order` in `AGENT_PRD_INDEX.md`).  
**Primary codebase:** `app/plugins/time_billing_module/`  
**Related:** Employee portal surfaces that consume timesheets/runsheets (read-only cross-check `PRD_UNIFIED_ADMIN_EMPLOYEE_PORTAL_IDENTITY.md` only if the task touches portal login or `tb_contractors`).

---

## 1. Purpose

This agent owns **scheduling, rota/shift behaviour, and timesheet/runsheet flows** inside Sparrow’s **time billing** plugin. It implements **client audit feedback** tracked under **`QA_CLIENT_AUDIT_ROUTING_2026-04.md`** (and future routing docs that assign ROTA-* or timesheet IDs to this module).

**Not in scope for this agent:** CRM, Ventus CAD, medical records clinical UI, inventory, HR appraisals — except where a thin integration hook is explicitly listed in the routing doc.

---

## 2. Mandatory discovery (before any code)

The agent **must** follow **`docs/dev/AGENT_PRD_INDEX.md`**: locate `sparrow_timesheet_scheduling`, then read **`read_order` in order**.

**Minimum reads for April 2026 audit work:**

1. **`QA_CLIENT_AUDIT_ROUTING_2026-04.md`** — full file; focus **§2** (matrix IDs **ROTA-ADDR-001**, **ROTA-PAY-001**, **ROTA-ROLE-001**) and **§3.3** (epics ROTA-1, ROTA-2).  
2. **`PRD_CURSOR_AGENT_TIMESHEET_SCHEDULING.md`** (this file).  
3. **`TIME_BILLING_RUNSHEETS_PUSH.md`** — context for runsheet/push behaviour if changing shifts or outbound data.

If the human adds **`PRD_ROTA_SHIFT_RULES_2026-04.md`** later, it slots **after** the audit routing doc per `AGENT_PRD_INDEX.md` maintenance rules.

---

## 3. Backlog from client audit (start here)

| ID | Epic | Acceptance summary |
|----|------|---------------------|
| **ROTA-ADDR-001** | Shift model | Shift creation supports **full address per shift** (not only pre-created client sites) + **staff job role** on the shift or assignment. |
| **ROTA-PAY-001** | Shift model | Pay capture supports **hourly or day rate** (and preserves/ migrates existing “labour cost” style fields where present). |
| **ROTA-ROLE-001** | Eligibility | **Configurable role ladder**: e.g. ECA cannot apply/take Para-level shift; Para may take ECA shift; hide ineligible shifts in application UI; optional admin override with audit. |

**Suggested implementation order**

1. **Data model + migrations** (address fields, pay type/rate, role metadata if missing).  
2. **ROTA-ROLE-001** enforcement (rules config + UI filter + server-side validation on apply/assign).  
3. **ROTA-ADDR-001** + **ROTA-PAY-001** admin/scheduler UI and reports.

One PR per ID if diffs would otherwise exceed ~400 lines.

---

## 4. Engineering conventions

- Match existing **`time_billing_module`** patterns: blueprints, templates, `install.py` upgrades, permissions.  
- **CSRF** on all mutating forms; align with `csrf_token()` / AJAX headers used elsewhere in Sparrow.  
- **Do not** expand into **event guest users** or cross-module identity refactors unless a separate PRD assigns them here.  
- **Seat limits** (`seat_limits.py` / users): only touch if explicitly tasked; document side effects.

---

## 5. Entry prompt (human copy)

Use with **`docs/dev/CURSOR_AGENT_ENTRY_PROMPT.md`**:

```text
agent_id: sparrow_timesheet_scheduling

Task: Implement client audit feedback from QA_CLIENT_AUDIT_ROUTING_2026-04.md — start with ROTA-ROLE-001, then ROTA-ADDR-001 and ROTA-PAY-001 (or specify one ID only for this PR).

Instructions:
1. Open docs/dev/AGENT_PRD_INDEX.md and read read_order for this agent_id.
2. Read every document in read_order, then implement the Task.
```

Add **`@docs/dev/AGENT_PRD_INDEX.md`** in Cursor.

---

## 6. Success criteria (agent handoff)

- [ ] All listed **read_order** files opened and scope restated in the chat.  
- [ ] Changes limited to **`time_billing_module`** (+ shared helpers only if unavoidable and noted).  
- [ ] Migrations idempotent; upgrade path documented in `install.py` or equivalent.  
- [ ] PR description lists **ROTA-*** IDs satisfied.

---

*Charter owner: QA / product. Update when new routing rows assign timesheet-only work.*
