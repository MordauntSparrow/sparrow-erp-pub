# CRM module — evidence-led mapping

This document ties **external practice** (what the industry and major platforms describe as useful CRM / sales overview behaviour) to **Sparrow’s CRM implementation**, so product decisions can be traced to sources and gaps are explicit.

It is a **living map**: when the dashboard or nav changes, update the “Implementation” column and status.

---

## What “evidence-led” means here

1. **Claim** — A concrete practice or metric described in a cited source (not opinion).
2. **Interpretation** — What we believe it implies for Sparrow (event-medical CRM + quotes + plans).
3. **Implementation** — Where it exists in code or UI, or why it is deferred.
4. **Status** — Done | Partial | Not done | N/A (domain mismatch).

Sources are **illustrative**, not endorsements of a vendor. Prefer updating this table over debating slogans.

---

## Source registry

| ID | Source | URL | Use in this map |
|----|--------|-----|-----------------|
| SF-1 | Salesforce (blog) — sales dashboard examples | https://www.salesforce.com/blog/sales-dashboard-examples/ | Typical dashboard *categories* (pipeline, activities, revenue lens). |
| SF-2 | Salesforce Trailhead — sales activity analysis | https://trailhead.salesforce.com/content/learn/modules/sales-activity-analysis/analyze-sales-activities | Activity reporting and tying work to outcomes. |
| M-1 | Monday.com — CRM dashboards (blog) | https://monday.com/blog/crm-and-sales/crm-dashboards/ | Real-time / at-a-glance KPIs and pipeline visibility. |
| TG-1 | Teamgate — CRM dashboard metrics (blog) | https://www.teamgate.com/blog/crm-dashboards-metrics-to-track-2026/ | Lists common metric families (pipeline, win rate, cycle, activity). |

Add rows (e.g. HubSpot, NHS digital, Purple Guide) when compliance or sector-specific evidence is required.

---

## Requirement matrix (evidence → Sparrow)

| Evidence theme | Representative claim | Sparrow interpretation | Implementation | Status |
|----------------|---------------------|---------------------------|----------------|--------|
| **At-a-glance overview** | Dashboards surface KPIs without drilling lists first (M-1, SF-1). | Single “Overview” page with counts and sections. | `crm_module.dashboard` → `templates/admin/crm_dashboard.html` | **Done** |
| **Pipeline by stage** | Pipeline health is shown by stage, not only total deal count (SF-1, TG-1). | Counts per `OPP_STAGES` + visual bar + “active” subset (excl. won/lost). | `_crm_dashboard_extras()` in `routes.py`; pipeline card in `crm_dashboard.html` | **Done** |
| **Drill to detail** | Users investigate stage pile-ups from the overview (SF-1 narrative). | Links to Kanban + list + recent opps. | Buttons/links to `opportunities_board`, `opportunities_list`, recent opportunity rows | **Done** |
| **Activity visibility** | Activity types and volume support coaching and follow-up (SF-2, TG-1). | Dated open tasks + **30-day mix by activity_type** on overview. | `_crm_dashboard_extras`: `activity_mix_30d`; `crm_dashboard.html` Activities card | **Partial** (no “closed deal ↔ activity” correlation reports) |
| **Recency / workload** | Reps need “what changed / what’s next” (SF-1). | Recent opportunities + recent quotes on dashboard. | `recent_opportunities`, `recent_quotes` in `crm_dashboard.html` | **Done** |
| **Quick actions** | Common flows one click away (general CRM UX). | Intake, board, new quote, new event plan, public enquiry. | Hero quick actions in `crm_dashboard.html` | **Done** |
| **Role-specific dashboards** | Different roles see different widgets (SF-1, SF-2). | **£ / weighted pipeline** and per-stage £ hidden unless `crm_module.edit`; read-only sees counts & bars; opp edit links gated on dashboard/search. | `show_financials=can_edit()`; `crm_dashboard.html`; `crm_search_results.html` | **Partial** (not separate exec/rep layouts) |
| **Win rate / conversion** | Track win rate and stage conversion (TG-1). | Quote status summary + **90d stage moves** to Won/Lost + recent change feed from audit table. | `quote_status_summary`; `moves_to_won_90d` / `moves_to_lost_90d`; `crm_opportunity_stage_history` | **Partial** (no % conversion by stage pair yet) |
| **Pipeline value / weighted pipeline** | Deal value and probability-weighted pipeline (TG-1, SF-1). | **SUM(amount) by stage**, open total, **weighted open** (`PIPELINE_STAGE_WEIGHTS` heuristic). | `_crm_dashboard_extras`; `PIPELINE_STAGE_WEIGHTS` in `routes.py` | **Partial** (weights are global defaults, not per-org forecast) |
| **Sales cycle length** | Time in stage / time to close (TG-1). | **Stage history** rows with timestamps; dashboard does not yet compute average days per stage or to-close. | `crm_opportunity_stage_history`; `log_opportunity_stage_change` | **Partial** |
| **Search / navigation** | Find records quickly (general CRM UX). | **CRM search** (`GET …/search?q=`) + compact search in subnav + hero form on overview. | `crm_module.crm_search`; `crm_search_results.html`; `crm_admin_base.html` | **Done** |
| **Forecast / quota** | Quota attainment and forecast (TG-1). | No quota model in CRM module. | — | **N/A** (unless product adds quotas) |
| **Data quality** | Dashboards only as good as underlying fields (SF-1). | Sparrow relies on manual entry + quote/opportunity linkage. | Training / validation rules out of scope for this doc | **Ongoing process** |
| **Domain: medical events** | Assessment-before-quote and plan documentation (internal product copy; align with sector guidance separately). | Public enquiry vs internal event plans separated in UI/nav. | `crm_admin_base.html` dropdowns; `public_enquiry_url` on dashboard; `partials/crm_medical_planning_process.html` | **Done** (UX separation); cite sector PRDs separately |

---

## How to use this in delivery

1. **Before a CRM UI change** — Add or adjust a row: which evidence claim are we satisfying?
2. **After shipping** — Flip status and paste the file path or route name.
3. **Backlog grooming** — Prioritise **Partial** and **Not done** rows that match your go-to-market (e.g. if you sell on value, add `SUM(amount)` by stage before win-rate analytics).

---

## Suggested next evidence-backed increments (ordered)

1. ~~**Pipeline value on overview**~~ — Shipped.
2. ~~**Activity mix snapshot**~~ — Shipped.
3. ~~**Quote outcomes on overview**~~ — Shipped.
4. ~~**Hide £ for read-only**~~ — Shipped (`show_financials`).
5. ~~**Stage audit + 90d won/lost + recents**~~ — Shipped (`crm_opportunity_stage_history` + logging on create/edit/Kanban/intake).
6. ~~**Weighted pipeline (heuristic)**~~ — Shipped (`weighted_pipeline_open`).
7. ~~**CRM omnisearch**~~ — Shipped (`crm_search` + nav + hero).
8. **Average days in stage / to close** — Derive from `crm_opportunity_stage_history` (TG-1).
9. **Configurable stage weights** — Store per tenant or rule set instead of `PIPELINE_STAGE_WEIGHTS` constants.
10. **Distinct manager / rep dashboards** — Separate widget sets (SF-1).

---

## Implementation index (quick)

| Area | Primary files |
|------|----------------|
| Dashboard data | `app/plugins/crm_module/routes.py` — `dashboard()`, `_crm_dashboard_extras()` |
| Dashboard UI | `app/plugins/crm_module/templates/admin/crm_dashboard.html` |
| Stage audit | `app/plugins/crm_module/crm_stage_history.py`; `crm_opportunity_stage_history` in `install.py` |
| Search | `crm_module.crm_search` in `routes.py`; `templates/admin/crm_search_results.html` |
| Subnav | `app/plugins/crm_module/templates/admin/crm_admin_base.html` |
| Styles | `app/plugins/crm_module/templates/admin/_crm_ui_styles.html` |
| Pipeline stages / weights | `OPP_STAGES`, `PIPELINE_STAGE_WEIGHTS` in `routes.py` |

---

*Last updated: stage history, weighted pipeline, financials gated by edit permission, CRM search.*
