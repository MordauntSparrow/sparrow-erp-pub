# PRD — Ventus CAD Agent

**Scope:** Dispatcher / CAD operational truth, resource state, and major-incident workflows aligned with MDT and field feedback.  
**Source index:** [QA_CLIENT_REPORT_ROUTING_2026-03.md](./QA_CLIENT_REPORT_ROUTING_2026-03.md)  
**Primary repo:** Sparrow `ventus_response_module` (CAD / dispatcher UI, dispatch APIs, `mdt_jobs` / unit assignment).

**Acceptance (global):** Client repro scenario for scene sync **closed with server + CAD logs**; major-incident paths **integration-tested** with MDT handoff; **no silent discard** of MDT major-incident payloads (every accepted message ack’d; every reject logged with reason).

**Dependencies:** Joint API/schema session with MDT for **METHANE / TST / MITT** payloads and **scene sync** contract (see §6).

---

## 1. Deliverables by ID

| ID | Summary | Status | Test notes |
|----|---------|--------|------------|
| **CAD-SCENE-001** | Reconcile **at scene** (and related resource flags) so CAD matches MDT when MDT marks units; fix incorrect “at scene” when crew are not at scene; document **source of truth**, **sync direction**, and **reconciliation / logging**. | Backlog | Repro from client (incident ID, timestamp, callsigns — [QA §5 item 4](./QA_CLIENT_REPORT_ROUTING_2026-03.md)); assert parity within agreed latency. |
| **CAD-MI-001** | **Major incident:** support **TST**, **MITT**, **METHANE** in CAD/dispatcher workflow — incident model updates, **alerts**, **linkage to resources**; **receive MDT-initiated payloads** ([MDT-MI-001](#3-related-work-packages-mdt--sparrow)). | Backlog | Template scenarios + MDT→CAD handoff tests; verify audit + dispatcher alert channel. |

---

## 2. CAD-SCENE-001 — Resource / scene truth

### 2.1 Problem (from client / QA)

- Other callsigns show **at scene** on **MDT** but not on **CAD**, or CAD shows at-scene when MDT / ground truth says otherwise.
- Risk: command picture diverges from crew-reported state → wrong prioritisation and handover.

### 2.2 Current implementation (baseline — to validate in code during impl.)

- Job lifecycle uses `mdt_jobs.status` and status history (e.g. `on_scene` appears in analytics and close-job rules in `ventus_response_module`).
- CAD UI and maps treat statuses such as `mobile`, `on_scene`, `leave_scene`, `at_hospital` consistently in several templates; **per-unit** “at scene” may be represented separately (e.g. `mdt_job_units`, job `data` JSON, or MDT-only state — **inventory during spike**).
- **Gap:** If MDT derives “at scene” from a **unit-level** or **client-computed** signal that never persists to the same fields CAD reads, surfaces will diverge.

### 2.3 Source of truth (to be fixed in joint session — proposal)

| Layer | Role |
|-------|------|
| **Authoritative store** | Sparrow DB tables the dispatcher API already reads for job list + unit rows (e.g. `mdt_jobs`, `mdt_job_units`, status event log if present). |
| **MDT** | **Originator** of crew-tapped transitions (mobile → on scene → …) **unless** policy says dispatcher overrides. |
| **CAD** | **Consumer** of the same fields for list, detail, and map badges; may **also** send dispatcher-driven updates that write the same store. |

**Decision required:** For each transition (e.g. on scene), specify **who may write**, **idempotency key**, and **conflict rule** (last-writer-wins vs dispatcher-wins vs crew-wins with audit).

### 2.4 Sync direction

1. **MDT → server:** Every MDT-marked scene/resource change must hit a **versioned** API that updates the authoritative row(s) and optional **status timeline** rows.
2. **Server → CAD:** Polling or push (existing CAD refresh mechanism) must include **per-unit** scene state when the job has multiple assigned units.
3. **CAD → server (optional):** Dispatcher manual “force status” remains supported only if product requires it; if so, it must **emit the same event type** as MDT so MDT can reconcile.

### 2.5 Reconciliation and logging

- **Structured log** (level INFO minimum) on every scene-related write: `cad`, `callsign`, `old_status`, `new_status`, `source` (`mdt` | `cad` | `system`), `request_id`, `user_id` if applicable.
- **Mismatch detector (optional P1):** Background or on-read check: if MDT last reported `on_scene` at T1 and CAD display still shows `mobile` after SLA, emit **reconciliation warning** with correlation IDs.
- **Metrics:** Counter for “MDT on_scene accepted”, “rejected (invalid transition)”, “CAD override”.

### 2.6 Acceptance (CAD-SCENE-001)

- Given the **client repro** incident, after fix: **MDT and CAD show the same at-scene set** for the same callsigns within **≤ N seconds** (N agreed with ops; default proposal 10s with normal poll interval).
- No unit shown **on scene** on CAD when MDT has not recorded on-scene (or GPS/policy rule says not at scene — if GPS rule is in scope, define in joint session).
- **Logs** available to prove the chain for one signed-off test run.

---

## 3. Related work packages (MDT + Sparrow)

| ID | Owner | Link to this PRD |
|----|-------|------------------|
| **MDT-MI-001** | MDT (+ CAD API) | Ingest target for **CAD-MI-001**; METHANE / TST / MITT **hot buttons** → structured POST + urgent dispatcher alert. |
| **MDT scene epic** (QA “Epic MDT-1”) | MDT | Pairs with **CAD-SCENE-001**; same API fields and latency targets. |
| **Sparrow alerting** | Optional | Email/SMS/push for major incident — only if product wants off-CAD channels; not blocking CAD-MI-001 MVP. |

---

## 4. CAD-MI-001 — Major incident (TST, MITT, METHANE)

### 4.1 Definitions (operational — clinical wording via client)

- **METHANE** — Major incident declaration / handover mnemonic (e.g. Major incident declared, Exact location, Type of incident, Hazards, Access, Number, Emergency services). **Structured fields** in API, not only free text.
- **TST** — **Tactical support team** (or client’s exact expansion): workflow state on the **incident record** (declared, mobilising, on scene, stood down, etc.) — **enum TBD** with client.
- **MITT** — **Major incident triage tool** (or client definition): linkage to **patients / triage sieve**, may reference **resources** and **sectors** — **schema TBD** with client.

### 4.2 Incident model (CAD / server)

- Extend or normalise **incident** / **job** representation so a CAD can be flagged **`major_incident: true`** with:
  - `mi_declared_at`, `mi_declared_by` (callsign / user id),
  - `mi_type` (`methane` | `tst` | `mitt` | `composite`),
  - **Structured METHANE** payload (JSON column or child table),
  - **TST / MITT** state and references (tables or JSON with migration path).
- **Resource linkage:** Associate responding units, command roles, and optional **sector** assignments to the same incident record for dispatcher board filters.

### 4.3 Dispatcher / CAD UX

- **High-visibility banner** or rail indicator when any active job on the board is a major incident.
- **Incident detail panel:** METHANE summary, editable only per permissions; timeline of MDT vs CAD updates.
- **Templates:** Pre-filled METHANE / TST / MITT **dispatcher actions** where CAD can complete or amend after MDT kick-off (product decision).

### 4.4 MDT-initiated payloads (no silent drop)

- **Single ingress route** (e.g. `POST …/major-incident` or extension of existing MDT sync) with:
  - JSON Schema or OpenAPI fragment **shared with MDT repo**.
  - **201/200** with echo of stored record id; **4xx** with machine-readable `error_code`; **5xx** retriable by MDT.
- Server must **log body hash + correlation id** on success and on validation failure (avoid logging full PHI in production if payloads contain patient detail — **redaction policy** in joint session).
- **MDT retry:** Idempotency key (`client_message_id`) so duplicate posts do not duplicate incidents.

### 4.5 Alerts

- **In-CAD:** Toast + persistent indicator + optional sound (feature-flag).
- **Out-of-band:** Optional integration with Sparrow notifications — separate ticket if required.

### 4.6 Acceptance (CAD-MI-001)

- **TST / MITT / METHANE** templates exercised end-to-end: MDT sends payload → CAD shows incident → dispatcher sees alert → resources linked.
- **Integration tests** in CI or staging: golden JSON fixtures from MDT-MI-001 contract.
- **No silent drop:** assert that invalid payloads return **non-2xx** and appear in logs; valid payloads always create/update store and return id.

---

## 5. Sequencing and risk

| Priority | Item |
|----------|------|
| **P0** | **CAD-SCENE-001** (command safety) — aligns with QA matrix P0 theme. |
| **P3 / coordinated** | **CAD-MI-001** + **MDT-MI-001** on same **critical path**; do not implement CAD ingest without frozen schema. |

**Risk:** Building METHANE UI before schema freeze causes rework — mitigate with **feature flag** and stub payloads in dev only.

---

## 6. Joint session checklist (dependencies)

**Attendees:** CAD backend, MDT (vue-connector), optional ops/clinical.

**Scene sync**

- [ ] Enumerate all statuses that imply “at scene” or “en route” on MDT.
- [ ] Map each to DB columns / events CAD already reads.
- [ ] Agree write paths (MDT-only vs bidirectional).
- [ ] Agree SLA and poll vs WebSocket.

**Major incident**

- [ ] METHANE field list (required vs optional) and max lengths.
- [ ] TST state machine diagram.
- [ ] MITT data: triage categories, patient counts, linkage to CAD number vs internal id.
- [ ] Idempotency and versioning (`schema_version` in payload).
- [ ] Auth: MDT service token vs user JWT.

**Output of session:** OpenAPI snippet or `docs/dev/CONTRACT_MDT_CAD_MI_2026.md` linked from this PRD.

---

## 7. Traceability

| QA matrix row | This PRD |
|---------------|----------|
| CAD-SCENE-001 | §2 |
| CAD-MI-001 | §4 |
| Epic CAD-1 / CAD-2 | §2, §4 |

---

*Maintain status column in §1 as work moves backlog → in progress → done. Link implementation PRs and log excerpts under “Test notes”.*
