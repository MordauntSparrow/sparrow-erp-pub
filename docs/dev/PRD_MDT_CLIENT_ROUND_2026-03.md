# PRD — MDT client round (MOTD, major incident, CAD scene contract)

**Source index:** [QA_CLIENT_REPORT_ROUTING_2026-03.md](./QA_CLIENT_REPORT_ROUTING_2026-03.md)  
**Repos:** Sparrow `ventus_response_module` (API + CAD), Ventus MDT `vue-connector`.

---

## MDT-MOTD-001 — Message of the day (global on MDT)

### Behaviour

- Crew sees the same dispatch MOTD as the CAD dispatch panel, after sign-on.
- **Modal** (`MdtMotdModal`, z-index 8500): dismissible; **does not cover** sign-off or stand-down (those use 9999); panic remains above MOTD by policy.
- **Re-show** when MOTD **content or metadata** changes (see version rule below).
- **Dismiss** stores the server `version` in `localStorage` under `mdtMotdDismissedVersion` (exported as `MDT_MOTD_DISMISSED_VERSION_KEY` in the MDT client). Cleared on full session reset / sign-off in the current client so the next sign-on can show MOTD again if still active.

### Server version rule

`version` = **SHA-256** (hex) of UTF-8 string:

`{motd_text}\x1e{motd_updated_by}\x1e{updated_at_iso}`

- `updated_at_iso` is the stored `motd_updated_at` serialized with `.isoformat()` when applicable.
- Any change to text, editor, or timestamp yields a **new** `version`.

### Client compare rule

- Poll **GET** `{api_base}/mdt/{callsign}/motd` every **3 minutes** and on **tab visibility** (`visibilitychange` → visible).
- If `text` is non-empty **and** `version !== localStorage.mdtMotdDismissedVersion`, open the modal.
- On dismiss, set `mdtMotdDismissedVersion = version`.

### CAD / real-time

- Dispatch POST to `/plugin/ventus_response_module/dispatch/motd` emits Socket.IO `mdt_event` with `type: dispatch_motd_updated` (CAD toast). MDT does not yet subscribe on-socket for MOTD; polling covers update detection within the poll window.

### Acceptance (MOTD)

- [ ] Empty MOTD: no modal.
- [ ] Non-empty MOTD: modal after login when version not dismissed.
- [ ] Dismiss: same version does not re-open until next fetch still matches dismissed.
- [ ] Change MOTD in CAD: new `version` → modal returns (within poll or after foreground).
- [ ] Sign-off / stand-down: MOTD hidden while those modals are open.

---

## MDT-MI-001 / CAD-MI-001 — Major incident handoff (MDT → CAD)

### MDT UI

- **Major incident handoff** in sidebar and under mobile **More** (`MajorIncident` nav action).
- Modal: templates **METHANE**, **TST**, **MITT**, **CUSTOM**; optional **CAD** link; submits to API.

### API

- **POST** `{api_base}/mdt/{callsign}/major_incident_handoff`
- JSON body:
  - `template`: `METHANE` | `TST` | `MITT` | `CUSTOM`
  - `fields`: object (string values); max serialized size 48KB
  - `notes`: optional, trimmed, max 4000 chars
  - `cad`: optional positive integer; if present and job exists, a **job comm** line is inserted
- Auth: crew JWT must match unit (`_mdt_jwt_allowed_for_callsign_crew`).
- **200** `{ ok, id, message }`; non-OK returns `error` message for client UX.

### Server-side trail and CAD alert

- Row in `mdt_major_incident_handoff` (audit storage).
- `log_audit('mdt_major_incident_handoff', …)`.
- Socket.IO `mdt_event`: `type: major_incident_handoff` (id, callsign, template, cad, summary, notes, fields, actor) — CAD dashboard: toast, chime, badge, panel refresh.

### Gaps / joint work

- **Start TST** as a distinct operational workflow (beyond structured POST) is **not** implemented; define with CAD product owner.
- **E2E:** run against CAD test environment: POST handoff → row + audit + socket + optional job comm.

### Acceptance (major incident)

- [ ] Each template submits and shows failure message on HTTP error.
- [ ] With valid `cad` on existing job: comm visible on CAD job.
- [ ] CAD receives `major_incident_handoff` event (toast / refresh).

---

## CAD-SCENE-001 — Scene state / other callsigns (contract, joint)

**Problem statement:** MDT may show other callsigns “at scene” while CAD does not, including cases where a unit is **not** truly at scene. Fix requires **both** sides and agreed server rules — **do not implement MDT-only “fixes”** that contradict CAD truth.

### Agreed principles (draft for sign-off)

1. **Source of truth:** CAD incident resource state (or equivalent job/unit allocation table) is authoritative for **dispatch-facing** “at scene” and resource lists unless policy states otherwise.
2. **MDT display:** MDT may show **derived** scene lists (e.g. peer units) only when built from the **same** contract the CAD uses, or clearly labelled as **MDT-local** (GPS / proximity) with SLA and disclaimers.
3. **MDT → server:** Status transitions (`on_scene`, etc.) must use the **same** validation rules as CAD (geofence, dispatcher override, debounce) once defined.
4. **Latency:** Expect **eventual consistency**; define max acceptable delay (e.g. WebSocket vs poll) per environment.

### Open points (to close with CAD)

- Geofence vs manual “at scene” vs dispatcher override precedence.
- Whether “at scene” for secondary units requires **assignment to incident** vs **proximity only**.
- Payload fields for `mdt_event` or REST sync for unit scene roster.

### Acceptance (documentation)

- [ ] This section reviewed by CAD + MDT owners; open points ticketed.

---

## Traceability

| ID            | Document section        |
|---------------|-------------------------|
| MDT-MOTD-001  | MDT-MOTD-001            |
| MDT-MI-001    | MDT-MI-001 / CAD-MI-001 |
| CAD-SCENE-001 | CAD-SCENE-001           |
