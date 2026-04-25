# Spike: EPCR “case changed” wake-up signal (Sparrow ↔ Cura)

**Status:** Design / spike only — **not implemented** in application code until promoted from P2.  
**PR title hint:** `docs: spike EPCR case realtime notify (Socket.IO option)` vs a future `feat(epcr): …` when implemented.

---

## 1. What this feature is (and is not)

| In scope | Out of scope |
|----------|----------------|
| After a **successful** DB commit on an EPCR case mutation, optionally emit a **minimal** event so clients can **re-poll** `GET /plugin/medical_records_module/api/cases/<id>` | Pushing full case JSON, live cursors, or changing merge / `record_version` rules |
| Same **authorization story** as HTTP: only users who may read the case should learn that it changed | `broadcast=True` to all sockets for EPCR |
| Optional **debounce** per `case_id` to limit noise | Safeguarding, Ventus MDT consumer, event manager |

**Payload shape (illustrative):**

```json
{
  "type": "epcr_case_updated",
  "caseId": 12345,
  "updatedAt": "2026-03-27T12:00:00.000000"
}
```

No patient identifiers, sections, or narrative text.

---

## 2. Technical options (trade-offs)

| Option | Where | Pros | Cons |
|--------|--------|------|------|
| **A. Socket.IO** (same instance as MDT) | `app.socketio` (`app/__init__.py`), handlers in `app/create_app.py` | Reuses production infra (Redis `message_queue` when set); same patterns as `ventus_response_module` | Cura needs `socket.io-client`; JWT must map to **rooms** used for emit |
| **B. SSE** | New Flask route, `text/event-stream` | One-way, simple mental model | Proxies/timeouts; second long-lived channel to operate |
| **C. Short poll only** | Cura | No Sparrow change | Already ~10s in `EpcrContext.tsx`; faster = tighter interval = more load |

**Recommendation for approval:** **Option A** behind env **`EPCR_CASE_SOCKET=1`** (or similar), default off until validated.

---

## 3. Reference: how Socket.IO works today

### 3.1 Instance

- Module singleton: `from app import socketio` (`app/__init__.py`).
- Initialised in `create_app()` (`app/create_app.py`, ~794–816): `socketio.init_app(app, cors_allowed_origins=…, async_mode=…, message_queue=redis_url?)`.

### 3.2 Connect / rooms (JWT path)

`@socketio.on('connect')` in `create_app.py` (~831–884):

- **Flask-Login browser:** joins `panel_user_<user_id>`.
- **JWT** (`auth.token` / query `token` / `jwt`): decodes via `decode_session_token`; joins **`mdt_user_<sub>`** (JWT `sub` = user id) and optionally **`mdt_callsign_<CS>`**.

EPCR/Cura JSON API principals come from the same JWT stack (`auth_jwt.py`: `sub`, `username`, `role`) — see `_cura_auth_principal()` in `medical_records_module/routes.py`.

**Gap:** Assigned collaborators are identified by **`username`** strings in `assignedUsers`, while the socket already joins **`mdt_user_{sub}`** (numeric/string user id). **Do not assume** `sub` equals username.

**Room design options (pick one when implementing):**

1. **`epcr_user_<username>`** — On `connect`, after JWT decode, also `join_room(f"epcr_user_{_sanitized_username}")` using the `username` claim (normalise to lowercase, restrict charset). Emit case updates to `room=epcr_user_alice`, `room=epcr_user_bob`, … for each entry in **post-commit** `assignedUsers` plus **privileged** viewers (see §4).
2. **Resolve username → `sub`** — DB lookup for each assignee, emit to `mdt_user_{sub}` — reuses existing rooms but adds query cost and must stay in sync with `assignedUsers`.

Option 1 is usually simpler for “emit to assigned usernames” without N DB hits.

### 3.3 Emit pattern reference (MDT)

`ventus_response_module/routes.py`:

- `_emit_mdt_job_assigned` (~35–64): loops callsigns, `socketio.emit("mdt_event", {…}, broadcast=True)` — **not** suitable as a model for EPCR (too broad).
- Targeted example: `socketio.emit('mdt_event', _cc_apply, room=f'mdt_callsign_{old_cs}')` (~4392) — **room-scoped** pattern to copy for EPCR.

---

## 4. Security and compliance

1. **No emit without matching access rules**  
   Recipients must be exactly those who could **GET** the case: usernames in `assignedUsers` **plus** users with **`_epcr_privileged_role()`** who are allowed to open the case (same as `_user_may_access_case_data` / Caldicott for browser privileged — mirror HTTP behaviour; do not leak to arbitrary admins not in session rules).

2. **Never** `broadcast=True` for `epcr_case_updated`.

3. **Minimal payload** — `caseId` + `updatedAt` (+ optional `recordVersion` if useful for dedupe only; still no PHI).

4. **Rate limit / debounce** — In-process dict: `(case_id,) -> last_emit_monotonic_ts`; skip if same case within e.g. **1s** (tunable). Document that multi-instance deployments need Redis-backed debounce if strict global cap is required.

5. **Audit (optional, no PHI)** — e.g. `logger.info("epcr_case_updated emitted case_id=%s recipients=n", case_id)` or existing audit logger pattern; do not log patient or section content.

---

## 5. Exact emit call sites (after successful `commit`)

All in **`app/plugins/medical_records_module/routes.py`**, internal blueprint `/plugin/medical_records_module/...`.

| # | Function | Trigger | Approx. location (search) |
|---|----------|---------|---------------------------|
| 1 | `cases_handler` | **POST** create/upsert success (`conn.commit()` after insert/update) | `_audit_epcr_api(f"EPCR API created/upserted case {case_id}")` |
| 2 | `case_handler` | **PUT** update success | `_audit_epcr_api(f"EPCR API updated case {case_id}")` |
| 3 | `add_case_collaborators` | **POST** collaborators success | `_audit_epcr_api(f"EPCR API added collaborators to case {case_id}")` |
| 4 | `close_case` | **PUT** close success | Immediately after `conn.commit()` that closes the case (~7539); before email side-effects |

**Also consider:** idempotent **POST** replays that return 200 without writing — **no** emit (no change). **PUT** idempotency paths that short-circuit without commit — **no** emit.

**Version conflict (409):** no emit (nothing persisted).

---

## 6. Cura client (when commander approves client work)

**Today:** `ecpr-fixit-buddy/src/contexts/EpcrContext.tsx` — `useEffect` on `state.currentCase?.id` sets **`setInterval(..., 10000)`** calling `apiService.pollCase(caseId)`; merges when `remoteCase.updatedAt !== local.updatedAt` (~887–908).

**Future (minimal):**

- Add `socket.io-client` (or thin `src/services/epcrRealtime.ts`).
- Connect with **same Bearer JWT** as REST (Socket.IO `auth: { token }` or query string — matches `create_app` connect handler).
- Subscribe only while **`currentCase` is set** (join lifecycle tied to case screen) **or** connect at login and filter client-side by `caseId` — latter is simpler but receives events for other cases the user is assigned to (still OK if payload is only ids; client ignores non-matching).
- On `epcr_case_updated`, if `caseId === currentCase.id`, **debounced** `pollCase` (e.g. 300–500 ms) — same code path as today’s interval.
- **Failure mode:** if socket disconnected or feature off, behaviour **identical** to current 10s polling.

---

## 7. Feature flag

- **`EPCR_CASE_SOCKET=1`** (or `true`) — enable emits and (later) Cura listener build flag.
- Default **unset / off** in production until signed off.

---

## 8. Test plan (when implemented)

1. Two browsers (or tablet + desktop), users **A** and **B** both in `assignedUsers` for case **X**.
2. **A** saves a section via normal **PUT**; **B** should see merged refresh **faster than 10s** (measure wall time; expect debounce + one GET).
3. Third user **C** not assigned: **no** event delivered (verify with devtools / server logs).
4. Kill socket on **B**: confirm **B** still converges within polling window.

---

## 9. Definition of done (this spike document)

- [x] Commander can see **where** code would live and **what** the feature does in one page.
- [x] Security: no PHI in payload; no broadcast; alignment with **assigned users** (+ privileged rules).
- [x] Clear **spike-only** vs future **implementation PR**.

---

## 10. Implement later checklist (for the promoted PR)

- [ ] Add `epcr_user_<username>` join on JWT `connect` in `create_app.py` (gated by env if desired).
- [ ] Helper `_emit_epcr_case_updated(case_id, updated_at_iso, assigned_usernames, …)` in `medical_records_module` (or small `epcr_realtime.py`) with debounce + flag check.
- [ ] Wire four call sites after commit; privileged recipient list from same rules as GET.
- [ ] Cura: optional module + debounced poll.
- [ ] Tests: unit test debounce + room list; manual two-browser checklist above.
