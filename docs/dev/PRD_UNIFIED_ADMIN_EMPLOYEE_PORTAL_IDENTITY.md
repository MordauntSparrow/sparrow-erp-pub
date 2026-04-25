# PRD: Unified admin & employee portal identity

**Status:** Draft for implementation  
**Owner:** Product / Engineering  
**Repo context:** Sparrow ERP (`users`, `tb_contractors`, employee portal plugin, seat limits)

---

## 1. Summary

Today, **core admin accounts** live in `users`, while **employee portal** identity and most portal data are keyed to **`tb_contractors`** (`contractor_id`). Admins who need to use the employee portal (e.g. VDI, shift work from the field) must either have a separate contractor record or cannot use portal features cleanly.

This feature **allows the same admin credentials** to sign into **both** the admin Flask application and the **employee portal** (separate subdomain/port is acceptable). **Employee-facing data** (todos, portal modules, FK-heavy tables) **stays on the contractor side**; **`users` links to that row** via `contractor_id` (resolved by email or a minimal stub), **without duplicating** main employee/profile fields on `users`.

---

## 2. Problem statement

- Employee portal authentication and session setup are **contractor-centric** (`find_tb_contractors_for_portal_login`, many tables reference `tb_contractors.id`).
- **Admins** exist only in `users`; they are not first-class portal identities unless a matching contractor row exists.
- Product expectation: **one person, one logical employee experience** in the portal whether they are “really” a contractor row or an admin user, **without** maintaining two copies of the same business data.

---

## 3. Goals

1. **Dual credential support on employee portal:** On sign-in, accept **both** `users` (username/email + password) **and** `tb_contractors` (existing rules), using the **same password hashing** (`AuthManager` / existing hash format).
2. **Admin uses both apps:** Users with admin (or other core) roles can open **admin app** and **employee portal** with the **same password** (deployment assumption: separate host/port/subdomain; **session collision between apps is explicitly out of scope**).
3. **Single source of truth for employee/portal data:** **Todos, portal settings, and other portal-scoped fields** remain stored **against `contractor_id`** (and related tables). **`users` does not duplicate** name, employment, HR profile, portal todos, etc.
4. **Stable link user → contractor:** `users.contractor_id` (nullable FK to `tb_contractors`) populated by **email match** or by creating a **minimal contractor stub** when an admin first needs portal access, so existing FKs keep working.
5. **Seat / plan behaviour unchanged in spirit:** Billable **people** are counted as **deduplicated across `users` and `tb_contractors` by email** (existing product direction). **Same email must not consume two seats** because they have both a user row and a contractor row.

---

## 4. Non-goals

- **Replacing `tb_contractors`** or migrating all portal tables to `user_id` in v1 (optional later phase).
- **Designing unified session cookies** across subdomains (assumed separate; no requirement to share Flask session).
- **Changing core admin login** to require contractor row (admin app continues to authenticate against `users` as today).
- **Guaranteeing** password sync between `users.password_hash` and `tb_contractors.password_hash` for linked rows unless explicitly specified (see §7.4 — recommend **single password** updates both when linked).

---

## 5. Current state (baseline)

- **`users`:** Core Sparrow accounts (username, email, `password_hash`, role, permissions, PIN fields, support-access flags, etc.).
- **`tb_contractors`:** Employee / contractor identity for time billing, HR, recruitment; **portal login** today; columns include email, username, name, `password_hash`, status, etc.
- **Employee portal:** Login resolves contractor via `find_tb_contractors_for_portal_login` (`app/objects.py`); plugin tables use **`contractor_id` → `tb_contractors`**.
- **Seat limits:** DB/env-driven cap; counting logic dedupes by email across active `users` and `tb_contractors` (align implementation with `app/seat_limits.py` and related hooks).

---

## 6. Proposed architecture

### 6.1 Canonical data

| Concern | Canonical store | Notes |
|--------|------------------|--------|
| Admin login, Sparrow roles, permissions | `users` | Unchanged for admin app. |
| Employee portal-facing rows (todos, equipment, themes on contractor, plugin FKs) | `tb_contractors` + existing satellite tables | **No move** in v1. |
| Link between same person | `users.contractor_id` (nullable) | **One direction** recommended; avoid bidirectional FK unless needed. |

**Rejected for v1:** Duplicating full contractor column set onto `users` (avoids drift and double maintenance).

### 6.2 Portal “principal” (implementation concept)

After authentication, employee portal code should resolve a single **portal principal**:

- `principal_source`: `contractor_direct` | `user_linked`
- `user_id` (UUID string, optional)
- `contractor_id` (int, **required for all portal DB writes** that FK contractor)
- `email` (for display and dedupe)
- Display fields (name, initials, avatar) resolved from **contractor row** when linked

**Rule:** All portal modules that today assume “logged-in contractor” must use this principal (or a small service API), **not** raw session assumptions that cannot represent a `users`-originated login.

### 6.3 Linking algorithm (post-login or lazy)

When login succeeds **via `users`**:

1. If `users.contractor_id` **is set** → use it (validate still exists).
2. Else **lookup** `tb_contractors` by **normalized email** match to `users.email`.
   - **0 rows** → **create minimal stub** `tb_contractors` row (email aligned with user; username allocation per existing helpers; default status per product rules).
   - **1 row** → set `users.contractor_id` and continue.
   - **2+ rows** → **fail closed** with a clear admin-facing error (data integrity); do not guess.

When login succeeds **via contractor** (existing path): `contractor_id` known; optional future: set `users.contractor_id` if a `users` row exists with same email (idempotency).

---

## 7. Authentication & security

### 7.1 Employee portal sign-in

- **Step A:** Attempt match on **`users`** (case-insensitive username or email per product parity with contractor rules).
- **Step B:** Verify password with **`AuthManager`** against `users.password_hash`.
- **Step C:** Resolve `contractor_id` per §6.3.
- **Else:** Existing contractor-only path (`find_tb_contractors_for_portal_login` + verify against `tb_contractors.password_hash`).

**Order policy:** Must be **documented** (e.g. prefer `users` match when both could apply, or prefer contractor — team choice; recommend **users first** for admin-led adoption, then contractor).

### 7.2 Same hash system

- Both tables continue using the **same hashing utilities**; no second algorithm.

### 7.3 Admin app

- **No change required** for core admin login in v1 beyond any **schema** addition (`contractor_id` on `users`).

### 7.4 Password changes (recommendation)

- **If `users` and contractor are linked** (same person): changing password in **either** admin “user password” UI or “portal password” UI should **update both hashes** OR the product **blocks** split passwords. **PRD default:** **keep one password** — update both when linked.

---

## 8. Seats, plans, and entitlements

- **Seat counting:** Continue **deduping by email** across billable `users` and active `tb_contractors` so linking **does not** double-count.
- **Admin vs contractor (entitlement):**
  - **Admin `users` row** grants **admin app** access.
  - **Employee portal** access for that person uses **linked `contractor_id`** for data; login may be via `users` credentials.
  - **“Advanced” / plan features** should attach to **the person (email / linked identity)** where possible, not to “which table they used to log in,” to avoid inconsistent behaviour.

---

## 9. Employee portal module compatibility

### 9.1 Requirement

**There must be no functional difference** in portal features when the session was established from **`users`** vs **`tb_contractors`**, after principal resolution.

### 9.2 Implementation implications

- Replace or wrap **direct `contractor_id` from legacy session** with **principal.contractor_id** everywhere portal behaviour depends on “current employee.”
- **New code** must not branch on “contractor login only” except during the narrow auth resolution phase.

### 9.3 Data location

- **Todos, portal-specific fields, plugin tables** remain **`contractor_id`-scoped** as today.
- **`users`** holds **link** + **admin-specific** attributes only.

---

## 10. Schema & migrations

### 10.1 `users` table

- Add **`contractor_id`** `INT NULL`, **unique** where non-null (optional but avoids one contractor claimed by two users).
- **FK** to `tb_contractors(id)` **ON DELETE SET NULL** (or restrict — product decision if deleting contractor should clear link).

### 10.2 Stub contractor (minimal row)

- Fields: at minimum whatever **NOT NULL** constraints require plus **email**, **username** (via existing allocation), **password_hash** optional policy (could mirror user hash at link time, or rely on user-only login — **must be decided** so contractor-based APIs still behave).

### 10.3 Backfill (optional script)

- For existing **`users`** rows with matching **contractor email**, set `contractor_id`.
- Report ambiguous emails for manual cleanup.

---

## 11. Rollout plan

| Phase | Description |
|-------|-------------|
| **1** | Schema: `users.contractor_id` + migration + indexes/FK. |
| **2** | Central resolver: email match, stub create, ambiguity handling. |
| **3** | Employee portal auth: dual lookup + principal in session. |
| **4** | Refactor portal routes/services to use principal (grep-driven audit for `contractor_id` / session assumptions). |
| **5** | Password sync policy when linked (if adopted). |
| **6** | QA: admin-only user, contractor-only user, linked user, ambiguous email, seat count regression. |

---

## 12. Acceptance criteria

1. Admin with **`users`** account and **no** prior contractor row can **log into employee portal**; after first successful login, a **contractor row exists** (stub or matched) and **`users.contractor_id` is set**.
2. Existing **contractor-only** portal login **unchanged** for end users.
3. **Todos / portal modules** behave identically whether login path was user or contractor, given successful link.
4. **Seat usage** for one email with both `users` and `tb_contractors` does **not** increase vs deduped policy before the feature.
5. **Ambiguous email** (multiple contractors) does **not** silently pick a row.
6. **No duplicated** employee profile fields required on `users` for portal to function (link + contractor row is sufficient).

---

## 13. Risks & mitigations

| Risk | Mitigation |
|------|------------|
| Hidden `contractor_id` assumptions in plugins | Systematic audit + principal helper; integration tests on hot paths (VDI, equipment, messages). |
| Email drift between `users` and contractor | Document change workflow; optional **admin “re-link”** tool later. |
| Split passwords for linked identity | PRD default: single password updates both hashes. |
| Stub contractor side effects (HR, payroll) | Minimal defaults; status **inactive** for payroll if needed until HR completes profile. |

---

## 14. Open questions (resolve before build)

1. **Stub contractor:** minimum required fields and **default `status`** (e.g. active vs pending) for payroll/HR safety.
2. **Login precedence** when email could match **both** tables with **different passwords** (should be rare if sync policy applies).
3. Whether **`tb_contractors.user_id` back-link** is useful for reporting vs **only** `users.contractor_id`.

---

## 15. Document history

- **2026-03-31:** Initial PRD from stakeholder discussion (unified login, link via email/`contractor_id`, no duplicate portal data, seat dedupe, portal module parity).
