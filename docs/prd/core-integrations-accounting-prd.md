# PRD: Core Integrations (Xero & FreeAgent) — UK, single-deployment

**Status:** Draft for implementation  
**Owner:** Core platform (not CRM-specific)  
**Audience:** Implementing engineer / “core module” agent  
**Scope geography:** United Kingdom only for v1  
**Deployment model:** One Sparrow deployment = one customer organisation (single-tenant per Docker image / domain). No multi-tenant row isolation required.

---

## 1. Problem statement

Operators need to **originate sales documents in Sparrow** (e.g. accepted quotes) without duplicating **Making Tax Digital (MTD)** behaviour, VAT filing, or “send invoice to client” workflows that belong in accounting software.

Sparrow should:

- Provide a **first-class Integrations** area in **core** admin.
- Support **Xero** and **FreeAgent** as first providers.
- Allow **manual** “create draft invoice” and a **deployment-level rule** to optionally **auto-create** drafts when the customer enables it.
- Surface **connection health** and **re-authorisation** when tokens/consent expire (provider-dependent).

Medical-grade security posture is a **competitive advantage**: tokens and audit logs must meet the same bar as the rest of admin (encryption at rest, least privilege, audit who connected/disconnected).

---

## 2. Goals (v1)

1. **Core route:** `/core/integrations` (GET + POST where needed) listing supported integrations with clear status.
2. **OAuth connect flows** for **Xero** and **FreeAgent** (authorisation code + refresh), per official docs.
3. **Disconnect** and **rotate/reconnect** flows.
4. **Health UI:** connected org name/identifier, last successful API call, last error, and **action required** states.
5. **Consent / token UX:** show **reconnect before** or **token refresh risk** where calculable; always show **Reconnect** when refresh fails or provider returns 401/invalid_grant.
6. **Settings (per deployment):**
   - Default accounting provider (`none` | `xero` | `freeagent`).
   - **Auto-create draft invoice** boolean + **trigger** (single choice for v1, e.g. *CRM quote accepted* — exact hook defined with CRM in a follow-on ticket if not in core).
7. **Audit:** who connected, when, IP/user agent optional, disconnect events.

## 3. Non-goals (v1)

- MTD submission, VAT returns, or replacing Xero/FreeAgent as ledger of record.
- Multi-currency beyond GBP unless trivially free from provider.
- Two-way sync of invoice edits from accounting back into Sparrow line-by-line.
- Purchase **bills** from inventory/timesheets (explicit v2; may stub UI placeholder copy only if desired).

---

## 4. Primary references (must follow)

Implementers must use **official** documentation and keep links in code comments / internal README:

| Provider   | Developer / OAuth docs |
|-----------|-------------------------|
| **Xero**  | https://developer.xero.com/documentation — OAuth 2.0, tenants, scopes, accounting API |
| **FreeAgent** | https://dev.freeagent.com/docs/oauth — OAuth 2.0; API docs under dev.freeagent.com |

**OAuth model:** User/consent-based **Authorization Code** flow for SMB accounting access. **Per-deployment** OAuth client id/secret (environment or core secure config), not a global multi-tenant marketplace app (unless product later standardises on one public app).

---

## 5. User stories

1. As **org admin**, I open **Integrations** and see Xero and FreeAgent cards with Not connected / Connected / Error.
2. As **org admin**, I click **Connect to Xero**, complete OAuth, and return to Sparrow with **Connected** and the **organisation/tenant** name visible.
3. As **org admin**, I can **Disconnect** to revoke Sparrow’s access (and delete tokens locally).
4. As **org admin**, I enable **Automatically create draft invoice** and choose the **trigger** (v1: one trigger only).
5. As **org admin**, I see when I must **Reconnect** (provider refresh failure or documented expiry policy).

---

## 6. UX / IA

### 6.1 Navigation

- Add **Integrations** next to **Settings** in `app/templates/partials/sparrow_admin_top_nav.html` (same gate as core settings: `can_open_org_admin_nav()` or equivalent).
- Target: `url_for('routes.core_integrations')` (new route on core `routes` blueprint).

### 6.2 Page layout (`/core/integrations`)

- **Cards** per provider: logo, short description, status pill, primary CTA (Connect / Reconnect / Disconnect), secondary “View in Xero/FreeAgent” when linked.
- **Global section:** default provider; automation toggle + trigger dropdown; “Save settings”.
- **Activity / diagnostics** (collapsed): last 10 integration events (connect, disconnect, token refresh fail, draft push queued — when invoice phase lands).

### 6.3 “Countdown” / consent copy

- Do **not** promise a universal timer unless product derives it from provider metadata.
- Recommended states: **Healthy**, **Warning** (e.g. refresh token within N days if documented), **Action required** (invalid_grant / refresh failure).
- Always show **Last successful API call** timestamp after any successful call (e.g. lightweight “whoami” or company fetch).

---

## 7. Technical design

### 7.1 Placement in codebase

- **Core** implementation in `app/routes.py` (or small sibling module `app/core_integrations/` imported by `routes.py`) so it is always available without a plugin manifest.
- **Templates** under `app/templates/core/` e.g. `integrations.html` extending the same admin shell as `core_module_settings.html` (reuse top nav + CSRF patterns from `/core/settings`).

### 7.2 Data model (single-tenant)

Suggested table `core_integration_connections` (names illustrative):

| Column | Purpose |
|--------|---------|
| `id` | PK |
| `provider` | `xero` \| `freeagent` |
| `status` | `disconnected` \| `connected` \| `error` |
| `encrypted_tokens` | JSON blob: access, refresh, expiry — **encrypted** (app-level crypto or DB column encryption policy aligned with medical records stance) |
| `provider_org_id` | Xero tenant id / FreeAgent company id |
| `provider_org_label` | Display name |
| `connected_by_user_id` | FK or string |
| `connected_at`, `updated_at` | timestamps |
| `last_error` | text nullable |
| `last_api_success_at` | nullable |

Suggested table `core_integration_settings`:

| Column | Purpose |
|--------|---------|
| `default_provider` | `none` \| `xero` \| `freeagent` |
| `auto_draft_invoice` | bool |
| `auto_draft_trigger` | enum string (v1 single value) |

Alternatively store settings in existing manifest / site JSON **only** if already encrypted and audited — **prefer DB** for clarity and audit.

### 7.3 OAuth callback URLs

- Register redirect URIs in each provider dashboard, e.g.  
  `https://{customer-domain}/core/integrations/oauth/xero/callback`  
  `https://{customer-domain}/core/integrations/oauth/freeagent/callback`
- CSRF **state** parameter mandatory; bind to session.

### 7.4 Scopes (least privilege)

Define minimal scopes per provider for **v1 connect only** (no invoice API until phase 2 if desired):

- Enough to validate connection and read organisation/company identity.

**Phase 1b / 2** (invoice draft — separate PR or same PR if small):

- Xero: Accounting API scopes for **contacts** + **invoices** (draft) — exact scope strings from current Xero docs.
- FreeAgent: scopes for **contacts** + **invoices** per FreeAgent docs.

Document chosen scopes in this repo (`docs/integrations/scopes.md`).

### 7.5 Background work

- Use existing app patterns for **async** or **cron** if present; otherwise synchronous OAuth exchange only in v1, with invoice push queued later.
- **Idempotency** for future invoice push: `external_reference` = Sparrow quote/sale id.

---

## 8. Security & compliance

- **Encrypt** token storage; restrict read to admin roles.
- **Audit log** connect/disconnect and settings changes.
- **Never** log access tokens or refresh tokens.
- **HTTPS only** redirect URIs in production.
- Align with **medical records** policy: Integrations page is **admin-only**; no PHI required on this screen.

---

## 9. Permissions

- Reuse gate used for `/core/settings` (org admin / customer super admin — match existing `can_open_org_admin_nav()` pattern).
- Optional fine permission later: `core.integrations.manage` — **v1** can inherit existing core admin permission to ship faster.

---

## 10. Acceptance criteria (v1)

1. `/core/integrations` loads for authorised users; 403/redirect for others.
2. Xero: Connect → callback → stored tenant + **Connected**; Disconnect clears tokens and status.
3. FreeAgent: same.
4. Settings: default provider + auto-draft toggle persist and reload correctly.
5. Simulated invalid refresh shows **Reconnect** and error message without leaking secrets.
6. Unit tests for: OAuth state validation, token encrypt/decrypt roundtrip (mocked), route auth gate.

---

## 11. Implementation order (suggested)

1. DB migration + models + encrypt helper.
2. Routes: index, OAuth start/callback per provider, disconnect POST.
3. Top nav link + template.
4. Settings section + persistence.
5. (Optional same PR if small) **Invoice draft** job behind manual button on CRM quote — **else** separate PR referencing CRM quote state.

---

## 12. Open questions for product (resolve before invoice phase)

- Exact **trigger** for auto draft: CRM quote `accepted` vs opportunity `won` vs explicit button only.
- Whether **one** connected provider is allowed at a time or both connected with a **default for sales**.

---

## 13. Handoff note for “CRM agent”

CRM changes (manual **Create draft invoice** on quote, idempotency flags on `crm_quotes`) should consume **core** integration service:

- `core.integrations.get_active_provider()`
- `core.integrations.push_draft_invoice(sale_ref)` (future)

Do **not** duplicate OAuth in CRM plugin.

---

*End of PRD*
