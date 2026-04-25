# Accounting integration OAuth (Sparrow `core_integrations`)

Official references:

- **Xero:** [OAuth 2.0 auth flow](https://developer.xero.com/documentation/guides/oauth2/auth-flow), [Scopes](https://developer.xero.com/documentation/guides/oauth2/scopes)
- **FreeAgent:** [OAuth 2.0](https://dev.freeagent.com/docs/oauth)
- **QuickBooks Online:** [Intuit OAuth 2.0](https://developer.intuit.com/app/developer/qbo/docs/develop/authentication-and-authorization/oauth-2.0), [Scopes](https://developer.intuit.com/app/developer/qbo/docs/learn/scopes)
- **Sage Accounting:** [OAuth 2.0](https://developer.sage.com/accounting/guides/authentication/oauth/)
- **FreshBooks:** [Authentication](https://www.freshbooks.com/api/authentication), [Scopes](https://www.freshbooks.com/api/scopes)

## Module manifest (`accounting_integration_consumer`)

Enabled plugins may add **`accounting_integration_consumer`** in `manifest.json` (object or list of objects). The core **Integrations** page then shows a card per entry with:

- **`summary`** — short text.
- **`provider_ids`** — optional list of provider slugs (`xero`, `freeagent`, …).
- **`permission_ids`** — optional list; labels are taken from User Management’s permission catalog, then from the plugin’s **`declared_permissions`**.
- **`options`** — optional rows with `label` plus one of: `value` (static), `integration_setting` (`default_provider` / `auto_draft_invoice` / `auto_draft_trigger`), `core_manifest_key` (dotted path), or `plugin_settings_key` (under manifest `settings`, unwraps `{ "value": … }`).

Implementation: `app/core_integrations/consumers.py`.

For **Xero** and **FreeAgent**, OAuth client ID and secret can be stored encrypted in
`core_integration_oauth_clients` (saved from **Integrations**) instead of only in the
environment. When both `XERO_CLIENT_ID` and `XERO_CLIENT_SECRET` are set in the environment,
those values are used and the database pair is ignored.

## Environment variables (Railway / `.env`)

| Variable | Provider |
|----------|-----------|
| `XERO_CLIENT_ID`, `XERO_CLIENT_SECRET` | Xero |
| `FREEAGENT_CLIENT_ID`, `FREEAGENT_CLIENT_SECRET` | FreeAgent |
| `FREEAGENT_API_BASE` (optional) | FreeAgent sandbox host |
| `QUICKBOOKS_CLIENT_ID`, `QUICKBOOKS_CLIENT_SECRET` or `INTUIT_CLIENT_ID`, `INTUIT_CLIENT_SECRET` | QuickBooks Online |
| `INTUIT_ENVIRONMENT=sandbox` | QuickBooks API host + sandbox keys |
| `SAGE_CLIENT_ID`, `SAGE_CLIENT_SECRET` | Sage Accounting |
| `FRESHBOOKS_CLIENT_ID`, `FRESHBOOKS_CLIENT_SECRET` | FreshBooks |
| `FRESHBOOKS_OAUTH_SCOPE` (optional) | FreshBooks authorize scope (default `user:profile:read`) |

## Xero

Used in `app/core_integrations/service.py` as `XERO_CONNECT_SCOPES`:

| Scope | Rationale |
|-------|-----------|
| `openid` | OpenID Connect baseline |
| `offline_access` | Refresh tokens |
| `profile` | Profile claims |
| `email` | Email claim |
| `accounting.settings` | Organisation / settings context |
| `accounting.contacts` | Create and match contacts on draft invoices |
| `accounting.transactions` | Create draft sales invoices (ACCREC) and line items |

**Environment (invoice lines):** optional `XERO_DEFAULT_ACCOUNT_CODE` (chart account code per org, default `200`) and optional `XERO_DEFAULT_TAX_TYPE` (e.g. regional tax type; omit to rely on account defaults where allowed).

After scope changes, existing Xero connections should **disconnect and reconnect** on Integrations so the new authorisation includes invoice permissions.

**Token:** `https://identity.xero.com/connect/token`  
**Authorisation:** `https://login.xero.com/identity/connect/authorize`  
**Connections:** `https://api.xero.com/connections`

## FreeAgent

- **Authorisation:** `GET {FREEAGENT_API_BASE}/v2/approve_app` …
- **Token:** `POST {FREEAGENT_API_BASE}/v2/token_endpoint` with HTTP Basic.

Company: `GET /v2/company`.

## QuickBooks Online (Intuit)

Sparrow uses **authorisation code** flow; Intuit redirects with **`realmId`** (QuickBooks company id). That value is stored as `provider_org_id` and in the encrypted token blob as `realm_id`.

- **Authorisation:** `https://appcenter.intuit.com/connect/oauth2`
- **Token:** `https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer`
- **Scopes (v1 connect):** `com.intuit.quickbooks.accounting offline_access` (`INTUIT_QB_SCOPES` in `providers.py`)
- **Company label:** `GET {quickbooks.api.intuit.com|sandbox-quickbooks.api.intuit.com}/v3/company/{realmId}/companyinfo/{realmId}`

## Sage (Business Cloud Accounting, API 3.1)

- **Authorisation:** `https://www.sageone.com/oauth2/auth/central` with `filter=apiv3.1`, `response_type=code`, `client_id`, `redirect_uri`, `scope` (default `full_access`), `state`.
- **Token:** `POST https://oauth.accounting.sage.com/token` with HTTP Basic.
- **Health / label:** `GET https://api.accounting.sage.com/v3.1/businesses` (first business used for display).

## FreshBooks

- **Authorisation:** `https://auth.freshbooks.com/oauth/authorize`
- **Token:** `POST https://api.freshbooks.com/auth/oauth/token` with **JSON** body (`grant_type`, `client_id`, `client_secret`, `code` or `refresh_token`, `redirect_uri` on refresh).
- **Profile:** `GET https://api.freshbooks.com/auth/api/v1/users/me`

When tightening scopes for production, update `FRESHBOOKS_OAUTH_SCOPE` and the FreshBooks app configuration together.
