# Inventory Control App — Lovable PRD & Prompt Guide

This document is a **Product Requirements Document (PRD)** and **prompt playbook** for building the Inventory Control app in Lovable. It follows Lovable’s best practices: plan before you prompt, build by component, use real content, and apply design buzzwords. Use it as a single source of truth for scope, API, settings, and auth—and as a base for copy‑paste or Plan-mode prompts.

---

## Phase 1: Foundation

### 1. Plan before you prompt

**What is this product?**  
A warehouse and inventory management app for Sparrow ERP. It handles items, locations, batches, stock movements, invoices (with PDF/DOCX/image scan), analytics, and exports. It is one module inside a larger ERP; users are already logged in via the core app.

**Who is it for?**  
Warehouse staff and admins who need to receive stock, pick and ship, adjust counts, run reports, and match/apply supplier invoices. Optional: suppliers (token-based) viewing their invoices and batches.

**Why will they use it?**  
To keep stock accurate, see what’s where, reorder in time, and reconcile with supplier invoices—all in one place, with a clear path back to the main ERP dashboard.

**One key action:**  
“See current stock and record or correct movements quickly.”

**Starter style (reuse in every section):**  
*Use a clean, professional ERP aesthetic. Neutral palette (slate/gray with a single accent), clear hierarchy, dense but scannable tables, and obvious primary actions. Tone: calm and efficient, not playful. Prefer cards with soft shadows and rounded corners (8px). Use a system font stack (e.g. Inter or Segoe UI).*

---

## 2. User journey (high level)

1. **Landing** → User arrives from core dashboard (or direct link). If not logged in → redirect to core login, then back.
2. **Trust** → Dashboard shows health, KPIs, AI summary, and charts (value by category, movements by type, activity).
3. **Act** → Nav to Items, Transactions, Batches, Invoices, Analytics, or Settings. Each area has list + add/edit + (where relevant) bulk or export.
4. **Exit** → Brand/logo in header links back to core dashboard; “Dashboard” nav item does the same.

---

## 3. Authentication (core-module flow)

The app **does not implement its own login**. It follows the same pattern as other ERP modules (e.g. Ventus):

- **Session-based:** User must be logged in via the **core app** (`/login`). The core sets a session cookie after successful login.
- **All API requests:**  
  - Send **cookies** with every request (`credentials: 'include'` or `same-origin` when the Lovable app is served from the same origin as the API).  
  - For **non-GET** requests (POST, PUT, PATCH, DELETE), send **CSRF token** in header: `X-CSRFToken: <value>`. The value is read from a cookie (e.g. `csrf_token` or `csrf`) set by the core app.
- **When unauthenticated:**  
  - If the API returns 401/403 or a redirect to login, the frontend should **redirect the user to the core login page** (e.g. `/login`).  
  - After login, the core typically redirects to dashboard; the user can then open Inventory again.
- **No sign-up in this app:** Users are created in the core; this app only consumes the existing session.

**Prompt snippet for auth:**  
*“If any API call returns 401 or 403, redirect the user to `/login` (core app login). Assume the user is already logged in via the core; send cookies with all requests and send the CSRF token from the cookie in the `X-CSRFToken` header for POST/PUT/PATCH/DELETE.”*

---

## 4. Settings page (required)

- **Purpose:** Let the user set the **API base URL** the app uses for all requests.
- **Default value (prefilled):**  
  - If the app is embedded or same-origin: `"/plugin/inventory_control"` (relative).  
  - If the app is deployed separately: full URL, e.g. `"https://your-erp.example.com/plugin/inventory_control"`.
- **Fields:**  
  - **API base URL** (text input): no trailing slash. Examples: `https://your-erp.example.com/plugin/inventory_control` or `/plugin/inventory_control`.  
  - Optional: “Test connection” button that calls `GET {baseUrl}/api/health` and shows success/failure.
- **Persistence:** Store in `localStorage` (e.g. key `inventory_control_api_base`) so it survives refresh.  
- **Usage:** Every API call in the app must use this base (e.g. `fetch(\`${apiBase}/api/items\`, ...)`).

**Prompt snippet for Settings:**  
*“Add a Settings page with a single text input: ‘API base URL’, prefilled with `/plugin/inventory_control`. Save to localStorage and use this base for all API requests. Include a ‘Test connection’ button that calls GET {baseUrl}/api/health and shows a success or error message.”*

---

## 5. Full API endpoint reference

Base path is **configurable** (see Settings). Below, `{base}` = that base (e.g. `/plugin/inventory_control`). All admin endpoints require **session auth** (cookies + CSRF for non-GET). Optional **Bearer token** is only for supplier-facing endpoints (not required for the main Lovable UI).

### Health & dashboard

| Method | Path | Description |
|--------|------|-------------|
| GET | `{base}/api/health` | Health check (e.g. status, db). |
| GET | `{base}/api/dashboard` | Metrics: total_items, low_stock_count, total_value, expiring_batches_count, recent_transactions_count, value_by_category, movements_by_type, movements_by_day, movement_summary (AI text). |

### Categories

| Method | Path | Description |
|--------|------|-------------|
| GET | `{base}/api/categories` | List (query: parent_id). |
| POST | `{base}/api/categories` | Create (JSON: name, code?, description?, parent_id?, sort_order?). |
| GET | `{base}/api/categories/<id>` | Get one. |
| PUT/PATCH | `{base}/api/categories/<id>` | Update. |
| DELETE | `{base}/api/categories/<id>` | Delete (items’ category_id set to null). |

### Items

| Method | Path | Description |
|--------|------|-------------|
| GET | `{base}/api/items` | List (query: skip, limit, search, category, category_id, is_active). Returns items with category_name. |
| POST | `{base}/api/items` | Create (JSON: sku, name, description?, barcode?, category_id?, unit?, reorder_point?, reorder_quantity?, standard_cost?, cost_method?, is_active?, …). |
| GET | `{base}/api/items/<id>` | Get one. |
| PUT/PATCH | `{base}/api/items/<id>` | Update. |
| DELETE | `{base}/api/items/<id>` | Archive (soft). |

### Locations

| Method | Path | Description |
|--------|------|-------------|
| GET | `{base}/api/locations` | List (query: parent_id). |
| POST | `{base}/api/locations` | Create (JSON: code, name, type?, parent_location_id?, address?). |
| GET | `{base}/api/locations/<id>` | Get one. |
| PUT/PATCH | `{base}/api/locations/<id>` | Update. |

### Batches

| Method | Path | Description |
|--------|------|-------------|
| GET | `{base}/api/batches` | List (query: item_id, supplier_id?, limit, skip). |
| POST | `{base}/api/batches` | Create (JSON: item_id, batch_number?, lot_number?, quantity?, expiry_date?, …). |
| GET | `{base}/api/batches/<id>` | Get one. |
| PUT/PATCH | `{base}/api/batches/<id>` | Update. |

### Transactions

| Method | Path | Description |
|--------|------|-------------|
| GET | `{base}/api/transactions` | List (query: item_id, location_id, transaction_type, from_date, to_date, skip, limit). |
| POST | `{base}/api/transactions` | Create (JSON: item_id, location_id, quantity, transaction_type, batch_id?, unit_cost?, reference_type?, reference_id?, weight?, weight_uom?, uom?). Types: in, out, adjustment, transfer, count, return, repack. |
| POST | `{base}/api/transactions/<id>/rollback` | Rollback (creates compensating movement). |

### Invoices

| Method | Path | Description |
|--------|------|-------------|
| GET | `{base}/api/invoices` | List (query: limit, skip, status?). |
| POST | `{base}/api/invoices/upload` | Upload file (form: file, source?). Accepts image, PDF, DOCX. |
| GET | `{base}/api/invoices/<id>` | Get invoice + lines. |
| PUT/PATCH | `{base}/api/invoices/<id>/lines/<line_id>/match` | Set item_id for line (JSON: item_id). |
| POST | `{base}/api/invoices/<id>/apply` | Apply to stock (JSON: location_id). |

### Repack / picking

| Method | Path | Description |
|--------|------|-------------|
| POST | `{base}/api/repack` | Split batch (JSON: source_batch_id, location_id, outputs: [{ quantity, batch_number?, lot_number?, … }]). |
| GET | `{base}/api/picking/suggest` | FEFO suggestions (query: item_id, location_id, quantity). |

### Analytics

| Method | Path | Description |
|--------|------|-------------|
| GET | `{base}/api/analytics/stock_levels` | Report (query: item_id?, location_id?, limit?, skip?). Returns report[] with item_sku, item_name, location_code, location_name, quantity_on_hand, quantity_reserved, quantity_available. |
| GET | `{base}/api/analytics/movers` | Fast/slow movers (query: days?, top?). Returns fast_movers[], slow_movers[] (sku, name, tx_count, qty_in, qty_out). |
| GET | `{base}/api/analytics/activity` | Recent activity (query: days?, limit?). Returns activity[] (performed_at, transaction_type, item_sku, item_name, location_code, quantity, …). |
| GET | `{base}/api/analytics/suppliers` | Suppliers list. |

### Export (CSV/JSON)

| Method | Path | Description |
|--------|------|-------------|
| GET | `{base}/api/export/items` | Query: format=csv|json, limit. |
| GET | `{base}/api/export/transactions` | Query: format, item_id, location_id, from_date, to_date, limit. |
| GET | `{base}/api/export/stock_levels` | Query: format, item_id, location_id, limit. |
| GET | `{base}/api/export/batches` | Query: format, item_id, limit. |

### Mobile (optional for Lovable UI)

| Method | Path | Description |
|--------|------|-------------|
| POST | `{base}/api/mobile/scan/in` | Scan in (JSON: barcode or sku, location_id, quantity?, …). |
| POST | `{base}/api/mobile/scan/out` | Scan out. |
| POST | `{base}/api/mobile/scan/adjust` | Adjust. |
| GET | `{base}/api/mobile/items/search` | Query: q. |
| GET | `{base}/api/mobile/items/<id>/stock` | Stock by location. |
| POST | `{base}/api/mobile/bulk/actions` | Bulk (JSON: actions[]). |

### Tokens (admin)

| Method | Path | Description |
|--------|------|-------------|
| POST | `{base}/api/tokens` | Create (JSON: name, role=supplier, supplier_id?, scopes?). Returns token once. |
| GET | `{base}/api/tokens` | List (no secret). |
| DELETE | `{base}/api/tokens/<id>` | Revoke. |

Supplier-only endpoints (Bearer token) are omitted here; the main Lovable app uses session auth only.

---

## 6. Features to support (checklist)

Use this as the scope list; build by **component** (one area at a time).

- [ ] **Settings** — API base URL (prefilled `/plugin/inventory_control`), save to localStorage, optional Test connection.
- [ ] **Auth handling** — Redirect to `/login` on 401/403; send cookies + X-CSRFToken for non-GET.
- [ ] **Layout** — Header with brand/logo linking to core dashboard; nav: Dashboard (core), Inventory (this app), Theme, Logout; optional sidebar or tabs for Inventory areas.
- [ ] **Dashboard** — Cards for total value, active items, low stock, expiring (30d), tx (7d); AI summary block; charts: value by category (doughnut), movements by type (bar), activity last 7 days (line).
- [ ] **Items** — List (search, filter by category); add/edit modal (SKU, name, description, barcode, **category select**, unit, reorder point/qty, cost, cost method, active); archive; category filter dropdown.
- [ ] **Categories** — List; add/edit (name, code, description, sort order); delete (with clear warning).
- [ ] **Locations** — List; add/edit (code, name, type, parent, address).
- [ ] **Batches** — List (filter by item); add/edit; link to repack.
- [ ] **Transactions** — List (filters: item, location, type, date range); add (item, location, quantity, type, batch?, unit cost?, ref); rollback action.
- [ ] **Invoices** — List; upload (file: image/PDF/DOCX); view invoice + lines; match line to item; apply to stock (select location).
- [ ] **Repack** — Form: source batch, location, outputs (quantity, batch/lot numbers); submit to POST /api/repack.
- [ ] **Analytics** — Tabs: Stock levels (table + Download CSV), Activity (table + date range + Download CSV), Fast/slow movers (two tables + Download CSV).
- [ ] **Exports** — Links or buttons to export items, transactions, stock levels, batches (CSV) using export endpoints.

---

## 7. Build by component (Lovable prompts)

Build **one section at a time**. Use the starter style and real copy below in each prompt.

### Dashboard

*“Create a dashboard section with: (1) a row of KPI cards: Total value, Active items, Low stock, Expiring (30d), Transactions (7d). (2) A single ‘Inventory summary’ card showing a short AI-generated summary text (fetch from API). (3) Three chart placeholders in a row: Stock value by category (doughnut), Movements by type (bar), Activity last 7 days (line). Use the dashboard API to populate; show loading states. Style: calm, professional ERP; cards with soft shadows and 8px radius.”*

### Items list + form

*“Create an Items page: a data table with columns SKU, Name, Category, UOM, Reorder, Cost, Active, and actions (Edit, Archive). Include a search input and a category filter dropdown. Add a floating or modal form for Add/Edit with fields: SKU, Name, Description, Barcode, Category (dropdown from API), Unit, Reorder point, Reorder qty, Standard cost, Cost method, Active. Use real labels and placeholders (e.g. ‘Reorder point’, ‘ea, kg’). Primary button: Save. Style: clean tables, minimal and professional.”*

### Categories

*“Create a Categories page: table with Name, Code, Description, Sort order, and Edit/Delete. Add/Edit modal: Name, Code, Description, Sort order. Delete must confirm. Style: consistent with Items—cards, 8px radius, professional.”*

### Settings

*“Create a Settings page with one input: ‘API base URL’, default value `/plugin/inventory_control`. A ‘Save’ button stores it in localStorage. A ‘Test connection’ button calls GET {baseUrl}/api/health and shows success or error in a small message below. Use a simple card layout and clear label.”*

### Analytics

*“Create an Analytics page with three tabs: Stock levels (table with Item SKU, Item name, Location, On hand, Reserved, Available; Refresh + Download CSV), Activity (table with Date, Type, Item, Location, Qty, Ref; dropdown for 7/14/30 days; Refresh + Download CSV), Fast/slow movers (two tables side by side: Fast movers and Slow movers with SKU, Name, Tx count, Qty in, Qty out; days dropdown; Download CSV). Load data when tab is selected. Style: professional, dense but readable tables.”*

---

## 8. Design buzzwords (use in every prompt)

- **Overall:** Calm, professional, ERP, minimal, clear hierarchy.  
- **Components:** Soft shadows, rounded corners (8px), card-based layout, scannable tables.  
- **Actions:** Primary button for main action (e.g. Save, Apply); secondary for Cancel or back.  
- **States:** Always show loading and empty states; error state with retry or message.  
- **Typography:** System font (Inter or Segoe UI), clear labels, no decorative fonts.

---

## 9. Prompt closer (Lovable best practice)

After pasting or adapting any section from this doc, add:

*“Ask me any questions you need in order to fully understand what I want from this feature and how I envision it.”*

Use **Plan mode** when you want Lovable to ask clarifying questions before implementing.

---

## 10. Version and scope

- **API base path:** `/plugin/inventory_control` (configurable in Settings).  
- **Auth:** Core app session (cookies + CSRF). Redirect to `/login` when not authenticated.  
- **OpenAPI:** `GET {base}/openapi.json` returns the full API spec (admin only).

This PRD and prompt guide is the single source for features, endpoints, settings, and auth for the Inventory Control Lovable app.
