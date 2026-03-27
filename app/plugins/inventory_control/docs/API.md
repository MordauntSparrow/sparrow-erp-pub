# Inventory Control API Reference

Base path: `/plugin/inventory_control`. Most JSON APIs require authentication (session).

**Lovable / frontend:** See [LOVABLE_PRD_AND_PROMPT.md](./LOVABLE_PRD_AND_PROMPT.md) for a full PRD, endpoint list, settings page (API URL), auth flow, and component-level prompts. Access is controlled by **admin role** (same as the rest of the system; a future core update will refine the permissions system). **Supplier-facing** endpoints accept **Bearer token** (role `supplier`) for external supplier access. Customer-facing features (order tracking, invoices, etc.) are **not** in this module; they will be in a future Sales module.

## OpenAPI

- **GET** `/plugin/inventory_control/openapi.json` — OpenAPI 3.0 spec for this plugin (admin role).

## Dashboard & Health

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/dashboard` | Metrics: total_items, low_stock_count, total_value, expiring_batches_count, recent_transactions_count |

## Items

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/items` | List items (query: skip, limit, search, category, is_active) |
| POST | `/api/items` | Create item (JSON: sku, name, description, barcode, category, unit, reorder_point, reorder_quantity, cost_method, standard_cost, …) |
| GET | `/api/items/<id>` | Get item |
| PUT/PATCH | `/api/items/<id>` | Update item |
| DELETE | `/api/items/<id>` | Archive item (soft) |

## Locations

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/locations` | List locations (query: parent_id) |
| POST | `/api/locations` | Create location (JSON: name, code, type, parent_location_id, address) |
| GET | `/api/locations/<id>` | Get location |
| PUT/PATCH | `/api/locations/<id>` | Update location |

## Batches / Lots

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/batches` | List batches (query: item_id, limit) |
| POST | `/api/batches` | Create batch (JSON: item_id, batch_number, lot_number, expiry_date, …) |
| GET | `/api/batches/<id>` | Get batch |
| PUT/PATCH | `/api/batches/<id>` | Update batch |

## Transactions

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/transactions` | List transactions (query: item_id, location_id, transaction_type, from_date, to_date, skip, limit) |
| POST | `/api/transactions` | Record transaction (JSON: item_id, location_id, quantity, transaction_type, batch_id?, unit_cost?, reference_type?, reference_id?, client_action_id?, **weight?**, **weight_uom?**, **uom?**) |
| POST | `/api/transactions/<id>/rollback` | Rollback transaction (creates compensating movement) |

Transaction types: `in`, `out`, `adjustment`, `transfer`, `count`, `return`, **`repack`**. Transactions and batches support **weight**, **weight_uom**; batches also support **unit_weight**, **unit_weight_uom** for per-unit weight.

## Invoices

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/invoices/upload` | Upload invoice image (form: file, source=amazon); runs OCR and creates invoice + lines |
| GET | `/api/invoices/<id>` | Get invoice and lines |
| PUT/PATCH | `/api/invoices/<id>/lines/<line_id>/match` | Set item_id for line (JSON: item_id) |
| POST | `/api/invoices/<id>/apply` | Apply matched lines to stock (JSON: location_id) |

## Repack / Split

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/repack` | Split one source batch into multiple new batches (JSON: **source_batch_id**, **location_id**, **outputs**: [{ quantity, weight?, weight_uom?, batch_number?, lot_number?, unit_weight?, unit_weight_uom? }, …]). Quantities must not exceed source batch available quantity. |

## Picking (FEFO)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/picking/suggest` | FEFO picking suggestions (query: **item_id**, **location_id**, **quantity**). Returns batches to pick, ordered by expiry soonest first. |

## Mobile API

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/mobile/scan/in` | Stock in (JSON: barcode/sku, location_id, quantity?, **weight?**, **weight_uom?**, **uom?**, client_action_id?) |
| POST | `/api/mobile/scan/out` | Stock out (JSON: barcode/sku, location_id, quantity?, **weight?**, **weight_uom?**, **uom?**, client_action_id?) |
| POST | `/api/mobile/scan/adjust` | Adjustment (JSON: barcode/sku, location_id, quantity, **weight?**, **weight_uom?**, client_action_id?) |
| GET | `/api/mobile/items/search` | Search items (query: q) |
| GET | `/api/mobile/items/<id>/stock` | Stock levels by location for item |
| POST | `/api/mobile/bulk/actions` | Bulk actions (JSON: actions[] with type, barcode/sku, location_id, quantity, **weight?**, **weight_uom?**, **uom?**, client_action_id) |

## Token-based auth (supplier portal only)

Tokens are created by admins via **POST /api/tokens** (JSON: name, role=`supplier`, supplier_id?, scopes?). Only **supplier** role is supported; customer-facing features live in the Sales module. The response includes **token** (raw secret) once; store it securely. Use **Authorization: Bearer &lt;token&gt;** for supplier endpoints.

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/tokens` | Create supplier token (admin). Returns token once. |
| GET | `/api/tokens` | List tokens (admin; no secret) |
| DELETE | `/api/tokens/<id>` | Revoke token (admin) |
| GET | `/api/supplier/items` | Supplier: limited item list (id, sku, name, uom). Token must have supplier_id for session-like use. |
| GET | `/api/supplier/invoices` | Supplier: list invoices (filtered by token supplier_id when set). |
| POST | `/api/supplier/receipts/confirm` | Supplier: confirm receipt — apply invoice to stock (JSON: invoice_id, location_id). Invoice must belong to token’s supplier_id when set. |
| GET | `/api/supplier/batches` | Supplier: view their supplied batches/lots (token supplier_id or query supplier_id for session). |
| GET | `/api/supplier/pos` | Supplier: view PO status (query: status?, limit, skip). Token supplier_id or query for session. |
| GET | `/api/supplier/compliance` | Supplier: list compliance documents (query: document_type?, limit, skip). |
| POST | `/api/supplier/compliance/upload` | Supplier: upload compliance doc (form: file, name?, document_type?). |

## CSV export

All export endpoints require session auth and inventory permission. Use **?format=csv** for CSV download; default is JSON.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/export/items?format=csv` | Export items (query: limit) |
| GET | `/api/export/transactions?format=csv` | Export transactions (query: item_id, location_id, from_date, to_date, limit) |
| GET | `/api/export/stock_levels?format=csv` | Export stock levels (query: item_id, location_id, limit) |
| GET | `/api/export/batches?format=csv` | Export batches (query: item_id, limit) |

## Analytics (stubbed)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/analytics/stock_levels` | Query: item_id, location_id |
| GET | `/api/analytics/movers` | Fast/slow movers |
| GET | `/api/analytics/suppliers` | Supplier performance |

## Data Models (summary)

- **inventory_items** — sku, name, description, barcode, qr_code_data, category, unit, default_location_id, reorder_point, reorder_quantity, is_active, cost_method (FIFO/LIFO/AVG), standard_cost, last_cost, primary_supplier_id, lead_time_days, external_sku, metadata (JSON).
- **inventory_locations** — name, code, type (warehouse/store/bin/virtual), parent_location_id, address, metadata.
- **inventory_batches** — item_id, batch_number, lot_number, expiry_date, manufacture_date, received_date, supplier_id, **weight**, **weight_uom**, **unit_weight**, **unit_weight_uom**, metadata.
- **inventory_stock_levels** — item_id, location_id, batch_id, quantity_on_hand, quantity_reserved, quantity_available, last_transaction_id.
- **inventory_transactions** — item_id, location_id, batch_id, transaction_type, quantity, uom, unit_cost, total_cost, **weight**, **weight_uom**, reference_type, reference_id, performed_by_user_id, performed_at, metadata, reversed_transaction_id, client_action_id.
- **inventory_suppliers** — name, code, contact_info (JSON), default_lead_time_days, rating, metadata.
- **inventory_invoices** — supplier_id, external_source, external_invoice_id, invoice_number, invoice_date, total_amount, currency, status (pending/parsed/validated/applied), raw_file_path, parsed_payload (JSON).
- **inventory_invoice_lines** — invoice_id, item_id, sku, description, quantity, unit_price, line_total, external_item_ref, match_status (matched/ambiguous/unmapped).
- **inventory_audit** — user_id, action, item_id, location_id, batch_id, transaction_id, details (JSON), created_at.
- **inventory_api_tokens** — token_hash, name, role (supplier only in API; customer reserved for Sales), supplier_id, customer_id, scopes (JSON), created_at, last_used_at.
- **inventory_purchase_orders** — supplier_id, order_number, status (draft/sent/confirmed/partially_received/received/closed), ordered_at, expected_date.
- **inventory_supplier_documents** — supplier_id, name, document_type, file_path, uploaded_at, uploaded_by_token_id.
