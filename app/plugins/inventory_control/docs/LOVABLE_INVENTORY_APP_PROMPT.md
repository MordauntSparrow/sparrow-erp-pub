# Lovable prompt: Bring the Inventory Control app up to date with the backend (equipment, sign-out, usage)

Use this prompt in Lovable to update the tablet/mobile or web app so it matches the current Inventory Control backend API.

---

## Base URL and auth

- **Base URL:** `https://<your-server>/plugin/inventory_control`
- **Auth:** Send session JWT in the header for all API requests:
  - `Authorization: Bearer <token>`
  - The token is returned by the main app login at `POST /api/login` in the response field `token`. If `token` is null, check `hint` and ensure the server has PyJWT installed.
- All endpoints below are relative to the base (e.g. `/api/dashboard` → `GET <base>/api/dashboard`).

---

## Endpoints and payloads

### Health and dashboard

**GET /api/health**  
- Response 200: `{ "status": "ok", "user": { "id", "username", "role" } }` (user present when authenticated).

**GET /api/dashboard**  
- Response 200: Dashboard metrics object (e.g. `total_items`, `low_stock_count`, `total_value`, `value_by_category`, `movements_by_type`, `movements_by_day`, `movement_summary`).

---

### People search (for “signed out to” / assignee)

**GET /api/people/search?q=<query>&limit=20**  
- Query: `q` (required, search string), `limit` (optional, default 20, max 50).
- Response 200: `{ "people": [ { "type": "user" | "contractor", "id": string, "label": string, "username"?: string, "email"?: string } ] }`.
- Use this to populate a “Signed out to” / assignee picker when recording transactions.

---

### Items (equipment flags and lead time)

**GET /api/items**  
- Query: optional `search`, `category_id`, `is_active`, `skip`, `limit`.
- Response 200: `{ "items": [ ... ] }`. Each item can include: `id`, `sku`, `name`, `description`, `unit`, `reorder_point`, `reorder_quantity`, `is_equipment`, `requires_serial`, `lead_time_days`, `category_id`, `is_active`, etc.

**GET /api/items/<item_id>**  
- Response 200: Single item object (same fields as above).

**POST /api/items**  
- Body: `{ "sku", "name", "description"?, "unit"?, "reorder_point"?, "reorder_quantity"?, "is_equipment"?: boolean, "requires_serial"?: boolean, "lead_time_days"?: number, "category_id"?, ... }`.
- Response 201: `{ "id": <item_id> }` or item object.

**PUT /api/items/<item_id>**  
- Body: Same fields as POST (partial update). Include `is_equipment`, `requires_serial`, `lead_time_days` when editing equipment items.

---

### Equipment assets (serialised units)

**GET /api/equipment/assets**  
- Query: `item_id` (optional), `status` (optional, e.g. `in_stock`, `loaned`, `assigned`, `maintenance`, `retired`, `lost`), `search` (optional, filters by serial).
- Response 200: `{ "assets": [ { "id", "item_id", "serial_number", "status", "make", "model", "purchase_date", "warranty_expiry", "service_interval_days", "condition", "metadata", "created_at", "updated_at" } ] }`.

**POST /api/equipment/assets**  
- Body: `{ "item_id": number, "serial_number": string, "make"?, "model"?, "purchase_date"?, "warranty_expiry"?, "service_interval_days"?, "condition"?, "metadata"?: object }`.
- Response 201: `{ "id": <asset_id> }`.
- Errors: 400 if `item_id` or `serial_number` missing or invalid; 404 if item not found.

**PATCH /api/equipment/assets/<asset_id>**  
- Body: Any of `make`, `model`, `purchase_date`, `warranty_expiry`, `service_interval_days`, `condition`, `status`, `metadata` (only send fields to update).
- Response 200: `{ "ok": true }`.
- Errors: 404 if asset not found; 400 on validation error.

---

### Transactions (sign-out / sign-in, loan, equipment)

**GET /api/transactions**  
- Query: `item_id`, `location_id`, `transaction_type`, `from_date`, `to_date`, `skip`, `limit` (all optional).
- Response 200: `{ "transactions": [ { "id", "item_id", "location_id", "batch_id", "transaction_type", "quantity", "uom", "unit_cost", "total_cost", "reference_type", "reference_id", "performed_by_user_id", "assignee_type", "assignee_id", "assignee_label", "is_loan", "due_back_date", "equipment_asset_id", "performed_at", "metadata", "weight", "weight_uom", ... } ] }`.
- Display assignee and loan/equipment info in transaction lists and detail views.

**POST /api/transactions**  
- Body:
  - Required: `item_id`, `location_id`, `quantity`, `transaction_type`.
  - Optional sign-out/loan: `assignee_type`, `assignee_id`, `assignee_label`, `is_loan` (boolean), `due_back_date` (YYYY-MM-DD), `equipment_asset_id` (number), or `equipment_serial` (string; resolved to asset if asset_id not provided).
  - Other optional: `batch_id`, `unit_cost`, `reference_type`, `reference_id`, `metadata`, `client_action_id`, `weight`, `weight_uom`, `uom`.
- `transaction_type`: one of `in`, `out`, `adjustment`, `transfer`, `count`, `return`, `repack`.
  - Use **`out`** for sign-out (with assignee and optionally `is_loan`, `due_back_date`, `equipment_asset_id` or `equipment_serial`).
  - Use **`return`** for sign-in/return of equipment or stock.
- Response 201: `{ "transaction_id", "unit_cost", "total_cost", "stock": { "quantity_on_hand", "quantity_available", ... } }`.
- Errors: 400 if required fields missing, or equipment serial/asset not found or not for this item, or asset not available for out.

---

### Item usage by person (consumables)

**GET /api/items/<item_id>/usage_by_person?months=6**  
- Query: `months` (optional, default 6, max 36).
- Response 200: `{ "months": number, "usage": [ { "assignee_type", "assignee_id", "assignee_label", "tx_count", "total_out", "avg_per_month", "last_used_at" } ] }`.
- Excludes loaned transactions; use for reorder/trending by person.

**GET /api/items/<item_id>/usage_by_person_monthly?months=12**  
- Query: `months` (optional, default 12, max 36).
- Response 200: `{ "months": [ "YYYY-MM", ... ], "series": [ { "label": string, "data": [ number, ... ] } ] }`.
- Use for charts: one series per person, data aligned to `months`.

---

### Categories, locations, batches

**GET /api/categories** – list categories.  
**POST /api/categories** – create (body: `name`, `code`?, etc.).  
**GET /api/categories/<id>**, **PUT /api/categories/<id>**, **DELETE /api/categories/<id>**.

**GET /api/locations** – list locations.  
**POST /api/locations** – create.  
**GET /api/locations/<id>**, **PUT /api/locations/<id>`.

**GET /api/batches** – list (query: `item_id`, `limit`).  
**POST /api/batches** – create (body: `item_id`, ...).  
**GET /api/batches/<id>`, **PUT /api/batches/<id>`.

---

### Item overview (for item detail screen)

**GET /api/items/<item_id>/overview?range_days=30&bucket=day**  
- Response 200: `{ "current_qoh", "avg_daily_out", "days_of_cover", "reorder_point", "lead_time_days", "lead_time_threshold", "reorder_now", "series": [ { "t", "qoh" } ], "bucket_used", "tx_count", ... }`.

---

## App-side behaviour to implement

1. **Login**  
   - Call main app `POST /api/login` with `username` and `password`.  
   - Read `response.token` and store it; send as `Authorization: Bearer <token>` on every Inventory API request.  
   - If `response.token` is null, show `response.hint` (e.g. “Session token not available…”) and do not assume Bearer auth will work.

2. **Equipment items**  
   - When creating/editing items, support `is_equipment` and `requires_serial`; for equipment, show “Equipment assets” and “Add serial” using the equipment assets API.  
   - Optionally show `lead_time_days` and use it in reorder/cover calculations.

3. **Recording transactions (sign-out / sign-in)**  
   - For **out** or **return**:  
     - Add “Signed out to” (assignee): use **GET /api/people/search?q=...** to search users and contractors; store and send `assignee_type`, `assignee_id`, `assignee_label`.  
     - Add “Loaned” checkbox; when true, send `is_loan: true` and optionally `due_back_date` (YYYY-MM-DD).  
     - For equipment items: allow selecting or entering equipment (by **equipment_asset_id** or **equipment_serial**); send one of them in the POST body.  
   - For **return**: use `transaction_type: "return"` and, if returning the same unit, send the same `equipment_asset_id` (or serial) so the backend can set the asset back to `in_stock`.

4. **Transaction list and detail**  
   - Show assignee (assignee_label or assignee_type + assignee_id), “Loaned” (is_loan), due back date, and equipment serial/asset when present.

5. **Equipment assets management**  
   - List assets per item: **GET /api/equipment/assets?item_id=<id>**.  
   - Create: **POST /api/equipment/assets** with `item_id`, `serial_number`, and optional make/model/purchase_date/warranty_expiry/service_interval_days/condition.  
   - Edit: **PATCH /api/equipment/assets/<asset_id>** with the fields to update (including `status` if you allow changing status in the app).

6. **Usage by person**  
   - On item detail, call **GET /api/items/<id>/usage_by_person?months=...** for a table and **GET /api/items/<id>/usage_by_person_monthly?months=...** for a monthly chart; exclude or clearly label loaned usage if needed (backend already excludes loaned in these endpoints).

7. **Errors**  
   - 401: Missing or invalid auth; ensure Bearer token is sent and not expired.  
   - 400/404: Check response body for `{ "error": "..." }` and show the message to the user.

---

## Summary table (equipment & sign-out)

| Action              | Method | Path                                      | Key request body/query                         | Key response                    |
|---------------------|--------|-------------------------------------------|------------------------------------------------|---------------------------------|
| Search people       | GET    | /api/people/search?q=&limit=              | q, limit                                      | people[]                        |
| List equipment      | GET    | /api/equipment/assets?item_id=&status=    | item_id, status, search                       | assets[]                        |
| Create equipment    | POST   | /api/equipment/assets                     | item_id, serial_number, make, model, ...      | id                              |
| Update equipment    | PATCH  | /api/equipment/assets/<id>                | make, model, status, condition, ...           | ok                              |
| Record transaction  | POST   | /api/transactions                         | item_id, location_id, quantity, transaction_type, assignee_*, is_loan, due_back_date, equipment_asset_id or equipment_serial | transaction_id, stock           |
| List transactions   | GET    | /api/transactions                         | item_id, location_id, transaction_type, ...   | transactions[] (with assignee_*, is_loan, equipment_asset_id) |
| Usage by person     | GET    | /api/items/<id>/usage_by_person?months=   | months                                        | months, usage[]                 |
| Usage monthly       | GET    | /api/items/<id>/usage_by_person_monthly?months= | months                                     | months[], series[]              |

Use this spec to align the Lovable app with the backend so equipment, sign-out/sign-in, assignee, loan, and usage features work end-to-end.
