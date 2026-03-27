# Lovable app — update prompt

Copy the block below into Lovable when you want to align the Inventory Control app with the latest backend. Use **Plan mode** if you want Lovable to ask clarifying questions first.

---

## Auth update prompt (use token for API calls)

Use this first if the app runs on a different origin than the ERP (e.g. Lovable preview or a separate domain). It makes the app use a Bearer token instead of relying on cookies, so login and all inventory API calls work cross-origin.

**Inventory Control app — switch to Bearer token auth**

Our backend now returns a **session token** on login. Please update the app so it uses this token for all Inventory API calls instead of (or in addition to) cookies. This allows the app to work when it is served from a different origin than the ERP (e.g. Lovable preview, tablets, mobile).

**1. Login**
- Keep calling `POST {erpBase}/api/login` with body `{ "username": "...", "password": "..." }`.
  - `{erpBase}` is the ERP root URL (e.g. `https://your-erp.example.com`), **not** the inventory plugin path. So if Settings has “API base URL” = `https://your-erp.example.com/plugin/inventory_control`, then `erpBase` = `https://your-erp.example.com`.
- The response already includes `user`, `site_settings`, etc. It now also includes **`token`** (a JWT string). Store this token in memory or in a secure way (e.g. React state or a small auth context). Do not put the token in localStorage if the app may be used on shared devices; in-memory is fine for a session.

**2. All requests to the Inventory plugin**
- For every request to `{base}/api/*` (e.g. dashboard, items, categories, invoices), send the token in the header:
  - **`Authorization: Bearer <token>`**
  - Use the token stored from the login response.
- You can still send `credentials: 'include'` if the backend is same-origin and you want cookies, but for cross-origin the **token is sufficient** and preferred. The backend accepts either session (cookie) or Bearer token for these endpoints.

**3. Settings**
- **API base URL** should remain the inventory plugin base, e.g. `https://your-erp.example.com/plugin/inventory_control` (no trailing slash).
- Add an optional **ERP base URL** (or derive it from API base) for login: e.g. `https://your-erp.example.com`. Login is always `POST {erpBase}/api/login`; all other calls use the existing API base URL with the Bearer token.

**4. Logout**
- Call `POST {erpBase}/api/logout` if you want to invalidate the server session (optional). Clear the stored token and redirect to the login screen.

**5. 401 handling**
- If any Inventory API call returns 401, clear the stored token and redirect the user to the login screen (or show “Session expired, please log in again”).

Implement the above so that after login the app sends `Authorization: Bearer <token>` on all requests to `{base}/api/*`. Keep the rest of the app behaviour and UI unchanged.

---

## Update prompt (copy from here)

**Inventory Control app — backend alignment update**

Our Sparrow ERP backend has been updated. Please update the Lovable Inventory Control app so it matches the API and flows below. Keep the existing calm, professional ERP style (soft shadows, 8px radius, scannable tables). Use the configured API base URL from Settings for all requests. **If the app uses token auth:** send `Authorization: Bearer <token>` on every request to the inventory API (token from login response). Otherwise send cookies and `X-CSRFToken` for non-GET. Redirect to login on 401/403.

**1. Invoice upload**

- **Upload request:** `POST {base}/api/invoices/upload` with **FormData** containing:
  - `file` (required): image, PDF, or DOCX
  - `source` (optional): platform/vendor name, e.g. "Amazon" (defaults to "Amazon" on backend if omitted)
  - `supplier_id` (optional): integer, from a supplier dropdown
- Add to the upload form:
  - A **Source / platform** text input (placeholder e.g. "e.g. Amazon"), so users can set the vendor or marketplace.
  - A **Supplier** dropdown populated from `GET {base}/api/analytics/suppliers` (options: id, display name/code). Value can be empty.
- After a successful upload response (e.g. `invoice_id`), open the **Review** step for that invoice (see below) so the user can see and correct extracted data before applying.

**2. Invoice list**

- Add a **Source / Vendor** column. Show, in order of preference: `external_source`, or `parsed_payload.supplier_name`, or `parsed_payload.external_source`. If `parsed_payload` is a string, parse it once for display or show `external_source`.
- Add a **Review** button (or link) per row that opens the Review UI for that invoice (see below).

**3. Invoice review (view and edit extracted data)**

- When the user clicks Review (or right after upload), show a **Review** screen or modal for that invoice.
- **Data:** Load `GET {base}/api/invoices/<id>`. Response includes `lines[]` and `parsed_payload` (object with e.g. `supplier_name`, `external_source`, `invoice_number`, `invoice_date`).
- **Header section (editable):**
  - **Invoice number** — text input, value from `invoice_number`.
  - **Invoice date** — date input, value from `invoice_date` (use YYYY-MM-DD for the input).
  - **Source / platform** — text input, value from `external_source` or parsed_payload (e.g. "Amazon").
  - **Supplier** — dropdown from `GET {base}/api/analytics/suppliers`, value from `supplier_id` (allow empty).
- **Lines section (editable):** A table with one row per line. Each row has:
  - **SKU** — text input (from `line.sku`)
  - **Description** — text input (from `line.description`)
  - **Qty** — number input (from `line.quantity`)
  - **Unit price** — number input (from `line.unit_price`)
  - **Line total** — number input (from `line.line_total`)
  - **Match to item** — dropdown of items from `GET {base}/api/items?limit=1000`; value = `line.item_id`. On change, call `PUT {base}/api/invoices/<id>/lines/<line_id>/match` with body `{ "item_id": <id> or null }`.
- **Actions:**
  - **Save changes** — (1) `PUT {base}/api/invoices/<id>` with body `{ invoice_number, invoice_date, external_source, supplier_id }` (send only defined fields; `supplier_id` can be null to clear). (2) For each line, `PUT {base}/api/invoices/<id>/lines/<line_id>` with body `{ sku, description, quantity, unit_price, line_total }` for the current values in the row. Then show a short success message and refresh the invoice list if on a list page.
  - **Apply to stock** — open the existing “Apply to stock” flow (select location, then `POST {base}/api/invoices/<id>/apply` with `{ location_id }`).
- Show a short hint above the table: e.g. “Edit any field if the scan was wrong, then Save. Match lines to inventory items before applying.”

**4. New/updated API usage**

- `PUT {base}/api/invoices/<id>` — update invoice header. Body can include: `supplier_id`, `external_source`, `invoice_number`, `invoice_date`, `total_amount`, `currency`. Omitted keys are unchanged; sending `supplier_id: null` clears it.
- `PUT {base}/api/invoices/<id>/lines/<line_id>` — update one line. Body can include: `sku`, `description`, `quantity`, `unit_price`, `line_total`.
- `GET {base}/api/invoices/<id>` — returns invoice plus `lines` array and `parsed_payload` (object). Use for the Review screen.
- Upload still uses `POST {base}/api/invoices/upload` with FormData; add `source` and `supplier_id` as form fields as above.

**5. Behaviour and edge cases**

- If the backend returns an error on upload (e.g. 400 with a message), show the error to the user and do not open Review.
- If `invoice_date` is null or missing, the date input can be left empty; the backend accepts null for that field.
- When saving the invoice header, send date as YYYY-MM-DD or null; do not send an empty string for date.

Please implement the invoice upload (with source and supplier), the list Source column and Review action, and the full Review flow with editable header, editable lines, match-to-item dropdown, Save changes, and Apply to stock. Keep the rest of the app (dashboard, items, categories, locations, batches, transactions, analytics, settings) unchanged unless something conflicts with these updates.

Ask me any questions you need in order to fully understand what I want from this feature and how I envision it.

---

## End of update prompt

Use this prompt in Lovable when you want the app to support the new invoice flow (upload with source/supplier, review and edit extracted data, then apply). For a full feature list and API reference, see [LOVABLE_PRD_AND_PROMPT.md](./LOVABLE_PRD_AND_PROMPT.md).
