# HR Module – Product & Implementation Plan

## Goal

Production-ready **HR and personnel management** comparable to Breathe HR, Personio, Charlie HR: contractor/employee records, document requests (right to work, DBS, driving licence, contracts), expiry tracking, and a clear contractor self-service view.

---

## Current State

### Data model (existing)
- **hr_staff_details** – contractor_id, phone, address (line1, line2, postcode), emergency_contact_name, emergency_contact_phone, updated_at
- **hr_document_requests** – contractor_id, title, description, required_by_date, status (pending, uploaded, approved, overdue), created_at
- **hr_document_uploads** – request_id, file_path, file_name, uploaded_at

### Contractor (public) features
- View/edit own profile (phone, address, emergency contact)
- List document requests; view request detail; upload file(s) per request
- Status shown (pending, uploaded, etc.)

### Admin
- Landing page only

---

## Admin Features (to build)

### 1. Contractor / staff search and profile
- **Search** – by name, email, phone (tb_contractors + hr_staff_details); pagination
- **Profile view** – single page per contractor:
  - Core info (name, email, status from tb_contractors)
  - HR details (phone, address, emergency contact) – **editable by admin**
  - **Structured fields** (see schema below): driving licence (number, expiry, copy), right to work (type, expiry, document), DBS (level, number, expiry), contract (type, start, end, document), other IDs
  - List of document requests and uploads with status
- **Edit profile** – save all admin-editable fields (address, emergency contact, licence, right to work, DBS, contract dates, etc.)

### 2. Document request management
- **List requests** – filter by contractor, status, date range; show required_by_date and overdue
- **Create request** – select contractor(s) (single or multi), title, description, required_by_date; optional **request type** (e.g. “Right to work”, “Driving licence”, “Contract”, “DBS”, “Other”)
- **View request** – see request + all uploads; **approve** or **reject** (with optional admin note); status: pending → uploaded → approved/rejected
- **Remind** – send reminder (creates portal message or email) for pending/overdue
- **Bulk request** – e.g. “Request right to work from all contractors who don’t have one on file or expired”

### 3. Document and expiry tracking
- **Expiry dashboard** – list documents/rights expiring in 30/60/90 days (driving licence, right to work, DBS, contract end)
- **Alerts** – optional ep_message or todo when a document is nearing expiry (admin-configurable)
- **Store document metadata** – link uploads to “type” (e.g. driving licence copy, passport, contract) and expiry where applicable

### 4. Reporting
- **Compliance overview** – % of contractors with right to work, DBS, contract; list of gaps
- **Export** – CSV of staff + key dates and statuses for audits

---

## Contractor (public) enhancements

- **Profile** – show read-only summary of what admin holds (e.g. “Your right to work is on file and expires on …”)
- **Document requests** – clearer labels (e.g. “Right to work”, “Driving licence”), due date prominence, and “Approved” badge when approved
- **Upload replacement** – if admin rejects, allow contractor to upload a new file for the same request
- **Optional**: contractor can upload a document “unsolicited” (e.g. new licence) and admin can attach it to a request type later

---

## Data model changes (migrations)

| Change | Purpose |
|--------|--------|
| **hr_staff_details**: add driving_licence_number, driving_licence_expiry, driving_licence_document_path (or link to upload) | Store licence and expiry |
| **hr_staff_details**: add right_to_work_type (e.g. passport, visa), right_to_work_expiry, right_to_work_document_path | Right to work |
| **hr_staff_details**: add dbs_level, dbs_number, dbs_expiry, dbs_document_path | DBS |
| **hr_staff_details**: add contract_type, contract_start, contract_end, contract_document_path | Contract |
| **hr_document_requests**: add request_type ENUM (e.g. 'right_to_work','driving_licence','dbs','contract','other'), approved_at, approved_by_user_id, rejected_at, rejected_by_user_id, admin_notes | Workflow and reporting |
| **hr_document_uploads**: add document_type (e.g. 'primary','replacement'), optional link to hr_staff_details field | Support “replacement” and link to profile fields |
| **hr_audit_log** (optional) | Log admin changes to profile and request status |

---

## Implementation order (admin first)

1. **Schema** – add columns to hr_staff_details and hr_document_requests (migration in install.py or separate migration step)
2. **Admin: Contractor search** – search by name/email, list results
3. **Admin: Profile view + edit** – single contractor page: display and edit core + HR details + new fields (licence, right to work, DBS, contract)
4. **Admin: Document request list + create** – list with filters; create request (single/multi contractor, type, due date)
5. **Admin: Request detail** – view uploads, approve/reject, add note
6. **Admin: Expiry dashboard** – list expiring documents and gaps
7. **Contractor: Profile** – show summary of stored docs and expiry where relevant
8. **Contractor: Request flow** – replacement upload and “Approved” state visible
9. **Reporting** – compliance overview and export

---

## Success criteria

- Admin can find any contractor, view and edit full HR profile including licence, right to work, DBS, contract
- Admin can request documents by type, see uploads, and approve/reject with notes
- Expiring documents are visible and actionable
- Contractors have a clear view of their data and requests and can replace rejected uploads
