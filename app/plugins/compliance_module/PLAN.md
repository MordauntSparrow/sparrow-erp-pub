# Compliance & Policies Module – Product & Implementation Plan

## Goal

Production-ready **policy and compliance management** comparable to Riliance, ComplianceBridge, and HR policy tools: create and version policies, assign to staff or groups, track acknowledgements, and support audits.

---

## Current State

### Data model (existing)
- **compliance_policies** – id, title, slug, summary, body, version, effective_from, effective_to, required_acknowledgement, active, created_at, updated_at
- **compliance_acknowledgements** – policy_id, contractor_id, acknowledged_at, ip_address, user_agent

### Contractor (public) features
- List active policies (effective date range); see acknowledged vs to-sign
- View policy (body); sign/acknowledge if required
- Dashboard shows pending count

### Admin
- Landing page only

---

## Admin Features (to build)

### 1. Policy library
- **List policies** – all (including inactive); filter by active, effective date, search by title/slug; sort by effective_from, title
- **Create policy** – title, slug (auto from title or manual), summary, body (rich text or markdown), version, effective_from, effective_to, required_acknowledgement (yes/no), active
- **Edit policy** – same fields; **versioning**: optionally create new version (bump version, new effective_from) and keep old for history
- **Deactivate** – set active = 0 so it no longer appears for staff (existing data preserved)
- **Duplicate** – copy policy as template for a new one

### 2. Acknowledgement and assignment
- **Assign to contractors** – optionally restrict which contractors must acknowledge (e.g. by role, department, or manual list); if no assignment, “all” (current behaviour)
- **Acknowledgement report** – per policy: list contractors, acknowledged yes/no, date, IP; export CSV
- **Remind** – send message or todo to contractors who haven’t signed (integrate with Employee Portal)

### 3. Policy categories and organisation
- **Categories** – e.g. “HR”, “Health & Safety”, “Data Protection”; policy has optional category; filter list by category
- **Ordering** – display order for contractor list (e.g. sort order field)

### 4. Audit and reporting
- **Audit log** – who created/updated which policy and when
- **Signing report** – across all policies: % signed, list of unsigned per policy
- **Export** – acknowledgements for a policy or all (CSV) for compliance audits

---

## Contractor (public) enhancements

- **Categories** – group policies by category on the list view
- **History** – optional “Previously signed” section for policies that are now inactive or superseded (read-only)
- **Clear CTA** – “You must sign by …” where applicable (e.g. required_by_date per assignment, optional)
- **Mobile** – ensure signing and view work well on small screens

---

## Data model changes (migrations)

| Change | Purpose |
|--------|--------|
| **compliance_policies**: add category_id INT NULL, display_order INT DEFAULT 0 | Categories and ordering |
| **compliance_categories** (new) | id, name, slug, description, display_order |
| **compliance_policy_assignments** (optional) | policy_id, contractor_id (or role/department if we have it); if empty = all must sign |
| **compliance_acknowledgements**: add optional required_by_date, optional reminder_sent_at | Support “sign by” and reminder tracking |
| **compliance_audit** (optional) | policy_id, user_id, action (created, updated, deactivated), at |

---

## Implementation order (admin first)

1. **Admin: Policy list** – list all policies, filters, link to create/edit
2. **Admin: Create / edit policy** – form for all fields; slug validation; version display
3. **Admin: Acknowledgement report** – per policy, list contractors and signed status; export CSV
4. **Schema** – categories table + category_id on policies; display_order
5. **Admin: Categories** – CRUD categories; assign category to policy
6. **Admin: Remind** – button to send portal message/todo to unsigned contractors (by policy)
7. **Contractor** – categories and ordering on list; optional “sign by” and history
8. **Optional**: policy_assignments for “who must sign” and required_by_date

---

## Success criteria

- Admin can create, edit, version, and deactivate policies with clear effective dates
- Admin can see who has signed and who hasn’t, and remind non-signers
- Categories and order make the contractor list scannable
- Export and audit trail support compliance reviews
