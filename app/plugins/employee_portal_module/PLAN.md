# Employee Portal Module – Product & Implementation Plan

## Goal

Make the Employee Portal a **production-ready staff hub** comparable to Breathe HR, Personio, and intranet portals: one place for messages, tasks, quick actions, and module entry. Admin must be able to control what contractors see and send them targeted messages and todos.

---

## Current State

### Data model (existing)
- **ep_migrations** – schema versioning
- **ep_messages** – contractor_id, source_module, subject, body, read_at, created_at
- **ep_todos** – contractor_id, source_module, title, link_url, due_date, completed_at, created_at

### Contractor (public) features
- Dashboard: profile, quick actions (time off, sickness), module tiles, messages (mark read), todos (mark complete)
- Login/logout; session shared with Time & Billing

### Admin
- Landing page only (links to Core settings, Plugins)

---

## Admin Features (to build)

### 1. Message centre
- **List messages** – filter by contractor, source module, read/unread, date range; pagination
- **Send message** – to one contractor, multiple (select list), or “all”; subject, body; optional link/CTA
- **View thread** – see single contractor’s messages (chronological)
- **Delete/archive** – soft delete or archive old messages

### 2. Todo / task management
- **List todos** – filter by contractor, source module, completed/pending, due date
- **Create todo** – assign to contractor(s), title, optional link_url, due_date, source_module = `employee_portal_module`
- **Edit todo** – change title, link, due date
- **Complete / reopen** – mark done or revert
- **Bulk create** – e.g. “Sign policy X” for all contractors who haven’t (integrate with Compliance)

### 3. Contractor lookup and portal view
- **Search contractors** – by name, email (reuse tb_contractors); click through to “View as contractor” summary
- **Portal preview** – read-only view of what that contractor sees (messages, todos, module links, pending counts)

### 4. Module visibility and defaults
- **Configure module tiles** – which modules appear and in what order (already driven by plugin manifest + enabled; optional overrides in config)
- **Default dashboard copy** – optional welcome message or notice for all (e.g. stored in core manifest or ep_settings)

### 5. Reporting and audit
- **Message delivery / read stats** – count sent, read, by module
- **Todo completion rates** – by contractor or by task type
- **Audit log** – who sent which message, who created which todo (optional table)

---

## Contractor (public) enhancements

- **Notifications** – badge counts on portal nav for unread messages and incomplete todos (already partially there)
- **Message detail view** – full-body view and mark read (optional dedicated page)
- **Todo list filters** – show all / pending / completed
- **Accessibility and mobile** – ensure all actions work on small screens and with screen readers

---

## Data model changes (migrations)

| Change | Purpose |
|--------|--------|
| **ep_messages**: add `sent_by_user_id` (INT NULL), `updated_at` | Audit who sent; track edits |
| **ep_todos**: add `created_by_user_id` (INT NULL), `updated_at` | Audit who created task |
| **ep_settings** (new table, optional) | Key-value for portal-wide settings (e.g. welcome banner text) |

---

## Implementation order (admin first)

1. **Admin: Contractor search** – search by name/email, list results, link to “Portal view” for that contractor
2. **Admin: Message list + send** – list with filters, form to send to one or many contractors
3. **Admin: Todo list + create/edit** – list with filters, create single/bulk, edit, complete/reopen
4. **Admin: Portal preview** – read-only dashboard view for a chosen contractor
5. **Admin: Message/todo audit fields** – migrations + populate sent_by/created_by
6. **Contractor: Message detail page** (optional) and small UX improvements

---

## Success criteria

- Admins can search contractors and see what they see in the portal
- Admins can send messages and create todos to individuals or groups
- All admin actions are behind admin/superuser and auditable where required
- Contractor experience remains fast and clear on desktop and mobile
