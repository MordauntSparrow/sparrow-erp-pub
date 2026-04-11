# Employee Portal (Employee Home) – Todo List

Development and enhancement checklist for the employee home module (port 80 / contractor portal).

---

## Done

- [x] Login with `tb_contractors`; session `tb_user` shared with time billing
- [x] Dashboard: profile (avatar/initials), company name, module links
- [x] Messages section (`ep_messages`); mark as read API
- [x] To-do section (`ep_todos`); mark complete API; other modules can push todos
- [x] Quick actions: Request time off, Report sickness (links to scheduling)
- [x] Pending counts: policies to sign (compliance), HR document requests
- [x] Module links greyed out when plugin is not enabled (manifest `enabled: false`)
- [x] Mobile-first layout and styling (base + dashboard templates)
- [x] README + install; other modules can register messages/todos

---

## Production-ready (done)

- [x] Services layer: `services.py` with limits, safe_next_url, safe_profile_picture_path, get_messages/get_todos/get_module_links/get_pending_counts
- [x] Safe redirect: `next` validated (relative path only); profile picture path validated (no path traversal)
- [x] Logging: login failure/success, inactive reject, API/dashboard errors (no secrets in logs)
- [x] API: 400/401/404/500 with JSON error; front-end handles non-OK and shows feedback
- [x] Quick actions only when scheduling module enabled
- [x] Accessibility: skip link, aria-labelledby, aria-label, autofocus, semantic main
- [x] Login: hidden `next`, autofocus, aria; Bootstrap alert danger for errors
- [x] README: production checklist (HTTPS, secret key, session cookies, rate limiting, logging, redirect/profile safety)

## In progress / Next

- [ ] Optional: portal-originated todos (e.g. “Complete your profile”) from this module
- [ ] Optional: pending counts only when compliance/HR are enabled (already safe via try/except)

---

## Future ideas

- [ ] Notifications badge (unread messages + incomplete todos count) in nav
- [ ] Dashboard widgets configurable per role or per contractor
- [ ] “Employee home” naming in UI (e.g. nav label “Home” vs “Portal”) for consistency with “employee home module”

---

## Module link ↔ plugin mapping (for grey-out)

| Dashboard label           | URL           | Plugin `system_name`     |
|---------------------------|---------------|--------------------------|
| Time & Billing            | /time-billing/ | time_billing_module     |
| Work                      | /work/        | work_module             |
| HR                        | /hr/          | hr_module               |
| Compliance & Policies     | /compliance/  | compliance_module       |
| Training                  | /training/    | training_module         |
| Scheduling & Shifts       | /scheduling/  | scheduling_module       |

Disabled or missing plugins are shown as greyed-out tiles with “Not available”.
