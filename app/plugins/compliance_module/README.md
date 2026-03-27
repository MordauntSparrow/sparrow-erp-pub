# Compliance & policies module

Organisational policies (Health & Safety, safeguarding, privacy, etc.) with **versioned issue** to all active contractors, **employee portal to-dos**, and **legal-style acknowledgement** (read + tick box). Intended to complement HR and avoid re-sending every revision through external e-sign for routine policy updates.

## Setup

1. Ensure **employee portal** is installed (`ep_todos` with `reference_type` / `reference_id`).
2. Run: `python app/plugins/compliance_module/install.py install`
3. Enable the plugin in your plugin manifest if required.

## Admin

- **Plugins → Compliance** (or `/plugin/compliance_module/`)
- Create a policy: title, category, summary, optional full text, optional PDF/Word upload.
- **Mandatory**: creates portal tasks and counts toward `pending_policies` on the dashboard.
- **Issue to all staff**: publishes (or bumps version), retires old mandatory to-dos for the previous version, and upserts a to-do per active contractor linking to `/compliance/policy/<id>`.

## Contractor experience

- **Compliance & Policies** tile on the employee portal (`/compliance/`).
- Mandatory items must be opened and acknowledged; acknowledgement stores version, timestamp, IP, and user-agent.
- Dashboard **“policies to sign”** uses `pending_policies_count(contractor_id)`.

## HR / “cannot work until signed”

Use `contractor_compliance_blocks_work(contractor_id)` from `compliance_module.services` in your own checks (e.g. before assigning shifts or submitting timesheets) if you want a hard gate. The portal surfaces pending counts; enforcement in other modules is optional.

## Integration

- **Employee portal** already links to this module when enabled (`MODULE_LINKS_CONFIG`).
- Works alongside **HR** document requests; policies here are organisation-wide acknowledgements, not per-file HR uploads.
